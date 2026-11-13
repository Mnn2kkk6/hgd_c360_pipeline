"""Đối soát 3 tầng Nguồn -> Mirror -> C360 + kiểm tra mẫu bằng bản tham chiếu Python + thống kê cờ DQ.

  python -m hgd.recon.reconcile                 # chạy đủ, ghi C360_RECON_RESULT
  python -m hgd.recon.reconcile --sample 500    # số công dân kiểm mẫu
Mã thoát 1 nếu có check FAIL (Airflow dùng để cảnh báo).
"""
from __future__ import annotations
import argparse
import sys
import uuid
from datetime import datetime

from hgd import db
from hgd.c360 import reference as ref
from hgd.generator.geo import dm_rows  # noqa: F401  (giữ tương thích import)

VALID = "NVL(m.TYPEID,'ZZ') NOT IN ('GT','GM','GG','GK') AND NVL(m.TRANG_THAI_DUYET,0) NOT IN (1,3,4)"


class Recon:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.run_id = uuid.uuid4().hex
        self.results: list[tuple] = []
        self.pending = 0

    def q1(self, sql, **b):
        self.cur.execute(sql, b)
        return self.cur.fetchone()[0]

    def add(self, name, scope, exp, act, status=None, note=None):
        diff = None if exp is None or act is None else act - exp
        if status is None:
            status = "PASS" if diff == 0 else "FAIL"
        self.results.append((self.run_id, name, scope, exp, act, diff, status, note))

    def save(self):
        self.cur.executemany(
            "INSERT INTO C360_RECON_RESULT (RUN_ID, CHECK_NAME, SCOPE, EXPECTED, ACTUAL, DIFF, STATUS, NOTE) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)",
            self.results)
        self.conn.commit()

    # ---- Tầng 1: Nguồn vs Mirror
    def src_vs_mirror(self):
        for t in ("VSS_HO_GD_NHANKHAU", "VSS_DM_DANTOC", "VSS_DM_QUOC_TICH", "VSS_DM_DBHC"):
            s = self.q1(f"SELECT COUNT(*) FROM VSS.{t}")
            m = self.q1(f"SELECT COUNT(*) FROM MIRROR.{t}")
            self.add("SRC_VS_MIRROR_TOTAL", t, s, m, note=None if s == m else "lệch có thể tạm thời nếu CDC còn trễ")
        # theo tỉnh (mã tỉnh nguồn dạng thô)
        q = "SELECT NVL(MA_TINH,'<NULL>'), COUNT(*) FROM {} GROUP BY MA_TINH"
        self.cur.execute(q.format("VSS.VSS_HO_GD_NHANKHAU")); s = dict(self.cur.fetchall())
        self.cur.execute(q.format("MIRROR.VSS_HO_GD_NHANKHAU")); m = dict(self.cur.fetchall())
        bad = {k: (s.get(k, 0), m.get(k, 0)) for k in set(s) | set(m) if s.get(k, 0) != m.get(k, 0)}
        self.add("SRC_VS_MIRROR_BY_TINH", f"{len(s)} nhóm mã tỉnh", 0, len(bad),
                 note="; ".join(f"{k}:{a}->{b}" for k, (a, b) in list(bad.items())[:10]) or None)

    # ---- Tầng 2: Mirror vs C360
    def mirror_vs_c360(self):
        self.pending = self.q1("SELECT COUNT(*) FROM C360_QUEUE")
        exp = self.q1(f"SELECT COUNT(DISTINCT m.MA_CD) FROM MIRROR.VSS_HO_GD_NHANKHAU m WHERE {VALID}")
        act = self.q1("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN")
        st = "PASS" if exp == act else ("PENDING" if self.pending > 0 else "FAIL")
        self.add("MIRROR_VS_C360_COUNT", "số công dân hợp lệ", exp, act, st, f"queue chờ={self.pending}")
        stale = self.q1(f"""SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN c WHERE NOT EXISTS
                            (SELECT 1 FROM MIRROR.VSS_HO_GD_NHANKHAU m WHERE m.MA_CD = c.MA_SO_BHXH AND {VALID})""")
        self.add("C360_STALE_ROWS", "C360 có mà Mirror không còn hợp lệ", 0, stale, "PASS" if stale == 0 else ("PENDING" if self.pending else "FAIL"))
        missing = self.q1(f"""SELECT COUNT(*) FROM (SELECT DISTINCT m.MA_CD FROM MIRROR.VSS_HO_GD_NHANKHAU m WHERE {VALID}) x
                              WHERE NOT EXISTS (SELECT 1 FROM C360_THONG_TIN_CA_NHAN c WHERE c.MA_SO_BHXH = x.MA_CD)""")
        self.add("C360_MISSING_ROWS", "Mirror hợp lệ mà C360 chưa có", 0, missing, "PASS" if missing == 0 else ("PENDING" if self.pending else "FAIL"))
        dup = self.q1("SELECT COUNT(*) FROM (SELECT MA_SO_BHXH FROM C360_THONG_TIN_CA_NHAN GROUP BY MA_SO_BHXH HAVING COUNT(*) > 1)")
        self.add("C360_DUP_KEY", "MA_SO_BHXH trùng", 0, dup)
        self.add("QUEUE_BACKLOG", "dòng đang chờ", None, self.pending, "INFO")

    # ---- Kiểm mẫu: Oracle vs bản tham chiếu Python
    def sample_check(self, n: int):
        # lấy mẫu: ưu tiên công dân nhiều hộ (khó nhất) + ngẫu nhiên
        self.cur.execute(f"""SELECT MA_CD FROM (SELECT MA_CD FROM MIRROR.VSS_HO_GD_NHANKHAU GROUP BY MA_CD HAVING COUNT(*) >= 2
                             ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM <= :n""", n=n // 2)
        cds = [r[0] for r in self.cur.fetchall()]
        self.cur.execute("SELECT MA_CD FROM (SELECT MA_CD FROM MIRROR.VSS_HO_GD_NHANKHAU SAMPLE(1) ORDER BY DBMS_RANDOM.VALUE) WHERE ROWNUM <= :n", n=n - len(cds))
        cds = list(dict.fromkeys(cds + [r[0] for r in self.cur.fetchall()]))
        if not cds:
            self.add("SAMPLE_REFERENCE", "không có dữ liệu", None, None, "INFO"); return
        dbhc = self._rows("SELECT * FROM MIRROR.VSS_DM_DBHC")
        qt = self._rows("SELECT * FROM MIRROR.VSS_DM_QUOC_TICH"); dt = self._rows("SELECT * FROM MIRROR.VSS_DM_DANTOC")
        mt, mx = ref.build_maps(dbhc)
        qt_ma, qt_id, dt_id = {r["MA"]: r for r in qt}, {int(r["ID"]): r for r in qt}, {int(r["ID"]): r for r in dt}
        bad: list[str] = []
        for i in range(0, len(cds), 500):
            part = cds[i:i + 500]
            binds = {f"c{j}": c for j, c in enumerate(part)}
            ph = ",".join(":" + k for k in binds)
            mir = self._rows(f"SELECT * FROM MIRROR.VSS_HO_GD_NHANKHAU WHERE MA_CD IN ({ph})", **binds)
            c360 = {r["MA_SO_BHXH"]: r for r in self._rows(f"SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH IN ({ph})", **binds)}
            by = {}
            for r in mir: by.setdefault(r["MA_CD"], []).append(r)
            for cd in part:
                best = ref.pick_latest(by.get(cd, []))
                got = c360.get(cd)
                if best is None:
                    if got is not None: bad.append(f"{cd}: C360 có nhưng đáng lẽ không")
                    continue
                if got is None:
                    bad.append(f"{cd}: thiếu trong C360"); continue
                exp = ref.normalize_row(best, mt, mx, qt_ma, qt_id, dt_id)
                for k in ("MA_HO_GD", "NGAY_SINH", "SO_DIEN_THOAI", "SO_DINH_DANH", "HO_TEN", "HO_TEN_KHONG_DAU", "MA_TINH", "MA_XA",
                          "NGUON_MAP_XA", "MA_TINH_LH", "MA_XA_LH", "MA_TINH_KS", "MA_XA_KS", "QUOC_TICH_MA", "DAN_TOC_MA", "DQ_FLAGS"):
                    if exp.get(k) != got.get(k):
                        bad.append(f"{cd}.{k}: Oracle={got.get(k)!r} tham chiếu={exp.get(k)!r}")
        st = "PASS" if not bad else ("PENDING" if self.pending else "FAIL")
        self.add("SAMPLE_REFERENCE", f"{len(cds)} công dân", 0, len(bad), st, " | ".join(bad[:5]) or None)

    def _rows(self, sql, **b):
        cur = self.conn.cursor()
        cur.execute(sql, b)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    # ---- Thống kê DQ
    def dq_summary(self):
        total = self.q1("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN")
        clean = self.q1("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE DQ_FLAGS IS NULL")
        self.add("DQ_CLEAN", f"tổng {total}", total, clean, "INFO", f"{(100 * clean / total) if total else 0:.2f}% sạch hoàn toàn")
        self.cur.execute("""SELECT f.FLAG_CODE, COUNT(*) FROM C360_THONG_TIN_CA_NHAN c JOIN C360_DQ_FLAG_DEF f
                            ON INSTR(';' || c.DQ_FLAGS || ';', ';' || f.FLAG_CODE || ';') > 0 GROUP BY f.FLAG_CODE ORDER BY 2 DESC""")
        for code, n in self.cur.fetchall():
            self.add("DQ_FLAG", code, total, n, "INFO", f"{(100 * n / total) if total else 0:.2f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=300)
    a = ap.parse_args()
    conn = db.connect("c360")
    r = Recon(conn)
    r.src_vs_mirror(); r.mirror_vs_c360(); r.sample_check(a.sample); r.dq_summary(); r.save()
    print(f"RUN {r.run_id}  ({datetime.now():%Y-%m-%d %H:%M:%S})")
    fails = 0
    for _, name, scope, exp, act, diff, st, note in r.results:
        if st != "INFO" or name == "DQ_CLEAN":
            print(f"{st:8s} {name:24s} {str(scope)[:40]:40s} exp={exp} act={act} diff={diff} {note or ''}")
        fails += st == "FAIL"
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
