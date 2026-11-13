"""Sinh dữ liệu BIẾN ĐỘNG trên nguồn VSS (kích hoạt luồng CDC -> Mirror -> Queue -> C360) + ghi manifest kỳ vọng.

  python -m hgd.generator.delta --scenario all --insert 20000 --update 2000 --chunk 2000 --sleep 0.5
  python -m hgd.tools.verify_delta out/delta_manifest.json      # (chờ 1-2 phút) kiểm tra C360 khớp kỳ vọng

Kịch bản: insert (công dân mới + công dân sang hộ mới), add_cccd, fix_xa, fix_ngay_sinh, approve, reject (TRANG_THAI_DUYET=4 / TYPEID=GG),
change_ma (đổi MA_CD, ghi MA_CD_OLD), delete_latest (xóa dòng mới nhất của người nhiều hộ), delete_all (xóa mọi dòng của công dân).
"""
from __future__ import annotations
import argparse
import json
import random
import time
from datetime import datetime
from pathlib import Path

from hgd import db
from hgd.c360 import reference as ref
from . import geo as G
from .people import SOURCE_COLS, cccd_for, gen_people

T = "VSS.VSS_HO_GD_NHANKHAU"
ALL = ["insert", "add_cccd", "fix_xa", "fix_ngay_sinh", "approve", "reject", "change_ma", "delete_latest", "delete_all"]
SINGLE = f"NOT EXISTS (SELECT 1 FROM {T} o WHERE o.MA_CD = t.MA_CD AND o.MA_HO_GD <> t.MA_HO_GD)"
VALID = "NVL(t.TYPEID,'ZZ') NOT IN ('GT','GM','GG','GK') AND NVL(t.TRANG_THAI_DUYET,0) NOT IN (1,3,4)"


def set_target(table: str):
    """Chạy kịch bản trên bảng khác (vd Mirror, để thử Near Real-time khi chưa có CDC)."""
    global T, SINGLE
    T = table
    SINGLE = f"NOT EXISTS (SELECT 1 FROM {T} o WHERE o.MA_CD = t.MA_CD AND o.MA_HO_GD <> t.MA_HO_GD)"


def _fetch(cur, sql, **b):
    cur.execute(sql, b)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


class Delta:
    def __init__(self, conn, rng, now, geo, dm, chunk, sleep, dbhc):
        self.conn, self.rng, self.now, self.geo, self.dm = conn, rng, now, geo, dm
        self.chunk, self.sleep, self.dbhc = chunk, sleep, dbhc
        self.cur = conn.cursor()
        self.manifest: list[dict] = []
        self.used: set[str] = set()      # MA_CD đã dùng ở kịch bản khác -> tránh chồng chéo làm sai kỳ vọng

    def _commit_pause(self):
        self.conn.commit()
        if self.sleep:
            time.sleep(self.sleep)

    def _max_seq(self, prefix):
        r = _fetch(self.cur, f"SELECT MAX(MA_CD) M FROM {T} WHERE MA_CD LIKE :p", p=prefix + "%")[0]["M"]
        return int(r[len(prefix):]) + 1 if r else 1

    def _rand_start(self, n):
        """Chọn n dòng đơn-hộ hợp lệ bắt đầu từ vị trí ngẫu nhiên của chỉ mục MA_CD."""
        return f"{self.rng.randint(1, 68):02d}{self.rng.randint(0, 9999999):08d}"

    def _pick(self, extra, n, bind=None):
        """n dòng đơn-hộ thỏa điều kiện, MA_CD < '7' (không đụng công dân do kịch bản insert/change_ma tạo), chưa dùng."""
        sql = (f"SELECT t.MA_CD, t.MA_HO_GD FROM {T} t WHERE t.MA_CD >= :s AND t.MA_CD < '7' AND ({extra}) AND {SINGLE} "
               f"ORDER BY t.MA_CD FETCH FIRST :n ROWS ONLY")
        out: list[dict] = []
        for start in [self._rand_start(n), self._rand_start(n), "0"]:
            for r in _fetch(self.cur, sql, s=start, n=n * 2, **(bind or {})):
                if r["MA_CD"] not in self.used and len(out) < n:
                    self.used.add(r["MA_CD"])
                    out.append(r)
            if len(out) >= n:
                break
        return out

    # ------------------------------------------------------------------ kịch bản
    def insert(self, n):
        seq = self._max_seq("8")
        rows = gen_people(n, self.geo, self.dm, self.rng, self.now, seq_start=seq, ma_prefix="8", ho_prefix="DLT", dup_rate=0.0)
        ins = f"INSERT INTO {T} ({','.join(SOURCE_COLS)}) VALUES ({','.join(':' + c for c in SOURCE_COLS)})"
        for ch in _chunks(rows, self.chunk):
            self.cur.executemany(ins, ch)
            self._commit_pause()
        ok = [r for r in rows if ref.is_valid_row(r)]
        for r in self.rng.sample(ok, min(30, len(ok))):
            self.manifest.append(dict(scenario="insert", ma_cd=r["MA_CD"], expect=dict(exists=True, ho_gd=r["MA_HO_GD"])))
        filtered = [r for r in rows if not ref.is_valid_row(r)]
        for r in self.rng.sample(filtered, min(10, len(filtered))):
            self.manifest.append(dict(scenario="insert_filtered", ma_cd=r["MA_CD"], expect=dict(exists=False)))
        # công dân hiện hữu sang hộ mới (MODIFY_DATE mới hơn) -> C360 phải lấy MA_HO_GD mới
        ex = self._pick(VALID, max(10, n // 20))
        for ch in _chunks(ex, self.chunk):
            for e in ch:
                new_ho = f"DLT_NEW_{e['MA_CD']}"
                sel = ",".join(":ho" if c == "MA_HO_GD" else ":newid" if c == "ID" else "SYSDATE" if c == "MODIFY_DATE" else c
                               for c in SOURCE_COLS)
                self.cur.execute(
                    f"INSERT INTO {T} ({','.join(SOURCE_COLS)}) SELECT {sel} FROM {T} WHERE MA_CD = :cd AND MA_HO_GD = :oldho",
                    ho=new_ho, newid=f"{e['MA_CD']}-{new_ho}", cd=e["MA_CD"], oldho=e["MA_HO_GD"])
            self._commit_pause()
        for e in ex[:30]:
            self.manifest.append(dict(scenario="new_household", ma_cd=e["MA_CD"], expect=dict(exists=True, ho_gd=f"DLT_NEW_{e['MA_CD']}")))

    def _update(self, sql, rows, binder):
        for ch in _chunks(rows, self.chunk):
            self.cur.executemany(sql, [binder(r) for r in ch])
            self._commit_pause()

    def add_cccd(self, n):
        rows = self._pick(f"{VALID} AND t.SO_CCCD IS NULL AND t.SO_CMTND IS NULL", n)
        val = {r["MA_CD"]: cccd_for("001", 1990, True, self.rng) for r in rows}
        self._update(f"UPDATE {T} SET SO_CCCD=:v, MODIFY_DATE=SYSDATE WHERE MA_CD=:cd AND MA_HO_GD=:ho", rows,
                     lambda r: dict(v=val[r["MA_CD"]], cd=r["MA_CD"], ho=r["MA_HO_GD"]))
        for r in rows[:30]:
            self.manifest.append(dict(scenario="add_cccd", ma_cd=r["MA_CD"], expect=dict(exists=True, so_dinh_danh=val[r["MA_CD"]], no_flag=["KHONG_SO_DINH_DANH"])))

    def fix_xa(self, n):
        rows = self._pick(f"{VALID} AND t.MA_XA IN ('99999','00000')", n)
        xs = {r["MA_CD"]: self.rng.choice(self.geo.xa) for r in rows}
        self._update(f"UPDATE {T} SET MA_XA=:x, MA_TINH=:t, MODIFY_DATE=SYSDATE WHERE MA_CD=:cd AND MA_HO_GD=:ho", rows,
                     lambda r: dict(x=xs[r["MA_CD"]]["ma"], t=xs[r["MA_CD"]]["tinh"] + "TTT", cd=r["MA_CD"], ho=r["MA_HO_GD"]))
        for r in rows[:30]:
            x = xs[r["MA_CD"]]
            self.manifest.append(dict(scenario="fix_xa", ma_cd=r["MA_CD"], expect=dict(exists=True, ma_xa=x["ma"], no_flag=["XA_KHONG_MAP", "TINH_KHONG_MAP", "TINH_LECH_XA"])))

    def fix_ngay_sinh(self, n):
        rows = self._pick(f"{VALID} AND (t.NGAY_SINH IN (17500101, 20991231) OR t.NGAY_SINH IS NULL)", n)
        self._update(f"UPDATE {T} SET NGAY_SINH=19900505, MODIFY_DATE=SYSDATE WHERE MA_CD=:cd AND MA_HO_GD=:ho", rows,
                     lambda r: dict(cd=r["MA_CD"], ho=r["MA_HO_GD"]))
        for r in rows[:30]:
            self.manifest.append(dict(scenario="fix_ngay_sinh", ma_cd=r["MA_CD"], expect=dict(exists=True, ngay_sinh="19900505", no_flag=["NGAY_SINH_LOI"])))

    def approve(self, n):
        rows = self._pick("t.TRANG_THAI_DUYET = 1 AND NVL(t.TYPEID,'ZZ') NOT IN ('GT','GM','GG','GK')", n)
        self._update(f"UPDATE {T} SET TRANG_THAI_DUYET=2, MODIFY_DATE=SYSDATE WHERE MA_CD=:cd AND MA_HO_GD=:ho", rows,
                     lambda r: dict(cd=r["MA_CD"], ho=r["MA_HO_GD"]))
        for r in rows[:30]:
            self.manifest.append(dict(scenario="approve", ma_cd=r["MA_CD"], expect=dict(exists=True)))

    def reject(self, n):
        rows = self._pick(VALID, n)
        half = len(rows) // 2
        self._update(f"UPDATE {T} SET TRANG_THAI_DUYET=4, MODIFY_DATE=SYSDATE WHERE MA_CD=:cd AND MA_HO_GD=:ho", rows[:half],
                     lambda r: dict(cd=r["MA_CD"], ho=r["MA_HO_GD"]))
        self._update(f"UPDATE {T} SET TYPEID='GG', MODIFY_DATE=SYSDATE WHERE MA_CD=:cd AND MA_HO_GD=:ho", rows[half:],
                     lambda r: dict(cd=r["MA_CD"], ho=r["MA_HO_GD"]))
        for r in rows[:15] + rows[half:half + 15]:
            self.manifest.append(dict(scenario="reject", ma_cd=r["MA_CD"], expect=dict(exists=False)))

    def change_ma(self, n):
        rows = self._pick(VALID, n)
        seq = self._max_seq("7")
        new = {r["MA_CD"]: f"7{seq + i:09d}" for i, r in enumerate(rows)}
        self._update(f"UPDATE {T} SET MA_CD_OLD=MA_CD, MA_CD=:new, ID=:newid, MODIFY_DATE=SYSDATE WHERE MA_CD=:cd AND MA_HO_GD=:ho", rows,
                     lambda r: dict(new=new[r["MA_CD"]], newid=f"{new[r['MA_CD']]}-{r['MA_HO_GD']}", cd=r["MA_CD"], ho=r["MA_HO_GD"]))
        for r in rows[:30]:
            self.manifest.append(dict(scenario="change_ma_new", ma_cd=new[r["MA_CD"]], expect=dict(exists=True, ma_cu=r["MA_CD"])))
            self.manifest.append(dict(scenario="change_ma_old", ma_cd=r["MA_CD"], expect=dict(exists=False)))

    def delete_latest(self, n):
        sql = (f"SELECT MA_CD FROM (SELECT MA_CD, COUNT(*) OVER (PARTITION BY MA_CD) C FROM {T} WHERE MA_CD >= :s AND MA_CD < '9') "
               f"WHERE C >= 2 AND MA_CD < '7' GROUP BY MA_CD FETCH FIRST :n ROWS ONLY")
        cds = [r["MA_CD"] for r in _fetch(self.cur, sql, s=self._rand_start(n), n=n * 2) if r["MA_CD"] not in self.used][:n]
        self.used.update(cds)
        for cd in cds:
            rows = _fetch(self.cur, f"SELECT * FROM {T} WHERE MA_CD = :c", c=cd)
            best = ref.pick_latest(rows)
            if best is None:
                continue
            rest = [r for r in rows if r["MA_HO_GD"] != best["MA_HO_GD"]]
            nxt = ref.pick_latest(rest)
            self.cur.execute(f"DELETE FROM {T} WHERE MA_CD=:c AND MA_HO_GD=:h", c=cd, h=best["MA_HO_GD"])
            self.manifest.append(dict(scenario="delete_latest", ma_cd=cd,
                                      expect=dict(exists=nxt is not None, ho_gd=nxt["MA_HO_GD"] if nxt else None)))
            if self.sleep and len(self.manifest) % self.chunk == 0:
                self._commit_pause()
        self.conn.commit()

    def delete_all(self, n):
        rows = self._pick(VALID, n)
        self._update(f"DELETE FROM {T} WHERE MA_CD=:cd AND MA_HO_GD=:ho", rows, lambda r: dict(cd=r["MA_CD"], ho=r["MA_HO_GD"]))
        for r in rows[:30]:
            self.manifest.append(dict(scenario="delete_all", ma_cd=r["MA_CD"], expect=dict(exists=False)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", default="all", help="all | " + " | ".join(ALL) + " (phân cách bằng dấu phẩy)")
    ap.add_argument("--insert", type=int, default=20000, help="số công dân mới")
    ap.add_argument("--update", type=int, default=1000, help="số bản ghi cho MỖI kịch bản cập nhật/xóa")
    ap.add_argument("--chunk", type=int, default=2000, help="số dòng mỗi lần commit (điều tiết tải lên CDC)")
    ap.add_argument("--sleep", type=float, default=0.5, help="nghỉ giữa các lần commit (giây)")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--target", choices=["vss", "mirror"], default="vss", help="mirror: sửa thẳng bảng Mirror")
    ap.add_argument("--manifest", default="out/delta_manifest.json")
    a = ap.parse_args()
    sc = ALL if a.scenario == "all" else [s.strip() for s in a.scenario.split(",")]
    now = datetime.now().replace(microsecond=0)
    rng = random.Random(a.seed if a.seed is not None else int(time.time()))
    dm = G.dm_rows(G.load_dm())
    conn = db.connect(a.target)
    dbhc_tbl = "VSS.VSS_DM_DBHC"
    if a.target == "mirror":
        set_target("VSS_HO_GD_NHANKHAU")
        dbhc_tbl = "VSS_DM_DBHC"
    with conn.cursor() as cur:   # dùng đúng danh mục đang có ở nguồn (đã có MO_HO cài thêm khi seed)
        cur.execute("SELECT ID,MA_DBHC,MA_CHA,TEN,TINH_TRANG,DM_VUNG_ID,MA_DBHC_SN,IS_LT,IS_CAP_XA,NGAY_HIEU_LUC,NGAY_HET_HIEU_LUC FROM " + dbhc_tbl)
        cols = [d[0] for d in cur.description]
        dm["dbhc"] = [dict(zip(cols, r)) for r in cur.fetchall()]
    geo = G.build_geo(dm["dbhc"], now, [])
    d = Delta(conn, rng, now, geo, dm, a.chunk, a.sleep, dm["dbhc"])
    for s in sc:
        t0 = time.time()
        getattr(d, s)(a.insert if s == "insert" else a.update)
        print(f"[{s}] xong trong {time.time() - t0:.1f}s")
    Path(a.manifest).parent.mkdir(parents=True, exist_ok=True)
    Path(a.manifest).write_text(json.dumps(d.manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Manifest: {a.manifest} ({len(d.manifest)} kỳ vọng). Chờ ~1-2 phút rồi chạy verify_delta.")


if __name__ == "__main__":
    main()
