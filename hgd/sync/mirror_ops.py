"""Thao tác trên vùng Mirror quanh Full Load.

  python -m hgd.sync.mirror_ops prepare        # trước Full Load: tắt trigger, bỏ các index (nạp direct-path nhanh)
  python -m hgd.sync.mirror_ops create-index   # sau Full Load: unique (MA_CD, MA_HO_GD) + index MODIFY_DATE + index SYNC_TS
  python -m hgd.sync.mirror_ops create-trigger # tạo/bật trigger Queue + SYNC_TS (SAU Full Load C360, TRƯỚC khi bật sink CDC)
Mọi lệnh chạy lại an toàn (bỏ qua "đã tồn tại"/"chưa tồn tại").
"""
import argparse
import oracledb
from hgd import db
from hgd.config import ROOT

INDEXES = {
    "UX_M_HGD_NK_MACD_HOGD": "CREATE UNIQUE INDEX UX_M_HGD_NK_MACD_HOGD ON VSS_HO_GD_NHANKHAU (MA_CD, MA_HO_GD) PARALLEL {p} NOLOGGING",
    "IX_M_HGD_NK_MODIFY": "CREATE INDEX IX_M_HGD_NK_MODIFY ON VSS_HO_GD_NHANKHAU (MODIFY_DATE) PARALLEL {p} NOLOGGING",
    "IX_M_HGD_NK_SYNC": "CREATE INDEX IX_M_HGD_NK_SYNC ON VSS_HO_GD_NHANKHAU (SYNC_TS) PARALLEL {p} NOLOGGING",
}


def _try(cur, sql, ignore=()):
    try:
        cur.execute(sql)
        return True
    except oracledb.DatabaseError as e:
        if e.args[0].code in ignore:
            return False
        raise


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["prepare", "create-index", "create-trigger"])
    ap.add_argument("--parallel", type=int, default=4)
    a = ap.parse_args()
    conn = db.connect("mirror")
    cur = conn.cursor()
    if a.cmd == "prepare":
        _try(cur, "ALTER TRIGGER TRG_HGD_NK_QUEUE DISABLE", ignore=(4080,))      # ORA-04080: trigger chưa tồn tại
        for name in INDEXES:
            _try(cur, f"DROP INDEX {name}", ignore=(1418,))                      # ORA-01418: index chưa tồn tại
        print("Đã tắt trigger và bỏ các index (nếu có).")
    elif a.cmd == "create-index":
        for name, ddl in INDEXES.items():
            try:
                created = _try(cur, ddl.format(p=a.parallel), ignore=(955, 1408))   # đã tồn tại / đã có index cùng cột
                if created:
                    cur.execute(f"ALTER INDEX {name} NOPARALLEL LOGGING")
                print(f"  {name}: {'đã tạo' if created else 'đã có sẵn'}")
            except oracledb.DatabaseError as e:
                if e.args[0].code == 1452:
                    print("TRÙNG KHÓA (MA_CD, MA_HO_GD) trong Mirror -> Full Load bị lặp hoặc nguồn có trùng. Kiểm tra:\n"
                          "  SELECT MA_CD, MA_HO_GD, COUNT(*) FROM VSS_HO_GD_NHANKHAU GROUP BY MA_CD, MA_HO_GD HAVING COUNT(*) > 1;")
                raise
    else:
        db.run_sql_file(conn, ROOT / "sql" / "22_mirror_trigger.sql")
        _try(cur, "ALTER TRIGGER TRG_HGD_NK_QUEUE ENABLE")
        print("Trigger TRG_HGD_NK_QUEUE đã bật.")
    conn.commit()


if __name__ == "__main__":
    main()
