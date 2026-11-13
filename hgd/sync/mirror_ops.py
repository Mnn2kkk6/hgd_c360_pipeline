"""Thao tác trên vùng Mirror quanh Full Load.

  python -m hgd.sync.mirror_ops prepare        # trước Full Load: tắt trigger, bỏ unique index (để nạp direct-path nhanh)
  python -m hgd.sync.mirror_ops create-index   # sau Full Load: tạo Unique Index (MA_CD, MA_HO_GD) - lỗi nếu có trùng khóa
  python -m hgd.sync.mirror_ops create-trigger # tạo/bật trigger đẩy MA_CD vào Queue (SAU Full Load, TRƯỚC khi bật sink CDC)
"""
import argparse
import oracledb
from hgd import db
from hgd.config import ROOT

IDX = "UX_M_HGD_NK_MACD_HOGD"


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
        _try(cur, f"DROP INDEX {IDX}", ignore=(1418,))                           # ORA-01418: index chưa tồn tại
        print("Đã tắt trigger (nếu có) và bỏ unique index (nếu có).")
    elif a.cmd == "create-index":
        try:
            cur.execute(f"CREATE UNIQUE INDEX {IDX} ON VSS_HO_GD_NHANKHAU (MA_CD, MA_HO_GD) PARALLEL {a.parallel} NOLOGGING")
            cur.execute(f"ALTER INDEX {IDX} NOPARALLEL LOGGING")
            print("Đã tạo unique index (MA_CD, MA_HO_GD).")
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
