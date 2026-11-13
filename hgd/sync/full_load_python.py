"""FULL LOAD bằng Python thuần (oracledb), KHÔNG cần Spark/Java/driver JDBC. Dùng khi mạng chặn Maven hoặc dữ liệu lab nhỏ.

  python -m hgd.sync.full_load_python                # tất cả bảng, 4 luồng
  python -m hgd.sync.full_load_python --threads 8 --tables main
Bảng lớn chia N luồng theo ORA_HASH(MA_CD) (mỗi luồng 1 kết nối đọc VSS + 1 kết nối ghi MIRROR). TRUNCATE rồi nạp -> chạy lại không nhân đôi.
Lưu ý: mỗi luồng quét bảng nguồn 1 lần; với hàng chục triệu dòng nên dùng bản Spark (full_load_spark.py).
"""
from __future__ import annotations
import argparse
import time
from concurrent.futures import ThreadPoolExecutor

from hgd import db

DM_TABLES = ["VSS_DM_DANTOC", "VSS_DM_QUOC_TICH", "VSS_DM_DBHC"]
MAIN = "VSS_HO_GD_NHANKHAU"


def mirror_columns(table: str) -> list[str]:
    conn = db.connect("mirror")
    with conn.cursor() as cur:
        cur.execute("SELECT column_name FROM user_tab_columns WHERE table_name = :t ORDER BY column_id", t=table)
        cols = [r[0] for r in cur.fetchall()]
    conn.close()
    if not cols:
        raise RuntimeError(f"Mirror chưa có bảng {table}")
    return cols


def copy_part(table: str, cols: list[str], part: int | None, parts: int, batch: int) -> int:
    src, dst = db.connect("vss"), db.connect("mirror")
    where = f" WHERE ORA_HASH(MA_CD, {parts - 1}) = {part}" if part is not None else ""
    sel = f"SELECT {','.join(cols)} FROM VSS.{table}{where}"
    ins = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(':' + str(i + 1) for i in range(len(cols)))})"
    n = 0
    with src.cursor() as rc, dst.cursor() as wc:
        rc.arraysize = batch
        rc.execute(sel)
        while True:
            rows = rc.fetchmany(batch)
            if not rows:
                break
            wc.executemany(ins, rows)
            dst.commit()
            n += len(rows)
    src.close(); dst.close()
    return n


def count(schema: str, owner: str | None, table: str) -> int:
    conn = db.connect(schema)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {owner + '.' if owner else ''}{table}")
        n = cur.fetchone()[0]
    conn.close()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", default="all", help="all | main | dm")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--no-truncate", action="store_true")
    a = ap.parse_args()
    tables = {"all": DM_TABLES + [MAIN], "main": [MAIN], "dm": DM_TABLES}[a.tables]
    if not a.no_truncate:
        conn = db.connect("mirror")
        with conn.cursor() as cur:
            for t in tables:
                cur.execute(f"TRUNCATE TABLE {t}")
        conn.close()
    for t in tables:
        t0 = time.time()
        cols = mirror_columns(t)
        if t == MAIN and a.threads > 1:
            with ThreadPoolExecutor(a.threads) as ex:
                futs = [ex.submit(copy_part, t, cols, p, a.threads, a.batch) for p in range(a.threads)]
                total = sum(f.result() for f in futs)          # f.result() ném lại lỗi của luồng nếu có
        else:
            total = copy_part(t, cols, None, 1, a.batch)
        s, m = count("vss", "VSS", t), count("mirror", None, t)
        print(f"{t:22s} đã chép={total:>9,} nguồn={s:>9,} mirror={m:>9,} {'KHỚP' if s == m else 'LỆCH'}  ({time.time() - t0:.0f}s)")
        if s != m:
            raise SystemExit(f"{t}: lệch số dòng")


if __name__ == "__main__":
    main()
