"""Nạp dữ liệu nền vào nguồn VSS: 3 danh mục (từ CSV thật) + nhân khẩu giả lập có dữ liệu bẩn.

  python -m hgd.generator.seed --people 200000 --seed 42 --truncate
"""
from __future__ import annotations
import argparse
import random
import time
from datetime import datetime

from hgd import db
from . import geo as G
from .people import SOURCE_COLS, gen_people, golden_cases

INS_MAIN = f"INSERT INTO VSS.VSS_HO_GD_NHANKHAU ({','.join(SOURCE_COLS)}) VALUES ({','.join(':' + c for c in SOURCE_COLS)})"


def load_dm_tables(conn, rows: dict) -> None:
    with conn.cursor() as cur:
        for t in ("VSS_DM_DANTOC", "VSS_DM_QUOC_TICH", "VSS_DM_DBHC"):
            cur.execute(f"DELETE FROM VSS.{t}")
        cur.executemany("INSERT INTO VSS.VSS_DM_DANTOC (ID,CAN_CU,MA,NGAY_HL,NGAY_HET_HL,TEN) VALUES (:ID,:CAN_CU,:MA,:NGAY_HL,:NGAY_HET_HL,:TEN)", rows["dantoc"])
        cur.executemany("INSERT INTO VSS.VSS_DM_QUOC_TICH (ID,MA,TEN) VALUES (:ID,:MA,:TEN)", rows["quoc_tich"])
        cols = ["ID", "MA_DBHC", "MA_CHA", "TEN", "TINH_TRANG", "DM_VUNG_ID", "MA_DBHC_SN", "IS_LT", "IS_CAP_XA", "NGAY_HIEU_LUC", "NGAY_HET_HIEU_LUC"]
        cur.executemany(f"INSERT INTO VSS.VSS_DM_DBHC ({','.join(cols)}) VALUES ({','.join(':' + c for c in cols)})", rows["dbhc"])
    conn.commit()


def insert_main(conn, rows: list[dict], batch: int = 10000, sleep: float = 0.0) -> int:
    n = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch):
            chunk = rows[i:i + batch]
            cur.executemany(INS_MAIN, chunk)
            conn.commit()
            n += len(chunk)
            if sleep:
                time.sleep(sleep)
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--people", type=int, default=100000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--mo-ho", type=int, default=6, help="số mã xã cũ giả bị tách sang >=2 xã mới (file thật không có ca nào)")
    ap.add_argument("--truncate", action="store_true", help="xóa dữ liệu nhân khẩu cũ trước khi nạp")
    ap.add_argument("--no-golden", action="store_true")
    ap.add_argument("--dup-rate", type=float, default=0.04)
    ap.add_argument("--now", default=None, help="YYYY-MM-DD (mặc định: hôm nay)")
    a = ap.parse_args()

    now = datetime.fromisoformat(a.now) if a.now else datetime.now().replace(microsecond=0)
    rng = random.Random(a.seed)
    rows_dm = G.dm_rows(G.load_dm())
    codes = G.inject_mo_ho(rows_dm["dbhc"], a.mo_ho, rng, now) if a.mo_ho else []
    geo = G.build_geo(rows_dm["dbhc"], now, codes)
    print(f"Địa bàn hiệu lực: {len(geo.tinh)} tỉnh, {len(geo.xa)} xã; mã MO_HO cài thêm: {codes}")

    conn = db.connect("vss")
    if a.truncate:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE VSS.VSS_HO_GD_NHANKHAU")
    load_dm_tables(conn, rows_dm)
    t0 = time.time()
    people = gen_people(a.people, geo, rows_dm, rng, now, dup_rate=a.dup_rate)
    if not a.no_golden:
        people += golden_cases(geo, now)
    n = insert_main(conn, people)
    print(f"Đã nạp {n:,} dòng nhân khẩu ({len({p['MA_CD'] for p in people}):,} công dân) trong {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
