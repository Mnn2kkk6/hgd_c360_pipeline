"""Chạy 1 hoặc nhiều file SQL: python -m hgd.tools.run_sql sql/10_vss_source.sql --as vss"""
import argparse
from hgd import db


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--as", dest="schema", required=True, help="vss | mirror | c360 | sys-pdb | sys-root")
    a = ap.parse_args()
    conn = db.connect(a.schema)
    for f in a.files:
        print(f"== {f} (as {a.schema})")
        print(f"   {db.run_sql_file(conn, f)} lệnh")


if __name__ == "__main__":
    main()
