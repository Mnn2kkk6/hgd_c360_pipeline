"""Dựng toàn bộ đối tượng DB theo đúng thứ tự phụ thuộc:  python -m hgd.tools.init_db [--skip-debezium]"""
import argparse
from hgd import db
from hgd.config import ROOT

PLAN = [
    ("sys-pdb", ["00_sys_users.sql"]),
    ("sys-root", ["01_sys_debezium.sql"]),
    ("vss", ["10_vss_source.sql"]),
    ("c360", ["20_c360_tables.sql"]),
    ("mirror", ["21_mirror_tables.sql"]),
    ("sys-pdb", ["02_grants.sql"]),
    ("c360", ["30_pkg_util.sql", "31_map_dbhc.sql", "32_view_normalized.sql", "33_procs_sync.sql", "40_datamart_bi.sql"]),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-debezium", action="store_true", help="bỏ qua tạo user Debezium (khi chưa bật ARCHIVELOG)")
    a = ap.parse_args()
    for schema, files in PLAN:
        if a.skip_debezium and files == ["01_sys_debezium.sql"]:
            continue
        conn = db.connect(schema)
        for f in files:
            print(f"== {f} (as {schema})")
            print(f"   {db.run_sql_file(conn, ROOT / 'sql' / f)} lệnh")
        conn.close()
    print("Xong. Lưu ý: trigger Mirror (sql/22) tạo SAU Full Load qua: python -m hgd.sync.mirror_ops create-trigger")


if __name__ == "__main__":
    main()
