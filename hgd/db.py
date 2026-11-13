"""Kết nối Oracle (python-oracledb thin mode, không cần Oracle Client) + chạy file SQL."""
from __future__ import annotations
import re
from pathlib import Path
import oracledb
from . import config

# Lỗi "đã tồn tại" được bỏ qua để chạy lại script DDL an toàn
IGNORABLE = {1920, 1543, 955, 1408, 2260, 2261, 1430, 1442, 4081, 942_000}


def connect(schema: str):
    """schema: vss | mirror | c360 | sys-pdb | sys-root"""
    s = schema.lower()
    if s == "sys-pdb":
        return oracledb.connect(user="sys", password=config.env("ORA_SYS_PASSWORD"), dsn=config.dsn(), mode=oracledb.AUTH_MODE_SYSDBA)
    if s == "sys-root":
        return oracledb.connect(user="sys", password=config.env("ORA_SYS_PASSWORD"), dsn=config.dsn(root=True), mode=oracledb.AUTH_MODE_SYSDBA)
    user, pwd = config.credentials(s)
    return oracledb.connect(user=user, password=pwd, dsn=config.dsn())


def split_sql(text: str) -> list[str]:
    """Tách file theo dòng chỉ gồm '/'. Bỏ khối chỉ có comment."""
    parts = re.split(r"^\s*/\s*$", text, flags=re.M)
    out = []
    for p in parts:
        body = "\n".join(l for l in p.splitlines() if not l.strip().startswith("--")).strip()
        if body:
            out.append(p.strip())
    return out


def run_sql_file(conn, path: str | Path, verbose: bool = True) -> int:
    text = Path(path).read_text(encoding="utf-8")
    n = 0
    with conn.cursor() as cur:
        for stmt in split_sql(text):
            stmt = stmt.rstrip().rstrip(";") if not re.match(r"(?is)^\s*(--.*\n)*\s*(DECLARE|BEGIN|CREATE\s+(OR\s+REPLACE\s+)?(PACKAGE|PROCEDURE|FUNCTION|TRIGGER))", stmt) else stmt
            try:
                cur.execute(stmt)
                n += 1
            except oracledb.DatabaseError as e:
                (err,) = e.args
                if getattr(err, "code", None) in IGNORABLE:
                    if verbose:
                        print(f"  [bỏ qua ORA-{err.code:05d}] {stmt.splitlines()[0][:70]}")
                    continue
                print(f"LỖI ở lệnh:\n{stmt[:400]}\n-> {e}")
                raise
        conn.commit()
    return n
