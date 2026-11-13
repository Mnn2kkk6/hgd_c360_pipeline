"""Đối chiếu C360 với manifest kỳ vọng của delta.py:  python -m hgd.tools.verify_delta out/delta_manifest.json"""
import argparse
import json
import sys
from collections import Counter
from hgd import db


def check(row: dict | None, exp: dict) -> str | None:
    """Trả None nếu đạt, ngược lại chuỗi lý do."""
    if not exp.get("exists", True):
        return None if row is None else "đáng lẽ KHÔNG còn trong C360"
    if row is None:
        return "đáng lẽ PHẢI có trong C360"
    flags = (row.get("DQ_FLAGS") or "").split(";")
    m = {"ho_gd": "MA_HO_GD", "so_dinh_danh": "SO_DINH_DANH", "ma_xa": "MA_XA", "ngay_sinh": "NGAY_SINH", "ma_cu": "MA_SO_BHXH_CU"}
    for k, col in m.items():
        if k in exp and row.get(col) != exp[k]:
            return f"{col}={row.get(col)!r}, kỳ vọng {exp[k]!r}"
    for f in exp.get("no_flag", []):
        if f in flags:
            return f"vẫn còn cờ {f}"
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("manifest")
    a = ap.parse_args()
    man = json.load(open(a.manifest, encoding="utf-8"))
    conn = db.connect("c360")
    res = Counter(); fails = []
    with conn.cursor() as cur:
        for item in man:
            cur.execute("SELECT MA_HO_GD, SO_DINH_DANH, MA_XA, NGAY_SINH, MA_SO_BHXH_CU, DQ_FLAGS FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = :c", c=item["ma_cd"])
            r = cur.fetchone()
            row = None if r is None else dict(zip(["MA_HO_GD", "SO_DINH_DANH", "MA_XA", "NGAY_SINH", "MA_SO_BHXH_CU", "DQ_FLAGS"], r))
            why = check(row, item["expect"])
            res[(item["scenario"], "PASS" if why is None else "FAIL")] += 1
            if why:
                fails.append((item["scenario"], item["ma_cd"], why))
    for sc in sorted({s for s, _ in res}):
        print(f"{sc:18s} PASS={res[(sc, 'PASS')]:3d}  FAIL={res[(sc, 'FAIL')]:3d}")
    for f in fails[:30]:
        print("  FAIL", *f)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM C360_QUEUE")
        print("Queue còn lại:", cur.fetchone()[0], "(nếu >0: NRT chưa xử lý xong, chạy lại sau ít phút)")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
