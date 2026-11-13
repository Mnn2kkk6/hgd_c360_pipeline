"""Gọi thủ tục trên schema C360:  python -m hgd.tools.call_proc PRC_C360_FULL_LOAD [tham số...]"""
import sys
import time
from hgd import db


def main():
    name, *args = sys.argv[1:]
    conv = [int(a) if a.lstrip("-").isdigit() else a for a in args]
    t0 = time.time()
    conn = db.connect("c360")
    with conn.cursor() as cur:
        cur.callproc(name, conv)
    conn.commit()
    print(f"{name} xong trong {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
