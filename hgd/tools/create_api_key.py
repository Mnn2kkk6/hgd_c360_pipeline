"""Tạo API key: in key MỘT LẦN, chỉ lưu SHA-256 trong C360_API_KEY.
   python -m hgd.tools.create_api_key --name "He thong A" --scopes lookup,batch,search,changes"""
import argparse
import hashlib
import secrets
from hgd import db


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--scopes", default="lookup,batch,search,changes")
    a = ap.parse_args()
    key = "hgd_" + secrets.token_urlsafe(32)
    conn = db.connect("c360")
    with conn.cursor() as cur:
        cur.execute("INSERT INTO C360_API_KEY (KEY_HASH, TEN_HE_THONG, SCOPES) VALUES (:h, :n, :s)",
                    h=hashlib.sha256(key.encode()).hexdigest(), n=a.name, s=a.scopes)
    conn.commit()
    print(f"API key (lưu lại, sẽ không hiển thị lần nữa):\n{key}")


if __name__ == "__main__":
    main()
