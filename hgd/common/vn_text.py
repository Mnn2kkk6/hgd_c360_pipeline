"""Chuẩn hóa tiếng Việt: bỏ dấu + viết hoa. Dùng chung cho API và bản tham chiếu; khớp PKG_C360_UTIL.khong_dau ở Oracle."""
from __future__ import annotations
import re
import unicodedata

_WS = re.compile(r"\s+")


def norm_name(s: str | None) -> str | None:
    if s is None:
        return None
    r = _WS.sub(" ", s).strip()
    return r or None


def khong_dau(s: str | None) -> str | None:
    r = norm_name(s)
    if r is None:
        return None
    r = unicodedata.normalize("NFC", r).replace("Đ", "D").replace("đ", "d")
    r = unicodedata.normalize("NFD", r)
    r = "".join(c for c in r if unicodedata.category(c) != "Mn")
    return unicodedata.normalize("NFC", r).upper()
