"""Bản THAM CHIẾU bằng Python của logic chuẩn hóa C360 (độc lập với PL/SQL).

Mục đích: (1) unit test quy tắc nghiệp vụ, (2) kiểm tra bao phủ của bộ sinh dữ liệu,
(3) đối soát MẪU trong recon (so kết quả Oracle với bản tham chiếu) và verify kịch bản biến động.
KHÔNG dùng để chạy pipeline thật: pipeline dùng view V_C360_NORMALIZED (Oracle) làm nguồn logic duy nhất.
"""
from __future__ import annotations
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Iterable

from hgd.common.vn_text import khong_dau, norm_name

LOAI_BO_TYPEID = {"GT", "GM", "GG", "GK"}
LOAI_BO_DUYET = {1, 3, 4}


# ---------------------------------------------------------------- hàm làm sạch
def norm_tinh_key(p: str | None) -> str | None:
    if p is None:
        return None
    v = p.strip().upper()
    if not v:
        return None
    v = re.sub(r"TTT$", "", v)
    if not re.fullmatch(r"[0-9]{1,3}", v) or int(v) == 0:
        return None
    return v.zfill(3)


def norm_xa_key(p: str | None) -> str | None:
    if p is None:
        return None
    v = p.strip()
    if not re.fullmatch(r"[0-9]{1,5}", v) or int(v) == 0:
        return None
    return v.zfill(5)


def norm_ngay_sinh(p: int | float | None, today: date | None = None) -> str | None:
    today = today or date.today()
    if p is None or p != int(p) or p <= 0:
        return None
    s = str(int(p))
    if len(s) == 4:
        y, m, d = int(s), 0, 0
    elif len(s) == 6:
        y, m, d = int(s[:4]), int(s[4:6]), 0
    elif len(s) == 8:
        y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
    else:
        return None
    if y < 1900 or y > today.year:
        return None
    if m == 0:
        return str(y) if d == 0 else None
    if m > 12:
        return None
    if d == 0:
        if y == today.year and m > today.month:
            return None
        return f"{y}{m:02d}"
    try:
        full = date(y, m, d)
    except ValueError:
        return None
    if full > today:
        return None
    return f"{y}{m:02d}{d:02d}"


def norm_phone(p: str | None) -> str | None:
    if p is None:
        return None
    v = re.sub(r"[^0-9]", "", p)
    if not v:
        return None
    if v[:2] == "84" and len(v) in (11, 12):
        v = v[2:] if v[2] == "0" else "0" + v[2:]
    elif v[0] != "0" and len(v) == 9:
        v = "0" + v
    return v if re.fullmatch(r"0[0-9]{9,10}", v) else None


def pick_dinh_danh(cccd: str | None, cmnd: str | None) -> str | None:
    for x in (cccd, cmnd):
        if x is not None and re.fullmatch(r"[0-9]{12}", x.strip()):
            return x.strip()
    return None


# ---------------------------------------------------------------- bảng Map địa bàn
def _is_active(r: dict, now: datetime) -> bool:
    if str(r.get("TINH_TRANG")) != "1":
        return False
    hl, het = r.get("NGAY_HIEU_LUC"), r.get("NGAY_HET_HIEU_LUC")
    return (hl is None or hl <= now) and (het is None or het > now)


def build_maps(dbhc_rows: Iterable[dict], now: datetime | None = None) -> tuple[dict, dict]:
    """Trả (map_tinh, map_xa). map_tinh[key3] = {ma_tinh, ten_tinh, nguon}; map_xa[key5] = {ma_xa, ma_tinh, ten_xa, ten_tinh, nguon, so_xa_moi}"""
    now = now or datetime.now()
    tinh_hl, xa_hl = [], []
    for r in dbhc_rows:
        if not _is_active(r, now):
            continue
        ma = (r.get("MA_DBHC") or "").strip()
        if ma.upper().endswith("TTT") or len(ma) <= 2:
            k = norm_tinh_key(ma)
            if k:
                tinh_hl.append((k, r.get("TEN"), r.get("MA_DBHC_SN")))
        elif re.fullmatch(r"[0-9]{5}", ma):
            xa_hl.append((ma, norm_tinh_key(r.get("MA_CHA")), r.get("TEN"), r.get("MA_DBHC_SN")))
    ten_tinh = {k: t for k, t, _ in tinh_hl}

    map_tinh: dict[str, dict] = {k: dict(ma_tinh=k, ten_tinh=t, nguon="HIEN_HANH") for k, t, _ in tinh_hl}
    sn_t: dict[str, set] = defaultdict(set)
    for k, _, sn in tinh_hl:
        for c in (sn or "").split(";"):
            ck = norm_tinh_key(c)
            if ck and ck not in map_tinh:
                sn_t[ck].add(k)
    for ck, news in sn_t.items():
        if len(news) == 1:
            n = next(iter(news))
            map_tinh[ck] = dict(ma_tinh=n, ten_tinh=ten_tinh[n], nguon="SAP_NHAP")
        else:
            map_tinh[ck] = dict(ma_tinh=None, ten_tinh=None, nguon="MO_HO")

    map_xa: dict[str, dict] = {}
    for ma, mt, ten, _ in xa_hl:
        map_xa[ma] = dict(ma_xa=ma, ma_tinh=mt, ten_xa=ten, ten_tinh=ten_tinh.get(mt), nguon="HIEN_HANH", so_xa_moi=1)
    sn_x: dict[str, set] = defaultdict(set)
    info = {ma: (mt, ten) for ma, mt, ten, _ in xa_hl}
    for ma, _, _, sn in xa_hl:
        for c in (sn or "").split(";"):
            ck = norm_xa_key(c)
            if ck and ck not in map_xa:
                sn_x[ck].add(ma)
    for ck, news in sn_x.items():
        if len(news) == 1:
            n = next(iter(news))
            mt, ten = info[n]
            map_xa[ck] = dict(ma_xa=n, ma_tinh=mt, ten_xa=ten, ten_tinh=ten_tinh.get(mt), nguon="SAP_NHAP", so_xa_moi=1)
        else:
            tinhs = {info[n][0] for n in news}
            mt = next(iter(tinhs)) if len(tinhs) == 1 else None
            map_xa[ck] = dict(ma_xa=None, ma_tinh=mt, ten_xa=None, ten_tinh=ten_tinh.get(mt) if mt else None,
                              nguon="MO_HO", so_xa_moi=len(news))
    return map_tinh, map_xa


def resolve_address(raw_tinh: str | None, raw_xa: str | None, map_tinh: dict, map_xa: dict, strict: bool) -> tuple[dict, list[str]]:
    """strict=True: địa chỉ thường trú (luôn gắn cờ nếu không map). False: LH/KS (chỉ gắn cờ khi có nhập mà sai)."""
    mx = map_xa.get(norm_xa_key(raw_xa)) if norm_xa_key(raw_xa) else None
    mt = map_tinh.get(norm_tinh_key(raw_tinh)) if norm_tinh_key(raw_tinh) else None
    f_tinh = (mx or {}).get("ma_tinh") or (mt or {}).get("ma_tinh")
    ten_tinh = (mx or {}).get("ten_tinh") if (mx or {}).get("ma_tinh") else (mt or {}).get("ten_tinh")
    res = dict(ma_tinh=f_tinh, ten_tinh=ten_tinh, ma_xa=(mx or {}).get("ma_xa"), ten_xa=(mx or {}).get("ten_xa"),
               nguon=(mx or {}).get("nguon"))
    lech = bool(mx and mx.get("ma_tinh") and mt and mt.get("ma_tinh") and mx["ma_tinh"] != mt["ma_tinh"])
    flags: list[str] = []
    if strict:
        if f_tinh is None: flags.append("TINH_KHONG_MAP")
        if mx is None: flags.append("XA_KHONG_MAP")
    else:
        if (raw_tinh is not None or raw_xa is not None) and f_tinh is None: flags.append("TINH_KHONG_MAP")
        if raw_xa is not None and mx is None: flags.append("XA_KHONG_MAP")
    if mx and mx["nguon"] == "MO_HO": flags.append("XA_MO_HO")
    if lech: flags.append("TINH_LECH_XA")
    return res, flags


# ---------------------------------------------------------------- lọc & khử trùng
def is_valid_row(r: dict) -> bool:
    return (r.get("TYPEID") or "ZZ") not in LOAI_BO_TYPEID and (r.get("TRANG_THAI_DUYET") or 0) not in LOAI_BO_DUYET


def _sort_key(r: dict):
    def k(v, neg=True):
        return (v is not None, v)
    md = r.get("MODIFY_DATE"); cd = r.get("CREATED_DATE"); i = r.get("ID")
    return (md is not None, md or datetime.min, cd is not None, cd or datetime.min, i is not None, i or "")


def pick_latest(rows: Iterable[dict]) -> dict | None:
    valid = [r for r in rows if is_valid_row(r)]
    return max(valid, key=_sort_key) if valid else None


def normalize_row(r: dict, map_tinh: dict, map_xa: dict, qt_by_ma: dict, qt_by_id: dict, dantoc_by_id: dict,
                  today: date | None = None) -> dict:
    flags: list[str] = []
    out: dict[str, Any] = {}
    out["MA_SO_BHXH"] = r["MA_CD"]
    out["MA_SO_BHXH_CU"] = r.get("MA_CD_OLD")
    out["SO_DINH_DANH"] = pick_dinh_danh(r.get("SO_CCCD"), r.get("SO_CMTND"))
    out["HO_TEN"] = norm_name(r.get("HO_TEN"))
    out["HO_TEN_KHONG_DAU"] = khong_dau(r.get("HO_TEN"))
    out["NGAY_SINH"] = norm_ngay_sinh(r.get("NGAY_SINH"), today)
    out["NGAY_SINH_GOC"] = r.get("NGAY_SINH")
    out["SO_DIEN_THOAI"] = norm_phone(r.get("PHONE_NUMBER"))
    out["MA_HO_GD"] = r.get("MA_HO_GD")
    for suf, strict in (("", True), ("_LH", False), ("_KS", False)):
        res, fl = resolve_address(r.get("MA_TINH" + suf), r.get("MA_XA" + suf), map_tinh, map_xa, strict)
        out["MA_TINH" + suf], out["TEN_TINH" + suf] = res["ma_tinh"], res["ten_tinh"]
        out["MA_XA" + suf], out["TEN_XA" + suf], out["NGUON_MAP_XA" + suf] = res["ma_xa"], res["ten_xa"], res["nguon"]
        flags += [f + suf for f in fl]
    # quốc tịch / dân tộc
    qt = r.get("QUOC_TICH"); q = None
    if qt is not None and qt.strip():
        v = qt.strip()
        q = qt_by_id.get(int(v)) if re.fullmatch(r"[0-9]{1,18}", v) else qt_by_ma.get(v.upper())
    out["QUOC_TICH_MA"], out["QUOC_TICH_TEN"] = (q or {}).get("MA"), (q or {}).get("TEN")
    dt = dantoc_by_id.get(r.get("DM_DAN_TOC_ID")) if r.get("DM_DAN_TOC_ID") is not None else None
    out["DAN_TOC_MA"], out["DAN_TOC_TEN"] = (dt or {}).get("MA"), (dt or {}).get("TEN")
    # cờ chất lượng (thứ tự khớp view: địa chỉ TT, LH, KS rồi các cờ khác)
    order_addr = []
    for suf in ("", "_LH", "_KS"):
        for base in ("TINH_KHONG_MAP", "XA_KHONG_MAP", "XA_MO_HO", "TINH_LECH_XA"):
            if base + suf in flags:
                order_addr.append(base + suf)
    other = []
    if out["NGAY_SINH"] is None: other.append("NGAY_SINH_LOI")
    if r.get("PHONE_NUMBER") is not None and out["SO_DIEN_THOAI"] is None: other.append("SDT_LOI")
    if out["SO_DINH_DANH"] is None: other.append("KHONG_SO_DINH_DANH")
    if qt is not None and qt.strip() and q is None: other.append("QUOC_TICH_KHONG_MAP")
    if r.get("DM_DAN_TOC_ID") is not None and dt is None: other.append("DAN_TOC_KHONG_MAP")
    allf = order_addr + other
    out["DQ_FLAGS"] = ";".join(allf) if allf else None
    return out


def build_c360(mirror_rows: Iterable[dict], dbhc_rows, qt_rows, dantoc_rows, today: date | None = None) -> dict[str, dict]:
    """Dựng C360 kỳ vọng (dict theo MA_SO_BHXH) từ các dòng Mirror."""
    mt, mx = build_maps(dbhc_rows)
    qt_ma = {r["MA"]: r for r in qt_rows}
    qt_id = {int(r["ID"]): r for r in qt_rows}
    dt_id = {int(r["ID"]): r for r in dantoc_rows}
    by_cd: dict[str, list] = defaultdict(list)
    for r in mirror_rows:
        by_cd[r["MA_CD"]].append(r)
    res = {}
    for cd, rows in by_cd.items():
        best = pick_latest(rows)
        if best:
            res[cd] = normalize_row(best, mt, mx, qt_ma, qt_id, dt_id, today)
    return res
