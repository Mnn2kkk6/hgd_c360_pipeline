"""Nạp 3 file danh mục CSV và dựng cấu trúc địa bàn (tỉnh/xã đang hiệu lực + mã cũ trong MA_DBHC_SN)."""
from __future__ import annotations
import random
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import pandas as pd

from hgd.config import ROOT

DM_DIR = ROOT / "data" / "dm"


def _find(prefix: str, dm_dir: Path = DM_DIR) -> Path:
    fs = sorted(dm_dir.glob(prefix + "*.csv"))
    if not fs:
        raise FileNotFoundError(f"Không thấy {prefix}*.csv trong {dm_dir}")
    return fs[-1]


def load_dm(dm_dir: Path = DM_DIR) -> dict[str, pd.DataFrame]:
    return {
        "dantoc": pd.read_csv(_find("DM_DAN_TOC", dm_dir), dtype=str, keep_default_na=False, na_values=[""]),
        "quoc_tich": pd.read_csv(_find("DM_QUOC_TICH", dm_dir), dtype=str, keep_default_na=False, na_values=[""]),
        "dbhc": pd.read_csv(_find("DM_DBHC", dm_dir), dtype=str, keep_default_na=False, na_values=[""]),
    }


def _py(v):
    """NaN/NaT/'' -> None, giữ nguyên kiểu còn lại."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def _ts(v):
    v = _py(v)
    return None if v is None else pd.to_datetime(v).to_pydatetime()


def _int(v):
    v = _py(v)
    return None if v is None else int(v)


def dm_rows(dm: dict[str, pd.DataFrame]) -> dict[str, list[dict]]:
    """Chuyển DataFrame -> list dict đúng kiểu để insert Oracle / dùng bản tham chiếu."""
    dantoc = [dict(ID=int(r.ID), CAN_CU=r.CAN_CU if _py(r.CAN_CU) is not None else " ", MA=r.MA,
                   NGAY_HL=_ts(r.NGAY_HL), NGAY_HET_HL=_ts(r.NGAY_HET_HL), TEN=r.TEN) for r in dm["dantoc"].itertuples()]
    qt = [dict(ID=int(r.ID), MA=r.MA, TEN=r.TEN) for r in dm["quoc_tich"].itertuples()]
    dbhc = [dict(ID=int(r.ID), MA_DBHC=_py(r.MA_DBHC), MA_CHA=_py(r.MA_CHA), TEN=_py(r.TEN), TINH_TRANG=_py(r.TINH_TRANG),
                 DM_VUNG_ID=_int(r.DM_VUNG_ID), MA_DBHC_SN=_py(r.MA_DBHC_SN), IS_LT=_int(r.IS_LT), IS_CAP_XA=_int(r.IS_CAP_XA),
                 NGAY_HIEU_LUC=_ts(r.NGAY_HIEU_LUC), NGAY_HET_HIEU_LUC=_ts(r.NGAY_HET_HIEU_LUC)) for r in dm["dbhc"].itertuples()]
    return {"dantoc": dantoc, "quoc_tich": qt, "dbhc": dbhc}


def inject_mo_ho(dbhc: list[dict], k: int, rng: random.Random, now: datetime) -> list[str]:
    """File thật không có trường hợp xã cũ bị tách (MO_HO). Chủ đích tạo k mã cũ giả (9xxxx) nằm trong
    MA_DBHC_SN của 2 xã mới: 2/3 số ca cùng tỉnh, 1/3 khác tỉnh. Sửa trực tiếp list `dbhc`, trả danh sách mã giả."""
    act = [r for r in dbhc if r["NGAY_HET_HIEU_LUC"] and r["NGAY_HET_HIEU_LUC"] > now and r["MA_DBHC"] and len(r["MA_DBHC"]) == 5]
    by_t: dict[str, list[dict]] = {}
    for r in act:
        by_t.setdefault(r["MA_CHA"], []).append(r)
    used = {r["MA_DBHC"] for r in dbhc}
    for r in dbhc:
        used.update((r["MA_DBHC_SN"] or "").split(";"))
    codes: list[str] = []
    tinhs = [t for t, xs in by_t.items() if len(xs) >= 2]
    for i in range(k):
        while True:
            c = f"9{rng.randint(0, 9999):04d}"
            if c not in used:
                break
        used.add(c)
        if i % 3 == 2:
            t1, t2 = rng.sample(tinhs, 2)
            a, b = rng.choice(by_t[t1]), rng.choice(by_t[t2])
        else:
            a, b = rng.sample(by_t[rng.choice(tinhs)], 2)
        for r in (a, b):
            r["MA_DBHC_SN"] = (r["MA_DBHC_SN"] + ";" if r["MA_DBHC_SN"] else r["MA_DBHC"] + ";") + c
        codes.append(c)
    return codes


@dataclass
class Geo:
    tinh: dict[str, dict] = field(default_factory=dict)       # '01' -> {ten, sn_old: [...]}
    xa: list[dict] = field(default_factory=list)              # {ma, tinh, ten, sn_old}
    xa_by_tinh: dict[str, list[dict]] = field(default_factory=dict)
    mo_ho_codes: list[str] = field(default_factory=list)


def build_geo(dbhc: list[dict], now: datetime, mo_ho_codes: list[str] | None = None) -> Geo:
    g = Geo(mo_ho_codes=mo_ho_codes or [])
    for r in dbhc:
        if str(r["TINH_TRANG"]) != "1" or not (r["NGAY_HET_HIEU_LUC"] is None or r["NGAY_HET_HIEU_LUC"] > now):
            continue
        ma = (r["MA_DBHC"] or "").strip()
        sn = [c for c in (r["MA_DBHC_SN"] or "").split(";") if c and c != ma]
        if len(ma) <= 2:
            g.tinh[ma.zfill(2)] = dict(ten=r["TEN"], sn_old=[c.zfill(2) for c in sn])
        elif len(ma) == 5:
            x = dict(ma=ma, tinh=(r["MA_CHA"] or "").zfill(2), ten=r["TEN"], sn_old=sn)
            g.xa.append(x)
    for x in g.xa:
        g.xa_by_tinh.setdefault(x["tinh"], []).append(x)
    return g
