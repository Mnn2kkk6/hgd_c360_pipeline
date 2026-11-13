"""Sinh dữ liệu nhân khẩu giả lập có CÀI SẴN dữ liệu bẩn theo đúng các dạng trong file yêu cầu."""
from __future__ import annotations
import random
from datetime import datetime, timedelta

from hgd.common.vn_text import khong_dau
from .geo import Geo

SOURCE_COLS = ["MA_CD", "MA_CN", "MA_HO_GD", "HO_TEN", "HO_TEN_UNSIGNED", "GIOI_TINH", "NGAY_SINH", "NOI_SINH", "QH_CHU_HO",
               "SO_CMTND", "SO_CCCD", "DIA_CHI", "DC_LIEN_LAC", "EMAIL_ACC", "PHONE_NUMBER", "MA_TINH", "MA_HUYEN", "MA_XA",
               "QUOC_TICH", "IS_ALIVE", "DEAD_DATE", "CREATED_DATE", "MODIFY_DATE", "ISCHU_HO", "TYPEID", "TRANG_THAI_DUYET",
               "MA_TINH_KS", "MA_XA_KS", "MA_TINH_LH", "MA_XA_LH", "ID", "DM_DAN_TOC_ID", "MA_CD_OLD"]

HO = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng", "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý"]
DEM_NAM = ["Văn", "Hữu", "Minh", "Quốc", "Đức", "Anh", "Công", "Thanh"]
DEM_NU = ["Thị", "Ngọc", "Hồng", "Thu", "Kim", "Mỹ"]
TEN_NAM = ["An", "Bình", "Dũng", "Hải", "Hiếu", "Hùng", "Long", "Nam", "Phúc", "Quân", "Sơn", "Tuấn", "Việt", "Đạt", "Khánh"]
TEN_NU = ["Anh", "Châu", "Hà", "Hạnh", "Lan", "Linh", "Mai", "Thảo", "Trang", "Yến", "Ngân", "Ánh", "Hương", "Oanh"]


def _weighted(rng, items):
    return rng.choices([i for i, _ in items], [w for _, w in items])[0]


def addr_raw(xa: dict, geo: Geo, rng: random.Random, scale: float = 1.0) -> tuple[str | None, str | None]:
    """(MA_TINH, MA_XA) dạng nguồn: có lẫn mã cũ, mất số 0 đầu, tỉnh lệch, mã sai, rỗng..."""
    err = {"old_sn": .15, "lost0": .05, "mo_ho": .03, "lech": .03, "unknown": .02, "wrong": .03, "null": .02}
    cur = 1 - sum(v * scale for v in err.values())
    items = [("current", cur)] + [(k, v * scale) for k, v in err.items()]
    var = _weighted(rng, items)
    xa_code = xa["ma"]
    if var == "old_sn" and xa["sn_old"]:
        xa_code = rng.choice(xa["sn_old"])
    elif var == "lost0":
        xa_code = str(int(xa_code))
    elif var == "mo_ho" and geo.mo_ho_codes:
        xa_code = rng.choice(geo.mo_ho_codes)
    elif var == "unknown":
        xa_code = "00000"
    elif var == "wrong":
        xa_code = "99999"
    elif var == "null":
        xa_code = None
    t_base = xa["tinh"]
    if var == "lech":
        t_base = rng.choice([t for t in geo.tinh if t != xa["tinh"]])
    tv = _weighted(rng, [("nnTTT", .66), ("0NN", .12), ("NN", .09), ("empty", .04 * scale), ("invalid", .02 * scale), ("old", .07)])
    code2 = t_base
    if tv == "old" and geo.tinh.get(t_base, {}).get("sn_old"):
        code2 = rng.choice(geo.tinh[t_base]["sn_old"])
    if tv in ("nnTTT", "old"):
        tinh = f"{code2}TTT"
    elif tv == "0NN":
        tinh = f"0{code2}"
    elif tv == "NN":
        tinh = str(int(code2)) if int(code2) < 10 else code2
    elif tv == "empty":
        tinh = rng.choice(["00TTT", "000", "00", None])
    else:
        tinh = "99TTT"
    return tinh, xa_code


def _dob(rng: random.Random, now: datetime) -> tuple[int | None, int]:
    y = rng.randint(1935, 2023)
    m, d = rng.randint(1, 12), rng.randint(1, 28)
    full = y * 10000 + m * 100 + d
    v = _weighted(rng, [("full", .88), ("y0000", .03), ("y", .01), ("ym00", .025), ("ym", .01), ("bad", .02), ("null", .015)])
    if v == "full": return full, y
    if v == "y0000": return y * 10000, y
    if v == "y": return y, y
    if v == "ym00": return y * 10000 + m * 100, y
    if v == "ym": return y * 100 + m, y
    if v == "bad": return rng.choice([y * 10000 + 230, 17500101, y * 10000 + 1340, 20991231]), y
    return None, y


def _phone(rng: random.Random) -> str | None:
    base = rng.choice(["9", "3", "7", "8", "5"]) + "".join(str(rng.randint(0, 9)) for _ in range(8))
    v = _weighted(rng, [("0", .70), ("84", .06), ("840", .03), ("nine", .05), ("space", .05), ("bad", .03), ("null", .08)])
    if v == "0": return "0" + base
    if v == "84": return "84" + base
    if v == "840": return "840" + base
    if v == "nine": return base
    if v == "space": return f"0{base[:3]} {base[3:6]} {base[6:]}"
    if v == "bad": return rng.choice(["12345", "0912", "abc"])
    return None


def cccd_for(tinh2: str, y: int, male: bool, rng: random.Random) -> str:
    c = (0 if male else 1) if y < 2000 else (2 if male else 3)
    return f"{int(tinh2):03d}{c}{y % 100:02d}" + "".join(str(rng.randint(0, 9)) for _ in range(6))


def gen_people(n: int, geo: Geo, dm: dict, rng: random.Random, now: datetime, seq_start: int = 1, ma_prefix: str | None = None,
               ho_prefix: str = "HGD", dup_rate: float = 0.04, error_scale: float = 1.0) -> list[dict]:
    """Sinh ~n công dân (+ bản sao nhiều hộ). ma_prefix=None: MA_CD = <2 số tỉnh><8 số seq>; có prefix: prefix + seq 9 số."""
    qt_ma = [r["MA"] for r in dm["quoc_tich"] if r["MA"] != "VN"]
    dt_ids = [r["ID"] for r in dm["dantoc"] if r["ID"] != 171]
    rows: list[dict] = []
    seq, hid = seq_start, seq_start
    while len(rows) < n:
        size = min(_weighted(rng, [(1, .12), (2, .22), (3, .28), (4, .22), (5, .10), (6, .06)]), n - len(rows))
        xa = rng.choice(geo.xa)
        ma_ho = f"{ho_prefix}{hid:09d}"
        hid += 1
        for k in range(size):
            male = rng.random() < .5
            ten = f"{rng.choice(HO)} {rng.choice(DEM_NAM if male else DEM_NU)} {rng.choice(TEN_NAM if male else TEN_NU)}"
            if rng.random() < .04:
                ten = f"  {ten.replace(' ', '   ', 1)} "
            ma_cd = f"{ma_prefix}{seq:09d}" if ma_prefix else f"{xa['tinh']}{seq:08d}"
            seq += 1
            ngay_sinh, y = _dob(rng, now)
            r = rng.random()
            cccd = cmnd = None
            if r < .72:
                cccd = cccd_for(xa["tinh"], y, male, rng)
                if rng.random() < .1: cmnd = "".join(str(rng.randint(0, 9)) for _ in range(9))
            elif r < .92:
                cmnd = "".join(str(rng.randint(0, 9)) for _ in range(9))
                if rng.random() < .05: cccd = "12345678901"
            qr = rng.random()
            quoc_tich = "VN" if qr < .85 else "233" if qr < .95 else "XX" if qr < .96 else rng.choice(qt_ma) if qr < .99 else None
            dr = rng.random()
            dan_toc = 171 if dr < .85 else 9999 if dr < .86 else None if dr < .87 else rng.choice(dt_ids)
            tinh, xa_raw = addr_raw(xa, geo, rng, error_scale)
            if rng.random() < .85: tinh_lh, xa_lh = tinh, xa_raw
            elif rng.random() < .03: tinh_lh, xa_lh = None, None
            else: tinh_lh, xa_lh = addr_raw(rng.choice(geo.xa), geo, rng, .5 * error_scale)
            tinh_ks, xa_ks = (None, None) if rng.random() < .08 else addr_raw(rng.choice(geo.xa), geo, rng, .6 * error_scale)
            tg = rng.random()
            typeid = "GT" if tg < .004 else "GM" if tg < .006 else "GG" if tg < .01 else "GK" if tg < .012 else \
                     "TM" if tg < .2 else "DC" if tg < .3 else None
            if typeid in ("GT", "GM", "GG", "GK"): typeid = rng.choice(["GT", "GM", "GG", "GK"]) if tg < .012 else typeid
            td = rng.random()
            duyet = 1 if td < .005 else 3 if td < .01 else 4 if td < .015 else 2 if td < .1 else 0 if td < .8 else None
            alive = rng.random() > .03
            created = now - timedelta(days=rng.randint(100, 1000), seconds=rng.randint(0, 86399))
            modify = created + timedelta(seconds=rng.randint(0, int((now - created).total_seconds())))
            rows.append(dict(
                MA_CD=ma_cd, MA_CN=ma_cd, MA_HO_GD=ma_ho, HO_TEN=ten, HO_TEN_UNSIGNED=khong_dau(ten),
                GIOI_TINH="1" if male else "0", NGAY_SINH=ngay_sinh, NOI_SINH=None, QH_CHU_HO="00" if k == 0 else "01",
                SO_CMTND=cmnd, SO_CCCD=cccd, DIA_CHI=f"Số {rng.randint(1, 200)}, {xa['ten']}", DC_LIEN_LAC=None,
                EMAIL_ACC=None if rng.random() < .8 else f"u{seq}@example.vn", PHONE_NUMBER=_phone(rng),
                MA_TINH=tinh, MA_HUYEN=None, MA_XA=xa_raw, QUOC_TICH=quoc_tich, IS_ALIVE="1" if alive else "0",
                DEAD_DATE=None if alive else 20200000 + rng.randint(101, 1228), CREATED_DATE=created, MODIFY_DATE=modify,
                ISCHU_HO="1" if k == 0 else "0", TYPEID=typeid, TRANG_THAI_DUYET=duyet, MA_TINH_KS=tinh_ks, MA_XA_KS=xa_ks,
                MA_TINH_LH=tinh_lh, MA_XA_LH=xa_lh, ID=f"{ma_cd}-{ma_ho}", DM_DAN_TOC_ID=dan_toc, MA_CD_OLD=None))
    # công dân thuộc nhiều hộ: bản sao sang hộ khác, MODIFY_DATE lệch (cũ hoặc mới hơn) để kiểm thử khử trùng
    base = list(rows)
    for src in rng.sample(base, int(len(base) * dup_rate)):
        for _ in range(1 if rng.random() < .8 else 2):
            c = dict(src)
            c["MA_HO_GD"] = f"{ho_prefix}{hid:09d}"
            hid += 1
            c["ID"] = f"{c['MA_CD']}-{c['MA_HO_GD']}"
            delta = timedelta(days=rng.randint(1, 300) * rng.choice([-1, 1]))
            c["MODIFY_DATE"] = min(src["MODIFY_DATE"] + delta, now)
            c["CREATED_DATE"] = min(c["CREATED_DATE"], c["MODIFY_DATE"])
            c["QH_CHU_HO"] = "01"
            c["ISCHU_HO"] = "0"
            xa2 = rng.choice(geo.xa)
            c["MA_TINH"], c["MA_XA"] = addr_raw(xa2, geo, rng, error_scale)
            rows.append(c)
    return rows


def golden_cases(geo: Geo, now: datetime) -> list[dict]:
    """Các ca cố định (MA_CD biết trước) để demo/kiểm thử; kỳ vọng được test trong tests/test_generator.py."""
    xa = geo.xa_by_tinh["01"][0]
    xb = geo.xa_by_tinh["79"][0] if "79" in geo.xa_by_tinh else geo.xa[1]
    d = lambda days: now - timedelta(days=days)

    def row(ma, ho, **kw):
        r = dict(MA_CD=ma, MA_CN=ma, MA_HO_GD=ho, HO_TEN="Nguyễn Văn Test", HO_TEN_UNSIGNED="NGUYEN VAN TEST", GIOI_TINH="1",
                 NGAY_SINH=19950815, NOI_SINH=None, QH_CHU_HO="00", SO_CMTND=None, SO_CCCD="001095012345", DIA_CHI="Số 1",
                 DC_LIEN_LAC=None, EMAIL_ACC=None, PHONE_NUMBER="0912345678", MA_TINH="01TTT", MA_HUYEN=None, MA_XA=xa["ma"],
                 QUOC_TICH="VN", IS_ALIVE="1", DEAD_DATE=None, CREATED_DATE=d(500), MODIFY_DATE=d(100), ISCHU_HO="1", TYPEID=None,
                 TRANG_THAI_DUYET=0, MA_TINH_KS=None, MA_XA_KS=None, MA_TINH_LH=None, MA_XA_LH=None, ID=f"{ma}-{ho}",
                 DM_DAN_TOC_ID=171, MA_CD_OLD=None)
        r.update(kw)
        r["ID"] = f"{ma}-{ho}"
        return r

    return [
        row("0120456789", "GOLD_HO_A", MODIFY_DATE=d(300), SO_CCCD=None, MA_XA="99999"),           # hộ cũ
        row("0120456789", "GOLD_HO_B", MODIFY_DATE=d(10), HO_TEN="  Nguyễn   Văn  Test "),         # hộ mới nhất -> được chọn
        row("0120456789", "GOLD_HO_C", MODIFY_DATE=d(200), TYPEID="GT"),                           # bị loại
        row("0133000001", "GOLD_HO_D"),                                                             # sạch hoàn toàn
        row("0133000002", "GOLD_HO_E", TYPEID="GG"),                                                # bị loại
        row("0133000003", "GOLD_HO_F", TRANG_THAI_DUYET=1),                                         # bị loại
        row("0133000004", "GOLD_HO_G", MA_TINH="79TTT", MA_XA=xa["ma"]),                           # tỉnh lệch xã
        row("0133000005", "GOLD_HO_H", NGAY_SINH=19950230),                                        # ngày sinh sai lịch
        row("0133000006", "GOLD_HO_I", PHONE_NUMBER="0912 345 678", QUOC_TICH="233", DM_DAN_TOC_ID=9999, MA_XA=xb["ma"], MA_TINH="079"),
        row("0133000007", "GOLD_HO_J", MA_XA="4", MA_TINH="1", SO_CCCD=None, SO_CMTND="123456789", PHONE_NUMBER="84912345678"),
    ]
