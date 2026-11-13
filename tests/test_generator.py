import random
from collections import Counter
from datetime import datetime
from hgd.generator import geo as G
from hgd.generator.people import SOURCE_COLS, gen_people, golden_cases
from hgd.c360 import reference as ref

NOW = datetime(2026, 11, 11, 12)


def _setup(n=20000, seed=7):
    rng = random.Random(seed)
    dm = G.dm_rows(G.load_dm())
    codes = G.inject_mo_ho(dm["dbhc"], 6, rng, NOW)
    geo = G.build_geo(dm["dbhc"], NOW, codes)
    rows = gen_people(n, geo, dm, rng, NOW) + golden_cases(geo, NOW)
    return dm, geo, rows


def test_geo_matches_file():
    dm = G.dm_rows(G.load_dm())
    geo = G.build_geo(dm["dbhc"], NOW)
    assert len(geo.tinh) == 36 and len(geo.xa) == 3322


def test_mo_ho_injected_and_detected():
    dm, geo, rows = _setup(100)
    mt, mx = ref.build_maps(dm["dbhc"], NOW)
    mo = [k for k, v in mx.items() if v["nguon"] == "MO_HO"]
    assert sorted(mo) == sorted(geo.mo_ho_codes)
    assert any(mx[k]["ma_tinh"] for k in mo) and any(mx[k]["ma_tinh"] is None for k in mo)   # cả 2 dạng: cùng tỉnh / khác tỉnh
    nguon = Counter(v["nguon"] for v in mx.values())
    assert nguon["HIEN_HANH"] == 3322 and nguon["SAP_NHAP"] > 6000


def test_columns_exist_and_keys_unique():
    _, _, rows = _setup(3000)
    assert all(set(r) == set(SOURCE_COLS) for r in rows)
    keys = [(r["MA_CD"], r["MA_HO_GD"]) for r in rows]
    assert len(keys) == len(set(keys))
    assert all(len(r["MA_CD"]) == 10 for r in rows)


def test_dirty_data_coverage_via_reference():
    dm, geo, rows = _setup(20000)
    c360 = ref.build_c360(rows, dm["dbhc"], dm["quoc_tich"], dm["dantoc"], NOW.date())
    flags = Counter(f for r in c360.values() for f in (r["DQ_FLAGS"] or "").split(";") if f)
    must = ["TINH_KHONG_MAP", "XA_KHONG_MAP", "XA_MO_HO", "TINH_LECH_XA", "NGAY_SINH_LOI", "SDT_LOI", "KHONG_SO_DINH_DANH",
            "QUOC_TICH_KHONG_MAP", "DAN_TOC_KHONG_MAP", "XA_KHONG_MAP_LH", "XA_KHONG_MAP_KS"]
    for f in must:
        assert flags[f] > 0, f
    nguon = Counter(r["NGUON_MAP_XA"] for r in c360.values())
    assert nguon["HIEN_HANH"] > 0 and nguon["SAP_NHAP"] > 0 and nguon["MO_HO"] > 0
    clean = sum(1 for r in c360.values() if r["DQ_FLAGS"] is None)
    assert 0.2 * len(c360) < clean < 0.9 * len(c360)


def test_filtered_rows_ratio_and_dedup():
    dm, geo, rows = _setup(20000)
    bad = sum(1 for r in rows if not ref.is_valid_row(r)) / len(rows)
    assert 0.015 < bad < 0.045
    multi = Counter(r["MA_CD"] for r in rows)
    assert sum(1 for v in multi.values() if v >= 2) > 400


def test_golden_expectations():
    dm, geo, rows = _setup(200)
    c = ref.build_c360(rows, dm["dbhc"], dm["quoc_tich"], dm["dantoc"], NOW.date())
    g = c["0120456789"]
    assert g["MA_HO_GD"] == "GOLD_HO_B" and g["HO_TEN"] == "Nguyễn Văn Test" and g["SO_DINH_DANH"] == "001095012345"
    assert c["0133000001"]["DQ_FLAGS"] is None and c["0133000001"]["NGAY_SINH"] == "19950815"
    assert "0133000002" not in c and "0133000003" not in c
    assert "TINH_LECH_XA" in c["0133000004"]["DQ_FLAGS"]
    assert c["0133000005"]["NGAY_SINH"] is None and "NGAY_SINH_LOI" in c["0133000005"]["DQ_FLAGS"]
    i = c["0133000006"]
    assert i["SO_DIEN_THOAI"] == "0912345678" and i["QUOC_TICH_MA"] == "VN" and "DAN_TOC_KHONG_MAP" in i["DQ_FLAGS"]
    j = c["0133000007"]
    assert j["MA_XA"] == "00004" and j["SO_DIEN_THOAI"] == "0912345678" and "KHONG_SO_DINH_DANH" in j["DQ_FLAGS"]
