from datetime import date, datetime
import pytest
from hgd.c360 import reference as ref
from hgd.common.vn_text import khong_dau, norm_name

T = date(2026, 11, 11)


@pytest.mark.parametrize("p,exp", [
    (19950815, "19950815"), (19950000, "1995"), (1995, "1995"), (19950800, "199508"), (199508, "199508"),
    (19950230, None), (17500101, None), (20991231, None), (19951301, None), (None, None), (12345, None),
    (19960229, "19960229"), (19950229, None), (20261201, None), (20261200, None), (20261100, "202611"), (20261011, "20261011"), (19951000, "199510"),
])
def test_ngay_sinh(p, exp):
    assert ref.norm_ngay_sinh(p, T) == exp


@pytest.mark.parametrize("p,exp", [
    ("0912345678", "0912345678"), ("84912345678", "0912345678"), ("840912345678", "0912345678"), ("912345678", "0912345678"),
    ("0912 345 678", "0912345678"), ("+84 912-345-678", "0912345678"), ("12345", None), (None, None), ("abc", None), ("01234567890", "01234567890"),
])
def test_phone(p, exp):
    assert ref.norm_phone(p) == exp


@pytest.mark.parametrize("p,exp", [("01TTT", "001"), ("33TTT", "033"), ("079", "079"), ("1", "001"), ("00TTT", None), ("000", None),
                                   (None, None), ("99TTT", "099"), ("ABC", None), ("1234", None), (" 34ttt ", "034")])
def test_tinh_key(p, exp):
    assert ref.norm_tinh_key(p) == exp


@pytest.mark.parametrize("p,exp", [("00004", "00004"), ("4", "00004"), ("00000", None), (None, None), ("123456", None), ("12a", None)])
def test_xa_key(p, exp):
    assert ref.norm_xa_key(p) == exp


def test_dinh_danh():
    assert ref.pick_dinh_danh("001095012345", "123456789") == "001095012345"
    assert ref.pick_dinh_danh("12345678901", "001095012345") == "001095012345"
    assert ref.pick_dinh_danh(None, "123456789") is None


def test_khong_dau():
    assert khong_dau("  Nguyễn   Đặng  Thị Ánh ") == "NGUYEN DANG THI ANH"
    assert khong_dau("Phạm Hữu Quý") == "PHAM HUU QUY"
    assert norm_name("  a   b ") == "a b"


def test_khong_dau_parity_with_oracle_translate_map():
    """Bản đồ TRANSLATE trong sql/30_pkg_util.sql phải cho cùng kết quả với khong_dau() Python."""
    import re
    sql = open("sql/30_pkg_util.sql", encoding="utf-8").read()
    frm = re.search(r"c_vn_from CONSTANT VARCHAR2\(2000\) := '(.*?)';", sql).group(1)
    to = re.search(r"c_vn_to   CONSTANT VARCHAR2\(2000\) := '(.*?)';", sql).group(1)
    assert len(frm) == len(to) and len(frm) > 100
    for a, b in zip(frm, to):
        assert khong_dau(a) == b.upper()


def _dbhc(ma, cha, ten, sn=None, het=datetime(2099, 12, 31)):
    return dict(MA_DBHC=ma, MA_CHA=cha, TEN=ten, MA_DBHC_SN=sn, TINH_TRANG="1", NGAY_HIEU_LUC=datetime(2025, 7, 1), NGAY_HET_HIEU_LUC=het)


def test_build_maps_priority_and_mo_ho():
    rows = [
        _dbhc("33", "0003", "Hưng Yên", "33;34"), _dbhc("01", "0003", "Hà Nội", "01"),
        _dbhc("11111", "33", "Xã A", "11111;22222;33333"), _dbhc("11112", "33", "Xã B", "11112;33333"),
        _dbhc("11113", "01", "Xã C", "11113;44444;55555"),          # 55555 cũng là xã hiện hành D?
        _dbhc("55555", "01", "Xã D", "55555"),
        _dbhc("22222", "33", "Xã cũ hết hiệu lực", None, het=datetime(2025, 6, 30)),
    ]
    mt, mx = ref.build_maps(rows)
    assert mt["034"]["nguon"] == "SAP_NHAP" and mt["034"]["ma_tinh"] == "033"
    assert mx["11111"]["nguon"] == "HIEN_HANH"
    assert mx["22222"]["nguon"] == "SAP_NHAP" and mx["22222"]["ma_xa"] == "11111"
    assert mx["33333"]["nguon"] == "MO_HO" and mx["33333"]["ma_xa"] is None and mx["33333"]["ma_tinh"] == "033"
    assert mx["55555"]["nguon"] == "HIEN_HANH"                      # hiện hành thắng SN của xã khác
    assert mx["44444"]["ma_xa"] == "11113"


def test_address_flags():
    mt = {"033": dict(ma_tinh="033", ten_tinh="HY", nguon="HIEN_HANH"), "001": dict(ma_tinh="001", ten_tinh="HN", nguon="HIEN_HANH")}
    mx = {"11111": dict(ma_xa="11111", ma_tinh="033", ten_xa="A", ten_tinh="HY", nguon="HIEN_HANH", so_xa_moi=1),
          "33333": dict(ma_xa=None, ma_tinh=None, ten_xa=None, ten_tinh=None, nguon="MO_HO", so_xa_moi=2)}
    r, f = ref.resolve_address("01TTT", "11111", mt, mx, True)
    assert r["ma_tinh"] == "033" and f == ["TINH_LECH_XA"]
    r, f = ref.resolve_address("33TTT", "33333", mt, mx, True)
    assert r["ma_tinh"] == "033" and r["ma_xa"] is None and f == ["XA_MO_HO"]
    r, f = ref.resolve_address(None, None, mt, mx, True)
    assert f == ["TINH_KHONG_MAP", "XA_KHONG_MAP"]
    r, f = ref.resolve_address(None, None, mt, mx, False)
    assert f == []                                                   # LH/KS rỗng không bị gắn cờ
    r, f = ref.resolve_address("33TTT", "99999", mt, mx, False)
    assert r["ma_tinh"] == "033" and f == ["XA_KHONG_MAP"]


def test_pick_latest_filters_then_dedups():
    t = datetime(2026, 1, 1)
    rows = [dict(MA_CD="1", MA_HO_GD="a", MODIFY_DATE=t, TYPEID="GT"),               # mới nhất nhưng bị loại
            dict(MA_CD="1", MA_HO_GD="b", MODIFY_DATE=datetime(2025, 1, 1), TRANG_THAI_DUYET=2),
            dict(MA_CD="1", MA_HO_GD="c", MODIFY_DATE=datetime(2024, 1, 1))]
    assert ref.pick_latest(rows)["MA_HO_GD"] == "b"
    assert ref.pick_latest([rows[0]]) is None
