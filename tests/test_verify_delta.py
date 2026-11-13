from hgd.tools.verify_delta import check


def test_check_rules():
    row = dict(MA_HO_GD="H1", SO_DINH_DANH="123456789012", MA_XA="00004", NGAY_SINH="19900505", MA_SO_BHXH_CU="OLD", DQ_FLAGS="SDT_LOI")
    assert check(row, dict(exists=True, ho_gd="H1", no_flag=["XA_KHONG_MAP"])) is None
    assert "MA_HO_GD" in check(row, dict(exists=True, ho_gd="H2"))
    assert "SDT_LOI" in check(row, dict(exists=True, no_flag=["SDT_LOI"]))
    assert check(None, dict(exists=False)) is None
    assert "KHÔNG" in check(row, dict(exists=False))
    assert "PHẢI" in check(None, dict(exists=True))
