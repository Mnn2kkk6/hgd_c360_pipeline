import pytest
from hgd.sync import loadplan as lp


def test_split_points_balanced_and_strict():
    keys = [f"{i:010d}" for i in range(1000)]
    pts = lp.split_points(keys, 4)
    assert pts == ["0000000250", "0000000500", "0000000750"]
    assert pts == sorted(set(pts))


def test_split_points_edge_cases():
    assert lp.split_points([], 4) == []
    assert lp.split_points(["a", "b"], 4) == []                 # mẫu < số khối
    assert lp.split_points(["a"] * 100, 4) == []                # toàn khóa trùng -> không chia được
    assert lp.split_points([str(i) for i in range(100)], 1) == []
    pts = lp.split_points(["a"] * 50 + ["b"] * 50, 4)
    assert pts == ["b"]                                          # trùng bị gộp, vẫn tăng chặt


def test_ranges_cover_everything_without_gap_or_overlap():
    pts = ["0300", "0600"]
    rg = lp.ranges_from_points(pts)
    assert rg == [(None, "0300"), ("0300", "0600"), ("0600", None)]
    # mỗi khóa thuộc đúng 1 khối (lo bao gồm, hi loại trừ)
    for k in ["0000", "0299", "0300", "0599", "0600", "9999"]:
        hits = [r for r in rg if (r[0] is None or k >= r[0]) and (r[1] is None or k < r[1])]
        assert len(hits) == 1, k
    assert lp.ranges_from_points([]) == [(None, None)]


def test_where_and_predicate():
    assert lp.where_binds("MA_CD", None, None) == ("", {})
    assert lp.where_binds(None, "a", "b") == ("", {})
    w, b = lp.where_binds("MA_CD", "0300", "0600")
    assert w == " WHERE MA_CD >= :lo AND MA_CD < :hi" and b == {"lo": "0300", "hi": "0600"}
    assert lp.where_binds("MA_CD", None, "0600")[0] == " WHERE MA_CD < :hi"
    assert lp.predicate_literal("MA_CD", "03", "06") == "MA_CD >= '03' AND MA_CD < '06'"
    assert lp.predicate_literal("MA_CD", None, None) == "1=1"
    assert "''" in lp.predicate_literal("MA_CD", "a'b", None)    # chống chèn SQL


def test_cleanup_rule():
    assert lp.needs_cleanup("RUNNING") and lp.needs_cleanup("FAILED")
    assert not lp.needs_cleanup("PENDING") and not lp.needs_cleanup("DONE")


def test_count_verdict_does_not_abort_on_live_source():
    assert lp.count_verdict(100, 100)[0] == "OK"
    st, msg = lp.count_verdict(104787, 104790)
    assert st == "WARN" and "CDC" in msg                         # trước đây: SystemExit làm gãy chuỗi lệnh
    assert lp.count_verdict(100, 90)[0] == "WARN"
    assert lp.count_verdict(100, 101, strict=True)[0] == "FAIL"
    assert lp.count_verdict(0, 0)[0] == "OK"


def test_exclude_cols_contains_sync_ts():
    assert "SYNC_TS" in lp.EXCLUDE_COLS
