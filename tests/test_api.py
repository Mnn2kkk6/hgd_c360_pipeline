import hashlib
from datetime import datetime, timedelta
import pytest
from fastapi.testclient import TestClient
from hgd.api.app import create_app, dec_cursor, enc_cursor
from hgd.api.repo import PERSON_COLS, Repo, dob_clause, FIELD_BY_TIER
from hgd.common.vn_text import khong_dau

NOW = datetime.now()


def P(ma, cu=None, dd=None, ten="Nguyễn Văn A", ns="19950815", sd=None, i=1):
    r = {c: None for c in PERSON_COLS}
    r.update(MA_SO_BHXH=ma, MA_SO_BHXH_CU=cu, SO_DINH_DANH=dd, HO_TEN=ten, HO_TEN_KHONG_DAU=khong_dau(ten), NGAY_SINH=ns,
            SYNC_DATE=sd or NOW - timedelta(hours=1), ID=i)
    return r


class FakeRepo(Repo):
    def __init__(self, rows):
        self.rows = rows
        self.audit = []
        self.keys = {hashlib.sha256(b"k-all").hexdigest(): dict(name="t", scopes={"lookup", "batch", "search", "changes"}, active=True),
                     hashlib.sha256(b"k-lookup").hexdigest(): dict(name="l", scopes={"lookup"}, active=True),
                     hashlib.sha256(b"k-off").hexdigest(): dict(name="o", scopes={"lookup"}, active=False)}

    def find(self, field, codes):
        col = FIELD_BY_TIER[field]
        out = {}
        for r in self.rows:
            if r[col] in codes:
                out.setdefault(r[col], []).append(r)
        return out

    def search(self, name_unsigned, dob, prefix, limit):
        out = []
        for r in self.rows:
            n = r["HO_TEN_KHONG_DAU"]
            if not (n.startswith(name_unsigned) if prefix else n == name_unsigned):
                continue
            ns = r["NGAY_SINH"]
            if dob and not ((len(dob) == 8 and ns in (dob, dob[:6], dob[:4])) or (len(dob) == 6 and (ns.startswith(dob) or ns == dob[:4])) or (len(dob) == 4 and ns.startswith(dob))):
                continue
            out.append(r)
        return out[:limit]

    def changes(self, after, lag_seconds, limit):
        until = datetime.now() - timedelta(seconds=lag_seconds)
        rs = sorted([r for r in self.rows if r["SYNC_DATE"] <= until], key=lambda r: (r["SYNC_DATE"], r["ID"]))
        if after:
            rs = [r for r in rs if (r["SYNC_DATE"], r["ID"]) > after]
        return rs[:limit]

    def get_key(self, h):
        return self.keys.get(h)

    def write_audit(self, rows):
        self.audit += rows


@pytest.fixture()
def client():
    rows = [P("0100000001", dd="001095012345", i=1), P("0100000002", cu="0100000009", i=2),
            P("0100000003", cu="DUP0000001", i=3), P("0100000004", cu="DUP0000001", i=4),
            P("0100000005", dd="001090000001", ten="Trần Thị Ánh", ns="1990", i=5),
            P("0100000006", ten="Trần Thị Ánh", ns="19900315", i=6),
            P("0100000007", ten="Trần Thị Ánh", ns="19910101", i=7),
            P("0100000008", i=8, sd=NOW)]                       # mới sync, nằm trong khoảng lag
    repo = FakeRepo(rows)
    with TestClient(create_app(repo, lag_seconds=120)) as c:
        c.repo = repo
        yield c


H = {"X-API-Key": "k-all"}


def test_auth(client):
    assert client.get("/api/v1/ca-nhan/0100000001").status_code == 401
    assert client.get("/api/v1/ca-nhan/0100000001", headers={"X-API-Key": "x"}).status_code == 401
    assert client.get("/api/v1/ca-nhan/0100000001", headers={"X-API-Key": "k-off"}).status_code == 401
    assert client.post("/api/v1/ca-nhan/batch", json={"ma": ["0100000001"]}, headers={"X-API-Key": "k-lookup"}).status_code == 403


def test_lookup_tiers(client):
    r = client.get("/api/v1/ca-nhan/0100000001", headers=H).json()
    assert r["nguon_khop"] == "MA_SO_BHXH" and r["du_lieu"]["ho_ten"] == "Nguyễn Văn A" and "id" not in r["du_lieu"]
    assert client.get("/api/v1/ca-nhan/0100000009", headers=H).json()["du_lieu"]["ma_so_bhxh"] == "0100000002"       # mã cũ
    assert client.get("/api/v1/ca-nhan/001090000001", headers=H).json()["du_lieu"]["ma_so_bhxh"] == "0100000005"     # định danh
    assert client.get("/api/v1/ca-nhan/0999999999", headers=H).status_code == 404
    assert client.get("/api/v1/ca-nhan/0100000009?loai=bhxh", headers=H).status_code == 404                          # ép tầng
    assert client.get("/api/v1/ca-nhan/ab", headers=H).status_code == 422


def test_conflict_returns_candidates(client):
    r = client.get("/api/v1/ca-nhan/DUP0000001", headers=H)
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["so_ung_vien"] == 2 and {u["ma_so_bhxh"] for u in d["ung_vien"]} == {"0100000003", "0100000004"}


def test_batch(client):
    r = client.post("/api/v1/ca-nhan/batch", headers=H, json={"ma": ["0100000001", "0100000009", "DUP0000001", "0999999999", "0100000001"]}).json()
    st = {x["ma"]: x["trang_thai"] for x in r["ket_qua"]}
    assert st == {"0100000001": "FOUND", "0100000009": "FOUND", "DUP0000001": "CONFLICT", "0999999999": "NOT_FOUND"}
    assert r["tong"] == 4 and r["tim_thay"] == 2
    assert client.post("/api/v1/ca-nhan/batch", headers=H, json={"ma": ["0100000001"] * 1001}).status_code == 422
    assert client.post("/api/v1/ca-nhan/batch", headers=H, json={"ma": []}).status_code == 422


def test_search_dob_variants(client):
    def n(q):
        return client.get("/api/v1/ca-nhan?" + q, headers=H).json()["so_ket_qua"]
    assert n("ho_ten=Tran Thi Anh") == 3
    assert n("ho_ten=TRẦN THỊ ÁNH&ngay_sinh=19900315") == 2      # đầy đủ + bản ghi chỉ có năm 1990
    assert n("ho_ten=Tran Thi Anh&ngay_sinh=199003") == 2
    assert n("ho_ten=Tran Thi Anh&ngay_sinh=1990") == 2
    assert n("ho_ten=Tran&kieu=bat_dau") == 3
    assert client.get("/api/v1/ca-nhan?ho_ten=Tran Thi Anh&ngay_sinh=19900", headers=H).status_code == 422


def test_changes_cursor_and_lag(client):
    a = client.get("/api/v1/thay-doi?limit=3", headers=H).json()
    assert a["so_ban_ghi"] == 3 and a["co_tiep"]
    b = client.get(f"/api/v1/thay-doi?limit=100&cursor={a['next_cursor']}", headers=H).json()
    got = [x["ma_so_bhxh"] for x in a["du_lieu"] + b["du_lieu"]]
    assert len(got) == len(set(got)) == 7 and "0100000008" not in got          # bản ghi mới sync bị giữ lại theo lag
    assert not b["co_tiep"]
    assert client.get("/api/v1/thay-doi?cursor=@@@", headers=H).status_code == 400


def test_cursor_roundtrip():
    d = datetime(2026, 1, 2, 3, 4, 5)
    assert dec_cursor(enc_cursor(d, 99)) == (d, 99)


def test_audit_written(client):
    client.get("/api/v1/ca-nhan/0100000001", headers=H)
    client.app.state.audit.stop()
    assert any(a["endpoint"] == "ca-nhan" and a["http_status"] == 200 for a in client.repo.audit)


def test_dob_clause():
    assert dob_clause(None) == ("", {})
    assert dob_clause("19900315")[1] == {"d8": "19900315", "d6": "199003", "d4": "1990"}
