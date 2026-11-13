"""Truy cập dữ liệu API. `Repo` là giao diện; `OracleRepo` chạy thật trên C360; test dùng FakeRepo trong tests/."""
from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

PERSON_COLS = ["MA_SO_BHXH", "MA_SO_BHXH_CU", "SO_DINH_DANH", "HO_TEN", "GIOI_TINH", "NGAY_SINH", "IS_ALIVE", "SO_DIEN_THOAI", "EMAIL",
               "QUOC_TICH_MA", "QUOC_TICH_TEN", "DAN_TOC_MA", "DAN_TOC_TEN", "MA_HO_GD", "QH_CHU_HO",
               "MA_TINH", "TEN_TINH", "MA_XA", "TEN_XA", "NGUON_MAP_XA", "DIA_CHI",
               "MA_TINH_LH", "TEN_TINH_LH", "MA_XA_LH", "TEN_XA_LH", "MA_TINH_KS", "TEN_TINH_KS", "MA_XA_KS", "TEN_XA_KS",
               "DQ_FLAGS", "SYNC_DATE", "ID"]
FIELD_BY_TIER = {"bhxh": "MA_SO_BHXH", "bhxh_cu": "MA_SO_BHXH_CU", "dinh_danh": "SO_DINH_DANH"}


class Repo(ABC):
    @abstractmethod
    def find(self, field: str, codes: list[str]) -> dict[str, list[dict]]: ...
    @abstractmethod
    def search(self, name_unsigned: str, dob: str | None, prefix: bool, limit: int) -> list[dict]: ...
    @abstractmethod
    def changes(self, after: tuple[datetime, int] | None, lag_seconds: int, limit: int) -> list[dict]: ...
    @abstractmethod
    def get_key(self, key_hash: str) -> dict | None: ...
    @abstractmethod
    def write_audit(self, rows: list[dict]) -> None: ...


def dob_clause(dob: str | None) -> tuple[str, dict]:
    """Ngày sinh nhập YYYY / YYYYMM / YYYYMMDD. Bản ghi chỉ có năm/tháng rút gọn vẫn khớp với ngày đầy đủ."""
    if not dob:
        return "", {}
    if len(dob) == 8:
        return " AND NGAY_SINH IN (:d8, :d6, :d4)", {"d8": dob, "d6": dob[:6], "d4": dob[:4]}
    if len(dob) == 6:
        return " AND (NGAY_SINH LIKE :d6 || '%' OR NGAY_SINH = :d4)", {"d6": dob, "d4": dob[:4]}
    return " AND NGAY_SINH LIKE :d4 || '%'", {"d4": dob}


class OracleRepo(Repo):
    def __init__(self, pool):
        self.pool = pool

    @classmethod
    def create(cls):
        import oracledb
        from hgd import config
        u, p = config.credentials("c360")
        return cls(oracledb.create_pool(user=u, password=p, dsn=config.dsn(), min=2, max=10, increment=1))

    def _q(self, sql: str, binds: dict | list | None = None) -> list[dict]:
        with self.pool.acquire() as conn, conn.cursor() as cur:
            cur.execute(sql, binds or {})
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def find(self, field, codes):
        col = FIELD_BY_TIER[field]
        out: dict[str, list[dict]] = {}
        for i in range(0, len(codes), 900):                      # giới hạn 1000 phần tử IN của Oracle
            part = codes[i:i + 900]
            binds = {f"c{j}": c for j, c in enumerate(part)}
            ph = ",".join(":" + k for k in binds)
            for r in self._q(f"SELECT {','.join(PERSON_COLS)} FROM C360_THONG_TIN_CA_NHAN WHERE {col} IN ({ph})", binds):
                out.setdefault(r[col], []).append(r)
        return out

    def search(self, name_unsigned, dob, prefix, limit):
        cl, b = dob_clause(dob)
        cond = "HO_TEN_KHONG_DAU LIKE :n || '%'" if prefix else "HO_TEN_KHONG_DAU = :n"
        return self._q(f"SELECT {','.join(PERSON_COLS)} FROM C360_THONG_TIN_CA_NHAN WHERE {cond}{cl} ORDER BY HO_TEN_KHONG_DAU, NGAY_SINH, MA_SO_BHXH "
                       f"FETCH FIRST :lim ROWS ONLY", {"n": name_unsigned, "lim": limit, **b})

    def changes(self, after, lag_seconds, limit):
        # Con trỏ (SYNC_DATE, ID); Oracle không có so sánh tuple nên khai triển tay. Chỉ lấy bản ghi đã "yên" >= lag giây
        # để tránh bỏ sót bản ghi commit muộn khi hệ thống ngoài đang phân trang.
        if after is None:
            return self._q(f"SELECT {','.join(PERSON_COLS)} FROM C360_THONG_TIN_CA_NHAN WHERE SYNC_DATE <= SYSDATE - :lag / 86400 "
                           f"ORDER BY SYNC_DATE, ID FETCH FIRST :lim ROWS ONLY", {"lag": lag_seconds, "lim": limit})
        return self._q(f"SELECT {','.join(PERSON_COLS)} FROM C360_THONG_TIN_CA_NHAN WHERE SYNC_DATE <= SYSDATE - :lag / 86400 "
                       f"AND (SYNC_DATE > :d OR (SYNC_DATE = :d AND ID > :i)) ORDER BY SYNC_DATE, ID FETCH FIRST :lim ROWS ONLY",
                       {"lag": lag_seconds, "d": after[0], "i": after[1], "lim": limit})

    def get_key(self, key_hash):
        r = self._q("SELECT TEN_HE_THONG, SCOPES, ACTIVE FROM C360_API_KEY WHERE KEY_HASH = :h", {"h": key_hash})
        return {"name": r[0]["TEN_HE_THONG"], "scopes": set(r[0]["SCOPES"].split(",")), "active": bool(r[0]["ACTIVE"])} if r else None

    def write_audit(self, rows):
        if not rows:
            return
        with self.pool.acquire() as conn, conn.cursor() as cur:
            cur.executemany("INSERT INTO C360_API_AUDIT_LOG (KEY_NAME, ENDPOINT, PARAMS, HTTP_STATUS, RESULT_COUNT, DURATION_MS, CLIENT_IP) "
                            "VALUES (:key_name, :endpoint, :params, :http_status, :result_count, :duration_ms, :client_ip)", rows)
            conn.commit()
