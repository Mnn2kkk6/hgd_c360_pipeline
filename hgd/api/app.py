"""REST API tra cứu C360 (chỉ đọc).

Các endpoint theo đặc tả:
  GET  /api/v1/ca-nhan/{ma}                 tra cứu một công dân theo thứ tự ưu tiên
  POST /api/v1/ca-nhan/tra-cuu-lo           tra cứu tối đa 1.000 mã
  GET  /api/v1/ca-nhan/tim-kiem             tìm theo họ tên + ngày sinh
  GET  /api/v1/ca-nhan/thay-doi             phân trang thay đổi theo (SYNC_DATE, ID)

Các đường dẫn /batch, /ca-nhan?ho_ten=... và /api/v1/thay-doi vẫn được giữ
làm alias tương thích ngược. Mọi API nghiệp vụ dùng X-API-Key.
"""
from __future__ import annotations

import base64
import hashlib
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field

from hgd.common.vn_text import khong_dau
from .audit import AuditLogger
from .repo import Repo

CODE_RE = re.compile(r"^[0-9A-Za-z]{5,30}$")
TIERS = ["bhxh", "bhxh_cu", "dinh_danh"]
TIER_LABEL = {"bhxh": "MA_SO_BHXH", "bhxh_cu": "MA_SO_BHXH_CU", "dinh_danh": "SO_DINH_DANH"}


def _ser(v):
    return v.isoformat() if isinstance(v, datetime) else v


def person(r: dict) -> dict:
    return {k.lower(): _ser(v) for k, v in r.items() if k != "ID"}


def candidate(r: dict) -> dict:
    return {"ma_so_bhxh": r["MA_SO_BHXH"], "ho_ten": r["HO_TEN"], "ngay_sinh": r["NGAY_SINH"]}


def enc_cursor(d: datetime, i: int) -> str:
    """Mã hóa con trỏ ổn định theo cặp (SYNC_DATE, ID)."""
    return base64.urlsafe_b64encode(f"{d.isoformat()}|{i}".encode()).decode()


def dec_cursor(c: str) -> tuple[datetime, int]:
    try:
        d, i = base64.urlsafe_b64decode(c.encode()).decode().split("|")
        return datetime.fromisoformat(d), int(i)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(400, "cursor không hợp lệ") from e


def resolve(repo: Repo, codes: list[str], loai: str) -> dict[str, dict]:
    """Dò MA_SO_BHXH -> MA_SO_BHXH_CU -> SO_DINH_DANH; giữ trạng thái CONFLICT."""
    tiers = TIERS if loai == "auto" else [loai]
    res: dict[str, dict] = {}
    left = list(dict.fromkeys(codes))
    for tier in tiers:
        if not left:
            break
        hit = repo.find(tier, left)
        nxt = []
        for code in left:
            rows = hit.get(code, [])
            if len(rows) == 1:
                res[code] = {
                    "trang_thai": "FOUND",
                    "nguon_khop": TIER_LABEL[tier],
                    "du_lieu": person(rows[0]),
                }
            elif len(rows) > 1:
                res[code] = {
                    "trang_thai": "CONFLICT",
                    "nguon_khop": TIER_LABEL[tier],
                    "ung_vien": [candidate(row) for row in rows[:20]],
                    "so_ung_vien": len(rows),
                }
            else:
                nxt.append(code)
        left = nxt
    for code in left:
        res[code] = {"trang_thai": "NOT_FOUND"}
    return res


class BatchIn(BaseModel):
    ma: list[str] = Field(min_length=1, max_length=1000)


def create_app(repo: Repo | None = None, lag_seconds: int | None = None) -> FastAPI:
    lag = int(os.environ.get("API_SYNC_LAG_SECONDS", "120")) if lag_seconds is None else lag_seconds
    if lag < 0:
        raise ValueError("API_SYNC_LAG_SECONDS phải >= 0")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if repo is None:
            from .repo import OracleRepo
            app.state.repo = OracleRepo.create()
        else:
            app.state.repo = repo
        app.state.audit = AuditLogger(app.state.repo, float(os.environ.get("API_AUDIT_FLUSH_SECONDS", "2")))
        app.state.audit.start()
        try:
            yield
        finally:
            app.state.audit.stop()

    app = FastAPI(title="HGD C360 API", version="1.1", lifespan=lifespan)

    def auth(scope: str):
        def dep(request: Request, x_api_key: str | None = Header(default=None)):
            if not x_api_key:
                raise HTTPException(401, "Thiếu header X-API-Key")
            key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
            key = request.app.state.repo.get_key(key_hash)
            if not key or not key["active"]:
                raise HTTPException(401, "API key không hợp lệ")
            if scope not in key["scopes"]:
                raise HTTPException(403, f"Key không có quyền '{scope}'")
            request.state.key_name = key["name"]
            return key
        return dep

    def audit(request: Request, endpoint: str, params: str, status: int, count: int, started: float):
        request.app.state.audit.log(
            key_name=getattr(request.state, "key_name", None),
            endpoint=endpoint,
            params=params[:1000],
            http_status=status,
            result_count=count,
            duration_ms=int((time.time() - started) * 1000),
            client_ip=request.client.host if request.client else None,
        )

    def check_code(code: str):
        if not CODE_RE.fullmatch(code):
            raise HTTPException(422, "Mã phải gồm 5-30 ký tự chữ/số")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    # Các route tĩnh phải đăng ký trước /ca-nhan/{ma}, nếu không chuỗi
    # 'tim-kiem' hoặc 'thay-doi' sẽ bị hiểu nhầm là mã công dân.
    @app.post("/api/v1/ca-nhan/tra-cuu-lo", operation_id="lookup_person_batch")
    @app.post("/api/v1/ca-nhan/batch", operation_id="lookup_person_batch_legacy")
    def batch(
        body: BatchIn,
        request: Request,
        loai: Literal["auto", "bhxh", "bhxh_cu", "dinh_danh"] = "auto",
        _=Depends(auth("batch")),
    ):
        started = time.time()
        for code in body.ma:
            check_code(code)
        result = resolve(request.app.state.repo, body.ma, loai)
        rows = [dict(ma=code, **result[code]) for code in dict.fromkeys(body.ma)]
        found = sum(row["trang_thai"] == "FOUND" for row in rows)
        audit(request, "tra-cuu-lo", f"n={len(body.ma)};loai={loai}", 200, found, started)
        return {"tong": len(rows), "tim_thay": found, "ket_qua": rows}

    @app.get("/api/v1/ca-nhan/tim-kiem", operation_id="search_persons")
    @app.get("/api/v1/ca-nhan", operation_id="search_persons_legacy")
    def search(
        request: Request,
        ho_ten: str = Query(min_length=2, max_length=100),
        ngay_sinh: str | None = Query(default=None, pattern=r"^(\d{4}|\d{6}|\d{8})$"),
        kieu: Literal["chinh_xac", "bat_dau"] = "chinh_xac",
        limit: int = Query(default=20, ge=1, le=100),
        _=Depends(auth("search")),
    ):
        started = time.time()
        rows = request.app.state.repo.search(khong_dau(ho_ten) or "", ngay_sinh, kieu == "bat_dau", limit)
        audit(request, "tim-kiem", f"ngay_sinh={ngay_sinh};kieu={kieu};limit={limit}", 200, len(rows), started)
        return {"so_ket_qua": len(rows), "du_lieu": [person(row) for row in rows]}

    @app.get("/api/v1/ca-nhan/thay-doi", operation_id="list_person_changes")
    @app.get("/api/v1/thay-doi", operation_id="list_person_changes_legacy")
    def changes(
        request: Request,
        cursor: str | None = None,
        limit: int = Query(default=1000, ge=1, le=5000),
        _=Depends(auth("changes")),
    ):
        started = time.time()
        after = dec_cursor(cursor) if cursor else None
        rows = request.app.state.repo.changes(after, lag, limit)
        next_cursor = enc_cursor(rows[-1]["SYNC_DATE"], rows[-1]["ID"]) if rows else cursor
        audit(request, "thay-doi", f"cursor={'Y' if cursor else 'N'};limit={limit}", 200, len(rows), started)
        return {
            "so_ban_ghi": len(rows),
            "next_cursor": next_cursor,
            "co_tiep": len(rows) == limit,
            "du_lieu": [person(row) for row in rows],
        }

    @app.get("/api/v1/ca-nhan/{ma}", operation_id="lookup_person")
    def get_one(
        ma: str,
        request: Request,
        loai: Literal["auto", "bhxh", "bhxh_cu", "dinh_danh"] = "auto",
        _=Depends(auth("lookup")),
    ):
        started = time.time()
        check_code(ma)
        result = resolve(request.app.state.repo, [ma], loai)[ma]
        status = {"FOUND": 200, "CONFLICT": 409, "NOT_FOUND": 404}[result["trang_thai"]]
        audit(request, "ca-nhan", f"ma={ma};loai={loai}", status, 1 if status == 200 else 0, started)
        if status == 200:
            return result
        raise HTTPException(status, detail=result if status == 409 else "Không tìm thấy")

    return app


app = create_app() if os.environ.get("HGD_API_AUTOSTART", "1") == "1" else None
