"""REST API tra cứu C360 (chỉ đọc).  Chạy:  uvicorn hgd.api.app:app --port 8000

  GET  /api/v1/ca-nhan/{ma}?loai=auto|bhxh|bhxh_cu|dinh_danh   tra 1 công dân
  POST /api/v1/ca-nhan/batch                                   tra tối đa 1000 mã
  GET  /api/v1/ca-nhan?ho_ten=&ngay_sinh=&kieu=                tìm theo họ tên (không dấu) + ngày sinh
  GET  /api/v1/thay-doi?cursor=&limit=                         đồng bộ thay đổi theo con trỏ (SYNC_DATE, ID)
Xác thực: header X-API-Key (lưu SHA-256 trong C360_API_KEY, scope: lookup,batch,search,changes).
"""
from __future__ import annotations
import base64
import hashlib
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
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


# ---------------------------------------------------------------- con trỏ
def enc_cursor(d: datetime, i: int) -> str:
    return base64.urlsafe_b64encode(f"{d.isoformat()}|{i}".encode()).decode()


def dec_cursor(c: str) -> tuple[datetime, int]:
    try:
        d, i = base64.urlsafe_b64decode(c.encode()).decode().split("|")
        return datetime.fromisoformat(d), int(i)
    except Exception as e:                                         # noqa: BLE001
        raise HTTPException(400, "cursor không hợp lệ") from e


# ---------------------------------------------------------------- tra cứu theo tầng
def resolve(repo: Repo, codes: list[str], loai: str) -> dict[str, dict]:
    """Tầng ưu tiên: MA_SO_BHXH -> MA_SO_BHXH_CU -> SO_DINH_DANH. Trả {mã: kết quả}.
    1 khớp -> FOUND; >=2 khớp (chỉ xảy ra ở tầng cũ/định danh) -> CONFLICT kèm ứng viên; không khớp -> NOT_FOUND."""
    tiers = TIERS if loai == "auto" else [loai]
    res: dict[str, dict] = {}
    left = list(dict.fromkeys(codes))
    for t in tiers:
        if not left:
            break
        hit = repo.find(t, left)
        nxt = []
        for c in left:
            rows = hit.get(c, [])
            if len(rows) == 1:
                res[c] = dict(trang_thai="FOUND", nguon_khop=TIER_LABEL[t], du_lieu=person(rows[0]))
            elif len(rows) > 1:
                res[c] = dict(trang_thai="CONFLICT", nguon_khop=TIER_LABEL[t], ung_vien=[candidate(r) for r in rows[:20]], so_ung_vien=len(rows))
            else:
                nxt.append(c)
        left = nxt
    for c in left:
        res[c] = dict(trang_thai="NOT_FOUND")
    return res


# ---------------------------------------------------------------- app
class BatchIn(BaseModel):
    ma: list[str] = Field(min_length=1, max_length=1000)


def create_app(repo: Repo | None = None, lag_seconds: int | None = None) -> FastAPI:
    lag = int(os.environ.get("API_SYNC_LAG_SECONDS", "120")) if lag_seconds is None else lag_seconds

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if repo is None:
            from .repo import OracleRepo
            app.state.repo = OracleRepo.create()
        else:
            app.state.repo = repo
        app.state.audit = AuditLogger(app.state.repo, float(os.environ.get("API_AUDIT_FLUSH_SECONDS", "2")))
        app.state.audit.start()
        yield
        app.state.audit.stop()

    app = FastAPI(title="HGD C360 API", version="1.0", lifespan=lifespan)

    def auth(scope: str):
        def dep(request: Request, x_api_key: str | None = Header(default=None)):
            if not x_api_key:
                raise HTTPException(401, "Thiếu header X-API-Key")
            k = request.app.state.repo.get_key(hashlib.sha256(x_api_key.encode()).hexdigest())
            if not k or not k["active"]:
                raise HTTPException(401, "API key không hợp lệ")
            if scope not in k["scopes"]:
                raise HTTPException(403, f"Key không có quyền '{scope}'")
            request.state.key_name = k["name"]
            return k
        return dep

    def audit(request: Request, endpoint: str, params: str, status: int, count: int, t0: float):
        request.app.state.audit.log(key_name=getattr(request.state, "key_name", None), endpoint=endpoint, params=params[:1000],
                                    http_status=status, result_count=count, duration_ms=int((time.time() - t0) * 1000),
                                    client_ip=request.client.host if request.client else None)

    def check_code(c: str):
        if not CODE_RE.match(c):
            raise HTTPException(422, "Mã phải gồm 5-30 ký tự chữ/số")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/api/v1/ca-nhan/{ma}")
    def get_one(ma: str, request: Request, loai: Literal["auto", "bhxh", "bhxh_cu", "dinh_danh"] = "auto", _=Depends(auth("lookup"))):
        t0 = time.time()
        check_code(ma)
        r = resolve(request.app.state.repo, [ma], loai)[ma]
        code = {"FOUND": 200, "CONFLICT": 409, "NOT_FOUND": 404}[r["trang_thai"]]
        audit(request, "ca-nhan", f"ma={ma};loai={loai}", code, 1 if code == 200 else 0, t0)
        if code == 200:
            return r
        raise HTTPException(code, detail=r if code == 409 else "Không tìm thấy")

    @app.post("/api/v1/ca-nhan/batch")
    def batch(body: BatchIn, request: Request, loai: Literal["auto", "bhxh", "bhxh_cu", "dinh_danh"] = "auto", _=Depends(auth("batch"))):
        t0 = time.time()
        for c in body.ma:
            check_code(c)
        res = resolve(request.app.state.repo, body.ma, loai)
        out = [dict(ma=c, **res[c]) for c in dict.fromkeys(body.ma)]
        audit(request, "batch", f"n={len(body.ma)};loai={loai}", 200, sum(1 for o in out if o["trang_thai"] == "FOUND"), t0)
        return {"tong": len(out), "tim_thay": sum(1 for o in out if o["trang_thai"] == "FOUND"), "ket_qua": out}

    @app.get("/api/v1/ca-nhan")
    def search(request: Request, ho_ten: str = Query(min_length=2, max_length=100), ngay_sinh: str | None = Query(default=None, pattern=r"^(\d{4}|\d{6}|\d{8})$"),
               kieu: Literal["chinh_xac", "bat_dau"] = "chinh_xac", limit: int = Query(default=20, ge=1, le=100), _=Depends(auth("search"))):
        t0 = time.time()
        rows = request.app.state.repo.search(khong_dau(ho_ten) or "", ngay_sinh, kieu == "bat_dau", limit)
        audit(request, "search", f"ho_ten={ho_ten};ngay_sinh={ngay_sinh};kieu={kieu}", 200, len(rows), t0)
        return {"so_ket_qua": len(rows), "du_lieu": [person(r) for r in rows]}

    @app.get("/api/v1/thay-doi")
    def changes(request: Request, cursor: str | None = None, limit: int = Query(default=1000, ge=1, le=5000), _=Depends(auth("changes"))):
        t0 = time.time()
        after = dec_cursor(cursor) if cursor else None
        rows = request.app.state.repo.changes(after, lag, limit)
        nxt = enc_cursor(rows[-1]["SYNC_DATE"], rows[-1]["ID"]) if rows else cursor
        audit(request, "thay-doi", f"cursor={'Y' if cursor else 'N'};limit={limit}", 200, len(rows), t0)
        return {"so_ban_ghi": len(rows), "next_cursor": nxt, "co_tiep": len(rows) == limit, "du_lieu": [person(r) for r in rows]}

    return app


app = create_app() if os.environ.get("HGD_API_AUTOSTART", "1") == "1" else None
