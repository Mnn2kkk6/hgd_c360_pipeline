"""Cấu hình đọc từ biến môi trường / file .env (không hard-code thông tin nhạy cảm)."""
from __future__ import annotations
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    f = ROOT / ".env"
    if not f.exists():
        return
    for line in f.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


_load_dotenv()


def env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise RuntimeError(f"Thiếu biến môi trường {name} (xem .env.example)")
    return v


def dsn(root: bool = False) -> str:
    svc = env("ORA_ROOT_SERVICE", "XE") if root else env("ORA_SERVICE", "XEPDB1")
    return f"{env('ORA_HOST', 'localhost')}:{env('ORA_PORT', '1521')}/{svc}"


def credentials(schema: str) -> tuple[str, str]:
    s = schema.lower()
    if s in ("vss", "mirror", "c360"):
        return env(f"{s.upper()}_USER", s.upper()), env(f"{s.upper()}_PASSWORD", f"{s}_pwd")
    raise ValueError(schema)


def jdbc_url() -> str:
    return f"jdbc:oracle:thin:@//{env('ORA_HOST', 'localhost')}:{env('ORA_PORT', '1521')}/{env('ORA_SERVICE', 'XEPDB1')}"
