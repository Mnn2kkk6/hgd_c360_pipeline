"""HGD -> Customer 360 pipeline."""
import sys

# Windows: khi đầu ra đi qua pipe/redirect (vd `| Select-String`), Python dùng cp1252 và lỗi khi in tiếng Việt.
# Ép UTF-8 cho mọi lệnh của dự án (không ảnh hưởng khi in thẳng ra console).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # noqa: BLE001  (stream không hỗ trợ reconfigure)
        pass
