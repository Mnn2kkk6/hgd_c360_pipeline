"""Ghi audit log bất đồng bộ theo lô: request không phải chờ INSERT; flush khi đủ lô hoặc mỗi vài giây."""
from __future__ import annotations
import queue
import threading
import logging

log = logging.getLogger("hgd.audit")


class AuditLogger:
    def __init__(self, repo, flush_seconds: float = 2.0, batch: int = 200, maxsize: int = 50000):
        self.repo, self.flush_seconds, self.batch = repo, flush_seconds, batch
        self.q: queue.Queue = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True, name="audit-writer")

    def start(self):
        self._t.start()

    def stop(self):
        self._stop.set()
        self._t.join(timeout=10)
        self._flush()

    def log(self, **row):
        try:
            self.q.put_nowait(row)
        except queue.Full:      # quá tải: bỏ log chứ không làm chậm API
            log.warning("audit queue đầy, bỏ 1 dòng")

    def _flush(self):
        rows = []
        while len(rows) < self.batch:
            try:
                rows.append(self.q.get_nowait())
            except queue.Empty:
                break
        if rows:
            try:
                self.repo.write_audit(rows)
            except Exception:                    # noqa: BLE001
                log.exception("ghi audit lỗi (%d dòng)", len(rows))

    def _run(self):
        while not self._stop.wait(self.flush_seconds):
            while not self.q.empty():
                self._flush()
