"""Ghi audit log bất đồng bộ theo lô; request không phải chờ INSERT."""
from __future__ import annotations

import logging
import queue
import threading

log = logging.getLogger("hgd.audit")


class AuditLogger:
    def __init__(self, repo, flush_seconds: float = 2.0, batch: int = 200, maxsize: int = 50000):
        if flush_seconds <= 0 or batch <= 0 or maxsize <= 0:
            raise ValueError("flush_seconds, batch và maxsize phải > 0")
        self.repo = repo
        self.flush_seconds = flush_seconds
        self.batch = batch
        self.q: queue.Queue = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True, name="audit-writer")

    def start(self):
        self._t.start()

    def stop(self):
        """Dừng writer rồi xả hết queue (không chỉ một batch cuối)."""
        self._stop.set()
        self._t.join(timeout=10)
        if self._t.is_alive():
            # Không ghi đồng thời từ thread shutdown khi worker vẫn đang ghi DB.
            log.error("audit writer chưa dừng sau 10 giây; còn %d bản ghi trong queue", self.q.qsize())
            return
        while not self.q.empty():
            self._flush()

    def log(self, **row):
        try:
            self.q.put_nowait(row)
        except queue.Full:
            # Ưu tiên không làm chậm API; tình huống mất log được cảnh báo rõ.
            log.warning("audit queue đầy, bỏ 1 dòng")

    def _flush(self):
        rows = []
        while len(rows) < self.batch:
            try:
                rows.append(self.q.get_nowait())
            except queue.Empty:
                break
        if not rows:
            return
        try:
            self.repo.write_audit(rows)
        except Exception:  # noqa: BLE001
            log.exception("ghi audit lỗi (%d dòng); các dòng này không được lưu", len(rows))

    def _run(self):
        while not self._stop.wait(self.flush_seconds):
            while not self.q.empty():
                self._flush()
