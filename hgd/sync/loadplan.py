"""Kế hoạch Full Load theo KHỐI khóa (key range) + sổ theo dõi tiến độ để chạy lại từng khối.

Vì sao khóa thay vì ORA_HASH(MA_CD): ORA_HASH buộc mỗi luồng quét TOÀN BỘ bảng nguồn (N luồng = N lần quét).
Chia theo khoảng MA_CD (có unique index (MA_CD, MA_HO_GD) ở nguồn) thì mỗi dòng chỉ được đọc đúng 1 lần.
Ranh giới khối lấy từ một mẫu nhỏ (SAMPLE) nên không phải đếm hay sắp xếp cả bảng.
Sổ tiến độ: MIRROR.MIRROR_LOAD_CHUNK (mỗi khối PENDING/RUNNING/DONE/FAILED) -> --resume chỉ nạp các khối chưa DONE.
"""
from __future__ import annotations
from dataclasses import dataclass

import oracledb

EXCLUDE_COLS = {"SYNC_TS"}          # cột do Mirror tự điền, không có ở nguồn
CTL_TABLE = "MIRROR_LOAD_CHUNK"
CTL_DDL = f"""CREATE TABLE {CTL_TABLE} (
    TABLE_NAME   VARCHAR2(60)  NOT NULL,
    CHUNK_ID     NUMBER(6)     NOT NULL,
    LO_KEY       VARCHAR2(60),
    HI_KEY       VARCHAR2(60),
    STATUS       VARCHAR2(10)  DEFAULT 'PENDING' NOT NULL,
    ROWS_LOADED  NUMBER,
    STARTED_AT   TIMESTAMP,
    FINISHED_AT  TIMESTAMP,
    ERROR_MSG    VARCHAR2(500),
    CONSTRAINT PK_{CTL_TABLE} PRIMARY KEY (TABLE_NAME, CHUNK_ID)
)"""


@dataclass
class Chunk:
    chunk_id: int
    lo: str | None
    hi: str | None
    status: str = "PENDING"
    rows: int | None = None


# ------------------------------------------------------------------ logic thuần (có test)
def split_points(sorted_keys: list[str], parts: int) -> list[str]:
    """Ranh giới khối từ mẫu khóa đã sắp xếp: parts-1 điểm chia theo phân vị, bỏ trùng, tăng chặt."""
    n = len(sorted_keys)
    if parts <= 1 or n < parts:
        return []
    pts: list[str] = []
    for i in range(1, parts):
        k = sorted_keys[min(n - 1, round(n * i / parts))]
        if (not pts or k > pts[-1]) and k > sorted_keys[0]:
            pts.append(k)
    return pts


def ranges_from_points(points: list[str]) -> list[tuple[str | None, str | None]]:
    """[(None,p1),(p1,p2),...,(pn,None)]: lo bao gồm, hi loại trừ; hai đầu mở nên phủ kín mọi khóa (kể cả khóa mới phát sinh)."""
    if not points:
        return [(None, None)]
    bounds = [None, *points, None]
    return [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]


def where_binds(col: str | None, lo: str | None, hi: str | None) -> tuple[str, dict]:
    """Mệnh đề WHERE dạng bind (cho oracledb)."""
    if not col or (lo is None and hi is None):
        return "", {}
    parts, binds = [], {}
    if lo is not None:
        parts.append(f"{col} >= :lo"); binds["lo"] = lo
    if hi is not None:
        parts.append(f"{col} < :hi"); binds["hi"] = hi
    return " WHERE " + " AND ".join(parts), binds


def predicate_literal(col: str | None, lo: str | None, hi: str | None) -> str:
    """Điều kiện dạng literal (cho Spark `predicates`); nhân đôi dấu nháy đơn để an toàn."""
    q = lambda s: "'" + s.replace("'", "''") + "'"
    if not col or (lo is None and hi is None):
        return "1=1"
    parts = []
    if lo is not None: parts.append(f"{col} >= {q(lo)}")
    if hi is not None: parts.append(f"{col} < {q(hi)}")
    return " AND ".join(parts)


def needs_cleanup(status: str) -> bool:
    """Khối từng chạy dở (RUNNING/FAILED) có thể đã ghi một phần vào Mirror -> xóa phần đó trước khi nạp lại."""
    return status in ("RUNNING", "FAILED")


def count_verdict(src: int, mirror: int, strict: bool = False) -> tuple[str, str]:
    """Kết luận so sánh số dòng. Lệch KHÔNG làm dừng chuỗi (nguồn còn giao dịch trong lúc nạp; CDC bù sau).
    Tính đầy đủ của lần nạp do trạng thái từng khối bảo đảm, không do số đếm. strict=True: lệch là FAIL."""
    if src == mirror:
        return "OK", "khớp"
    diff = mirror - src
    pct = abs(diff) * 100 / max(src, 1)
    msg = f"lệch {diff:+,} dòng ({pct:.3f}%)"
    if strict:
        return "FAIL", msg
    return "WARN", msg + " - do nguồn còn thay đổi trong lúc nạp; CDC sẽ bù khi bật sink (kiểm tra lại bằng reconcile)"


# ------------------------------------------------------------------ sổ tiến độ (Oracle)
def ensure_ctl(conn) -> None:
    with conn.cursor() as cur:
        try:
            cur.execute(CTL_DDL)
        except oracledb.DatabaseError as e:
            if e.args[0].code != 955:        # ORA-00955: đã tồn tại
                raise


def reset_plan(conn, table: str, ranges: list[tuple[str | None, str | None]]) -> list[Chunk]:
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {CTL_TABLE} WHERE TABLE_NAME = :t", t=table)
        cur.executemany(f"INSERT INTO {CTL_TABLE} (TABLE_NAME, CHUNK_ID, LO_KEY, HI_KEY) VALUES (:1, :2, :3, :4)",
                        [(table, i, lo, hi) for i, (lo, hi) in enumerate(ranges)])
    conn.commit()
    return [Chunk(i, lo, hi) for i, (lo, hi) in enumerate(ranges)]


def load_plan(conn, table: str) -> list[Chunk]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT CHUNK_ID, LO_KEY, HI_KEY, STATUS, ROWS_LOADED FROM {CTL_TABLE} WHERE TABLE_NAME = :t ORDER BY CHUNK_ID", t=table)
        return [Chunk(*r) for r in cur.fetchall()]


def mark(conn, table: str, chunk_id: int, status: str, rows: int | None = None, err: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""UPDATE {CTL_TABLE} SET STATUS = :s, ROWS_LOADED = NVL(:r, ROWS_LOADED), ERROR_MSG = :e,
                       STARTED_AT = CASE WHEN :s = 'RUNNING' THEN SYSTIMESTAMP ELSE STARTED_AT END,
                       FINISHED_AT = CASE WHEN :s IN ('DONE','FAILED') THEN SYSTIMESTAMP ELSE NULL END
                WHERE TABLE_NAME = :t AND CHUNK_ID = :c""",
            s=status, r=rows, e=(err or None) and err[:500], t=table, c=chunk_id)
    conn.commit()


def sample_keys(src_conn, owner: str, table: str, key_col: str, parts: int) -> list[str]:
    """Mẫu khóa để tính ranh giới: SAMPLE theo thống kê NUM_ROWS, ~100k khóa; bảng nhỏ đọc hết."""
    with src_conn.cursor() as cur:
        cur.execute("SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER = :o AND TABLE_NAME = :t", o=owner, t=table)
        row = cur.fetchone()
        num_rows = (row[0] if row else None) or 0
        target = max(parts * 2000, 100_000)
        sample = "" if num_rows <= target else f" SAMPLE({max(0.0001, target * 100 / num_rows):.6f})"
        cur.execute(f"SELECT {key_col} FROM {owner}.{table}{sample}")
        keys = sorted(r[0] for r in cur.fetchall())
        if len(keys) < parts * 20 and sample:      # mẫu quá thưa -> đọc hết cột khóa (chỉ quét chỉ mục)
            cur.execute(f"SELECT {key_col} FROM {owner}.{table}")
            keys = sorted(r[0] for r in cur.fetchall())
    return keys
