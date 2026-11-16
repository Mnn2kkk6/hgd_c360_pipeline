"""FULL LOAD bằng Python thuần (oracledb): nguồn VSS -> MIRROR. Không cần Spark/Java/driver JDBC.

  python -m hgd.sync.full_load_python                    # nạp mới (TRUNCATE + kế hoạch khối mới)
  python -m hgd.sync.full_load_python --resume           # sau sự cố: chỉ nạp các khối chưa DONE
  python -m hgd.sync.full_load_python --threads 8 --chunks 64 --tables main

Thiết kế (xử lý góp ý review):
 * Chia KHỐI theo khoảng MA_CD (không dùng ORA_HASH): mỗi dòng nguồn chỉ được đọc 1 lần, không còn N lần quét bảng.
 * Mỗi khối là 1 đơn vị nạp + commit riêng, ghi tiến độ vào MIRROR_LOAD_CHUNK. Sự cố giữa chừng -> --resume
   bỏ qua khối DONE, xóa phần ghi dở của khối RUNNING/FAILED rồi nạp lại; không phải nạp lại từ đầu.
 * Lệch số dòng nguồn/Mirror cuối quá trình CHỈ CẢNH BÁO (nguồn còn giao dịch; CDC bù). Tính đầy đủ do trạng thái khối bảo đảm.
   Dùng --strict nếu muốn lệch là lỗi.
"""
from __future__ import annotations
import argparse
import time
from concurrent.futures import ThreadPoolExecutor

from hgd import db
from . import loadplan as lp

DM_TABLES = ["VSS_DM_DANTOC", "VSS_DM_QUOC_TICH", "VSS_DM_DBHC"]
MAIN = "VSS_HO_GD_NHANKHAU"
KEY_COL = {"VSS_HO_GD_NHANKHAU": "MA_CD"}      # bảng không có khóa chia khối -> 1 khối duy nhất


def mirror_columns(table: str) -> list[str]:
    conn = db.connect("mirror")
    with conn.cursor() as cur:
        cur.execute("SELECT column_name FROM user_tab_columns WHERE table_name = :t ORDER BY column_id", t=table)
        cols = [r[0] for r in cur.fetchall() if r[0] not in lp.EXCLUDE_COLS]
    conn.close()
    if not cols:
        raise RuntimeError(f"Mirror chưa có bảng {table}")
    return cols


def copy_chunk(table: str, cols: list[str], chunk: lp.Chunk, key_col: str | None, batch: int) -> int:
    src, dst = db.connect("vss"), db.connect("mirror")
    try:
        lp.mark(dst, table, chunk.chunk_id, "RUNNING")
        where, binds = lp.where_binds(key_col, chunk.lo, chunk.hi)
        if lp.needs_cleanup(chunk.status):           # khối từng chạy dở: xóa phần đã ghi trước khi nạp lại
            with dst.cursor() as c:
                c.execute(f"DELETE FROM {table}{where}", binds)
            dst.commit()
        sel = f"SELECT {','.join(cols)} FROM VSS.{table}{where}"
        ins = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(':' + str(i + 1) for i in range(len(cols)))})"
        n = 0
        with src.cursor() as rc, dst.cursor() as wc:
            rc.arraysize = batch
            rc.execute(sel, binds)
            while True:
                rows = rc.fetchmany(batch)
                if not rows:
                    break
                wc.executemany(ins, rows)
                dst.commit()
                n += len(rows)
        lp.mark(dst, table, chunk.chunk_id, "DONE", rows=n)
        return n
    except Exception as e:                           # noqa: BLE001
        try:
            lp.mark(dst, table, chunk.chunk_id, "FAILED", err=str(e))
        except Exception:                            # noqa: BLE001  (không để lỗi ghi sổ che lỗi gốc)
            pass
        raise
    finally:
        src.close(); dst.close()


def count(schema: str, owner: str | None, table: str) -> int:
    conn = db.connect(schema)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {owner + '.' if owner else ''}{table}")
        n = cur.fetchone()[0]
    conn.close()
    return n


def prepare_plan(table: str, key_col: str | None, chunks: int, resume: bool) -> tuple[list[lp.Chunk], bool]:
    """Trả (danh sách khối, có phải tiếp tục kế hoạch cũ không)."""
    mc = db.connect("mirror")
    try:
        lp.ensure_ctl(mc)
        plan = lp.load_plan(mc, table) if resume else []
        if plan and any(c.status == "DONE" for c in plan):
            return plan, True
        # nạp mới: Mirror rỗng + kế hoạch mới
        with mc.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")
        if key_col and chunks > 1:
            sc = db.connect("vss")
            try:
                pts = lp.split_points(lp.sample_keys(sc, "VSS", table, key_col, chunks), chunks)
            finally:
                sc.close()
        else:
            pts = []
        return lp.reset_plan(mc, table, lp.ranges_from_points(pts)), False
    finally:
        mc.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", default="all", help="all | main | dm")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--chunks", type=int, default=None, help="số khối cho bảng lớn (mặc định 4 x số luồng)")
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--resume", action="store_true", help="tiếp tục: chỉ nạp khối chưa DONE, không TRUNCATE")
    ap.add_argument("--strict", action="store_true", help="lệch số dòng cuối cùng là lỗi (mặc định chỉ cảnh báo)")
    ap.add_argument("--stop-after", type=int, default=None, help="(thử nghiệm) dừng sau N khối để mô phỏng sự cố, rồi chạy lại với --resume")
    a = ap.parse_args()
    tables = {"all": DM_TABLES + [MAIN], "main": [MAIN], "dm": DM_TABLES}[a.tables]
    exit_code = 0
    for t in tables:
        t0 = time.time()
        key_col = KEY_COL.get(t)
        chunks = (a.chunks or a.threads * 4) if key_col else 1
        plan, resumed = prepare_plan(t, key_col, chunks, a.resume)
        todo = [c for c in plan if c.status != "DONE"]
        print(f"{t}: {'TIẾP TỤC' if resumed else 'NẠP MỚI'} - {len(plan)} khối, còn {len(todo)} khối cần nạp")
        if a.stop_after is not None:
            todo = todo[:a.stop_after]
        cols = mirror_columns(t)
        workers = a.threads if key_col else 1
        with ThreadPoolExecutor(workers) as ex:
            futs = [ex.submit(copy_chunk, t, cols, c, key_col, a.batch) for c in todo]
            loaded = sum(f.result() for f in futs)        # f.result() ném lại lỗi của khối; trạng thái FAILED đã được ghi
        mc = db.connect("mirror")
        final = lp.load_plan(mc, t)
        mc.close()
        not_done = [c for c in final if c.status != "DONE"]
        if not_done:
            print(f"  CHƯA XONG {len(not_done)}/{len(final)} khối ({', '.join(str(c.chunk_id) + ':' + c.status for c in not_done[:8])}). "
                  f"Chạy lại với --resume.")
            raise SystemExit(3 if a.stop_after is not None else 1)
        s, m = count("vss", "VSS", t), count("mirror", None, t)
        verdict, msg = lp.count_verdict(s, m, a.strict)
        print(f"  {t:22s} nạp lần này={loaded:>9,} nguồn={s:>9,} mirror={m:>9,} [{verdict}] {msg} ({time.time() - t0:.0f}s)")
        if verdict == "FAIL":
            exit_code = 1
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
