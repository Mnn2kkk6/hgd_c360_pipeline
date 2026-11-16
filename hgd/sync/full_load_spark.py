"""FULL LOAD bằng PySpark: nguồn VSS -> MIRROR. Dùng cho khối lượng lớn.

  python -m hgd.sync.full_load_spark                 # nạp mới
  python -m hgd.sync.full_load_spark --resume        # sau sự cố: chỉ nạp các khối chưa DONE
Khác bản trước (xử lý góp ý review):
 * Chia khối theo khoảng MA_CD và truyền cho Spark qua `predicates` (không dùng ORA_HASH): mỗi dòng đọc 1 lần.
 * Nạp theo "đợt" (mỗi đợt = N khối chạy song song, 1 job Spark); trạng thái từng khối ghi vào MIRROR_LOAD_CHUNK,
   sự cố giữa chừng không phải nạp lại từ đầu (--resume).
 * Lệch số dòng cuối cùng chỉ CẢNH BÁO (CDC bù); --strict để coi là lỗi.
"""
from __future__ import annotations
import argparse
import os
import re
import time
from pathlib import Path

from hgd import config, db
from hgd.config import ROOT, env
from . import loadplan as lp
from .full_load_python import DM_TABLES, KEY_COL, MAIN, count, prepare_plan


def bare_number_columns() -> list[str]:
    """Cột NUMBER không precision: Spark mặc định ép DECIMAL(38,10) làm hỏng số nguyên -> khai customSchema LONG."""
    ddl = (ROOT / "sql" / "10_vss_source.sql").read_text(encoding="utf-8")
    block = ddl[ddl.index("CREATE TABLE VSS.VSS_HO_GD_NHANKHAU"):]
    block = block[:block.index("PARTITION BY RANGE")]
    return re.findall(r"^\s+(\w+)\s+NUMBER\s*,?\s*$", block, flags=re.M)


def _jar_conf() -> tuple[str, str]:
    """Ưu tiên driver Oracle có sẵn trên máy (SPARK_OJDBC_JAR hoặc docker/connect/libs/ojdbc*.jar) để khỏi tải Maven."""
    jar = os.environ.get("SPARK_OJDBC_JAR")
    if not jar:
        found = sorted((ROOT / "docker" / "connect" / "libs").glob("ojdbc*.jar"))
        jar = str(found[0]) if found else None
    if jar:
        return "spark.jars", Path(jar).resolve().as_uri()
    return "spark.jars.packages", env("SPARK_OJDBC_PACKAGE", "com.oracle.database.jdbc:ojdbc11:23.5.0.24.07")


def spark_session():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("hgd_full_load")
            .config(*_jar_conf())
            .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh").getOrCreate())


def _props(schema: str, **extra) -> dict:
    u, p = config.credentials(schema)
    return {"user": u, "password": p, "driver": "oracle.jdbc.OracleDriver", **{k: str(v) for k, v in extra.items()}}


def load_wave(spark, table: str, key_col: str | None, wave: list[lp.Chunk], fetch: int, batch: int, parts_write: int) -> None:
    mc = db.connect("mirror")
    try:
        for c in wave:
            lp.mark(mc, table, c.chunk_id, "RUNNING")
            if lp.needs_cleanup(c.status):               # khối từng chạy dở: xóa phần ghi dở trước khi nạp lại
                where, binds = lp.where_binds(key_col, c.lo, c.hi)
                with mc.cursor() as cur:
                    cur.execute(f"DELETE FROM {table}{where}", binds)
                mc.commit()
        preds = [lp.predicate_literal(key_col, c.lo, c.hi) for c in wave]
        extra = {"fetchsize": fetch}
        if table == MAIN:
            extra["customSchema"] = ",".join(f"{c} LONG" for c in bare_number_columns())
        df = spark.read.jdbc(url=config.jdbc_url(), table=f"VSS.{table}", predicates=preds, properties=_props("vss", **extra))
        (df.repartition(min(parts_write, max(len(wave), 1))).write
           .jdbc(url=config.jdbc_url(), table=table, mode="append", properties=_props("mirror", batchsize=batch)))
        for c in wave:
            lp.mark(mc, table, c.chunk_id, "DONE")
    except Exception as e:                               # noqa: BLE001
        for c in wave:
            try:
                lp.mark(mc, table, c.chunk_id, "FAILED", err=str(e))
            except Exception:                            # noqa: BLE001
                pass
        raise
    finally:
        mc.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", default="all", help="all | main | dm")
    ap.add_argument("--chunks", type=int, default=None, help="số khối cho bảng lớn (mặc định 4 x FULLLOAD_PARTITIONS)")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--strict", action="store_true")
    ap.add_argument("--stop-after", type=int, default=None, help="(thử nghiệm) dừng sau N đợt để mô phỏng sự cố")
    a = ap.parse_args()
    tables = {"all": DM_TABLES + [MAIN], "main": [MAIN], "dm": DM_TABLES}[a.tables]
    par, fetch, batch = int(env("FULLLOAD_PARTITIONS", "8")), int(env("FULLLOAD_FETCHSIZE", "10000")), int(env("FULLLOAD_BATCHSIZE", "10000"))
    spark = spark_session()
    exit_code = 0
    for t in tables:
        t0 = time.time()
        key_col = KEY_COL.get(t)
        chunks = (a.chunks or par * 4) if key_col else 1
        plan, resumed = prepare_plan(t, key_col, chunks, a.resume)
        todo = [c for c in plan if c.status != "DONE"]
        print(f"{t}: {'TIẾP TỤC' if resumed else 'NẠP MỚI'} - {len(plan)} khối, còn {len(todo)} khối")
        waves = [todo[i:i + par] for i in range(0, len(todo), par)] if key_col else [todo]
        for n, wave in enumerate(waves):
            if a.stop_after is not None and n >= a.stop_after:
                break
            if wave:
                load_wave(spark, t, key_col, wave, fetch, batch, par)
        mc = db.connect("mirror"); final = lp.load_plan(mc, t); mc.close()
        not_done = [c for c in final if c.status != "DONE"]
        if not_done:
            print(f"  CHƯA XONG {len(not_done)}/{len(final)} khối. Chạy lại với --resume.")
            raise SystemExit(3 if a.stop_after is not None else 1)
        s, m = count("vss", "VSS", t), count("mirror", None, t)
        verdict, msg = lp.count_verdict(s, m, a.strict)
        print(f"  {t:22s} nguồn={s:>12,} mirror={m:>12,} [{verdict}] {msg} ({time.time() - t0:.0f}s)")
        if verdict == "FAIL":
            exit_code = 1
    spark.stop()
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
