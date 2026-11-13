"""FULL LOAD bằng PySpark: nguồn VSS -> vùng MIRROR (4 bảng), nạp lại nhiều lần KHÔNG nhân đôi (TRUNCATE rồi nạp).

  python -m hgd.sync.full_load_spark                 # tất cả bảng
  python -m hgd.sync.full_load_spark --tables main   # chỉ bảng nhân khẩu
Song song hóa: bảng lớn chia FULLLOAD_PARTITIONS phân vùng theo ORA_HASH(MA_CD) (số nguyên, đều, không lệch tỉnh).
"""
from __future__ import annotations
import argparse
import re
import time
from pathlib import Path

from hgd import config, db
from hgd.config import ROOT, env

DM_TABLES = ["VSS_DM_DANTOC", "VSS_DM_QUOC_TICH", "VSS_DM_DBHC"]
MAIN = "VSS_HO_GD_NHANKHAU"


def bare_number_columns() -> list[str]:
    """Cột NUMBER không precision: Spark mặc định ép DECIMAL(38,10) làm hỏng số nguyên -> khai customSchema LONG."""
    ddl = (ROOT / "sql" / "10_vss_source.sql").read_text(encoding="utf-8")
    block = ddl[ddl.index("CREATE TABLE VSS.VSS_HO_GD_NHANKHAU"):]
    block = block[:block.index("PARTITION BY RANGE")]
    return re.findall(r"^\s+(\w+)\s+NUMBER\s*,?\s*$", block, flags=re.M)


def _jar_conf() -> tuple[str, str]:
    """Ưu tiên driver Oracle có sẵn trên máy (SPARK_OJDBC_JAR hoặc docker/connect/libs/ojdbc*.jar) để khỏi tải Maven
    (mạng chặn repo1.maven.org); nếu không có mới để Spark tải qua spark.jars.packages."""
    import os
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


def _opts(schema: str) -> dict:
    u, p = config.credentials(schema)
    return {"url": config.jdbc_url(), "user": u, "password": p, "driver": "oracle.jdbc.OracleDriver"}


def load_table(spark, table: str, parts: int, fetch: int, batch: int) -> int:
    src = _opts("vss")
    if table == MAIN:
        n = parts
        q = f"(SELECT t.*, ORA_HASH(t.MA_CD, {n - 1}) AS HGD_PART FROM VSS.{table} t) q"
        custom = ",".join(f"{c} LONG" for c in bare_number_columns())
        df = (spark.read.format("jdbc").options(**src).option("dbtable", q).option("partitionColumn", "HGD_PART")
              .option("lowerBound", 0).option("upperBound", n - 1).option("numPartitions", n)
              .option("fetchsize", fetch).option("customSchema", custom).load().drop("HGD_PART"))
    else:
        df = spark.read.format("jdbc").options(**src).option("dbtable", f"VSS.{table}").option("fetchsize", fetch).load()
    # Mirror là bản sao nguyên trạng: không biến đổi dữ liệu. Ghi bằng JDBC batch, mode append sau khi TRUNCATE.
    (df.write.format("jdbc").options(**_opts("mirror")).option("dbtable", table).option("batchsize", batch)
       .option("numPartitions", min(parts, 8)).mode("append").save())
    return 0


def truncate_mirror(tables: list[str]):
    conn = db.connect("mirror")
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f"TRUNCATE TABLE {t}")


def count(schema: str, owner: str | None, table: str) -> int:
    conn = db.connect(schema)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {owner + '.' if owner else ''}{table}")
        return cur.fetchone()[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", default="all", help="all | main | dm")
    ap.add_argument("--no-truncate", action="store_true")
    a = ap.parse_args()
    tables = {"all": DM_TABLES + [MAIN], "main": [MAIN], "dm": DM_TABLES}[a.tables]
    parts, fetch, batch = int(env("FULLLOAD_PARTITIONS", "8")), int(env("FULLLOAD_FETCHSIZE", "10000")), int(env("FULLLOAD_BATCHSIZE", "10000"))
    if not a.no_truncate:
        truncate_mirror(tables)
    spark = spark_session()
    for t in tables:
        t0 = time.time()
        load_table(spark, t, parts, fetch, batch)
        s, m = count("vss", "VSS", t), count("mirror", None, t)
        print(f"{t:22s} nguồn={s:>12,} mirror={m:>12,} {'KHỚP' if s == m else 'LỆCH'}  ({time.time() - t0:.0f}s)")
        if s != m:
            raise SystemExit(f"Full Load {t} lệch số dòng (nguồn có thể đang biến động; CDC sẽ bù phần chênh sau khi bật sink)")
    spark.stop()


if __name__ == "__main__":
    main()
