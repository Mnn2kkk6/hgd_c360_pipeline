"""Quản lý connector Debezium (Kafka Connect REST).

  python -m hgd.sync.cdc_ops source-create   # Oracle LogMiner -> Kafka (bắt đầu theo dõi thay đổi, KHÔNG snapshot dữ liệu)
  python -m hgd.sync.cdc_ops sink-create     # Kafka -> Mirror (upsert/delete theo khóa)
  python -m hgd.sync.cdc_ops sink-pause | sink-resume | status | delete <name>

Trình tự đúng (xem README): source-create -> Full Load Mirror -> create-index -> create-trigger -> sink-create.
Vì sao nguồn tạo TRƯỚC Full Load: mọi thay đổi trong lúc Full Load chạy đã nằm trong Kafka; sau Full Load,
sink phát lại theo thứ tự, trạng thái cuối hội tụ về trạng thái mới nhất của nguồn (upsert/delete theo khóa là lũy đẳng).
"""
from __future__ import annotations
import argparse
import json
import sys
import requests
from hgd import config
from hgd.config import env

TABLES = {"VSS.VSS_HO_GD_NHANKHAU": "MA_CD,MA_HO_GD", "VSS.VSS_DM_DANTOC": "ID", "VSS.VSS_DM_QUOC_TICH": "ID", "VSS.VSS_DM_DBHC": "ID"}
SRC, SINK = "hgd-oracle-source", "hgd-mirror-sink"


def source_config() -> dict:
    prefix = env("TOPIC_PREFIX", "hgd")
    return {
        "connector.class": "io.debezium.connector.oracle.OracleConnector",
        "tasks.max": "1",
        "database.hostname": env("CDC_ORA_HOST", "oracle"), "database.port": env("CDC_ORA_PORT", "1521"),
        "database.user": env("DBZ_USER", "c##dbzuser"), "database.password": env("DBZ_PASSWORD", "dbz"),
        "database.dbname": env("ORA_ROOT_SERVICE", "XE"), "database.pdb.name": env("ORA_SERVICE", "XEPDB1"),
        "topic.prefix": prefix,
        "table.include.list": ",".join(TABLES),
        # Bảng nguồn không có PK -> chỉ định khóa logic cho message key (JDBC sink dùng để upsert/delete)
        "message.key.columns": ";".join(f"{t}:{k}" for t, k in TABLES.items()),
        "snapshot.mode": "no_data",                      # chỉ lấy schema; dữ liệu đầu do Spark Full Load
        "log.mining.strategy": "online_catalog",
        "decimal.handling.mode": "double",               # NUMBER không scale -> số, tránh struct VariableScaleDecimal
        "time.precision.mode": "connect",
        "tombstones.on.delete": "false",
        "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
        "schema.history.internal.kafka.topic": f"schema-history.{prefix}",
        # Điều tiết khi nguồn có đợt biến động lớn
        "max.batch.size": "2048", "max.queue.size": "16384", "poll.interval.ms": "500",
        "log.mining.batch.size.default": "20000", "log.mining.sleep.time.default.ms": "1000",
    }


def sink_config() -> dict:
    prefix = env("TOPIC_PREFIX", "hgd")
    return {
        "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
        "tasks.max": "1",
        "connection.url": f"jdbc:oracle:thin:@//{env('CDC_ORA_HOST', 'oracle')}:{env('CDC_ORA_PORT', '1521')}/{env('ORA_SERVICE', 'XEPDB1')}",
        "connection.username": env("MIRROR_USER", "MIRROR"), "connection.password": env("MIRROR_PASSWORD", "mirror_pwd"),
        "topics.regex": rf"{prefix}\.VSS\..*",
        "insert.mode": "upsert", "delete.enabled": "true",
        "primary.key.mode": "record_key", "schema.evolution": "none",
        # Đổi tên topic 'hgd.VSS.VSS_HO_GD_NHANKHAU' -> 'VSS_HO_GD_NHANKHAU' để trùng tên bảng trong schema MIRROR
        # (Debezium 2.7 không có collection.name.format; mặc định đặt tên bảng theo topic)
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": rf"{prefix}\.VSS\.(.*)",
        "transforms.route.replacement": "$1",
        "table.name.format": "${topic}",
        "batch.size": "2000", "consumer.override.max.poll.records": "2000",
    }


def _url(path: str) -> str:
    return env("KAFKA_CONNECT_URL", "http://localhost:8083") + path


def create(name: str, cfg: dict):
    r = requests.put(_url(f"/connectors/{name}/config"), json=cfg, timeout=30)
    print(r.status_code, r.text[:600])
    r.raise_for_status()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["source-create", "sink-create", "sink-pause", "sink-resume", "status", "delete", "print-config"])
    ap.add_argument("name", nargs="?")
    a = ap.parse_args()
    if a.cmd == "source-create":
        create(SRC, source_config())
    elif a.cmd == "sink-create":
        create(SINK, sink_config())
    elif a.cmd in ("sink-pause", "sink-resume"):
        r = requests.put(_url(f"/connectors/{SINK}/{a.cmd.split('-')[1]}"), timeout=30)
        print(r.status_code)
    elif a.cmd == "status":
        for n in (SRC, SINK):
            r = requests.get(_url(f"/connectors/{n}/status"), timeout=30)
            if r.ok:
                j = r.json()
                print(n, j["connector"]["state"], [t["state"] for t in j["tasks"]], [t.get("trace", "")[:200] for t in j["tasks"] if t["state"] == "FAILED"])
            else:
                print(n, "chưa tạo")
    elif a.cmd == "delete":
        print(requests.delete(_url(f"/connectors/{a.name}"), timeout=30).status_code)
    else:
        print(json.dumps({"source": source_config(), "sink": sink_config()}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    sys.exit(main())
