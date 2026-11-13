"""3 DAG vận hành pipeline HGD -> C360.

 hgd_initial_load   (thủ công)  Dựng lần đầu / dựng lại: source CDC -> Full Load Spark -> index/trigger -> sink -> Map -> Full Load C360 -> Data Mart -> đối soát
 hgd_c360_nrt       (mỗi phút)  Gọi PRC_C360_NRT: Queue -> MERGE/DELETE C360. max_active_runs=1 (Oracle còn khóa SYNC_CONTROL lần nữa)
 hgd_c360_maintenance (30 phút) Làm mới Map DBHC + Data Mart; đối soát đầy đủ lúc 02:00 hằng ngày
Không bật DBMS_SCHEDULER (sql/34) cùng lúc với DAG NRT.
"""
from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PY = "cd /opt/project && python -m"
DEFAULT = dict(owner="data-eng", retries=1, retry_delay=timedelta(minutes=1))


def call_proc(proc: str, *args):
    from hgd import db
    conn = db.connect("c360")
    try:
        with conn.cursor() as cur:
            cur.callproc(proc, list(args))
        conn.commit()
    finally:
        conn.close()


def check_queue_health():
    """Cảnh báo (fail task) nếu Queue ứ > 200.000 dòng hoặc dòng cũ nhất quá 15 phút."""
    from hgd import db
    conn = db.connect("c360")
    with conn.cursor() as cur:
        cur.execute("SELECT SO_DONG_CHO, (SYSTIMESTAMP - CU_NHAT) FROM V_QUEUE_BACKLOG")
        n, age = cur.fetchone()
    if n and n > 200_000:
        raise RuntimeError(f"Queue ứ {n} dòng")
    if n and age is not None and age > timedelta(minutes=15):
        raise RuntimeError(f"Dòng Queue cũ nhất {age}")


with DAG("hgd_initial_load", start_date=datetime(2026, 1, 1), schedule=None, catchup=False, default_args=DEFAULT, tags=["hgd"]) as initial:
    steps = [
        BashOperator(task_id="mirror_prepare", bash_command=f"{PY} hgd.sync.mirror_ops prepare"),
        BashOperator(task_id="cdc_source_create", bash_command=f"{PY} hgd.sync.cdc_ops source-create"),
        BashOperator(task_id="spark_full_load", bash_command=f"{PY} hgd.sync.full_load_spark --tables all"),
        BashOperator(task_id="mirror_create_index", bash_command=f"{PY} hgd.sync.mirror_ops create-index"),
        BashOperator(task_id="mirror_create_trigger", bash_command=f"{PY} hgd.sync.mirror_ops create-trigger"),
        BashOperator(task_id="cdc_sink_create", bash_command=f"{PY} hgd.sync.cdc_ops sink-create"),
        PythonOperator(task_id="refresh_map_dbhc", python_callable=call_proc, op_args=["PRC_REFRESH_MAP_DBHC"]),
        PythonOperator(task_id="c360_full_load", python_callable=call_proc, op_args=["PRC_C360_FULL_LOAD"], execution_timeout=timedelta(hours=12)),
        PythonOperator(task_id="refresh_datamart", python_callable=call_proc, op_args=["PRC_REFRESH_DATAMART"]),
        BashOperator(task_id="reconcile", bash_command=f"{PY} hgd.recon.reconcile --sample 500"),
    ]
    for a, b in zip(steps, steps[1:]):
        a >> b

with DAG("hgd_c360_nrt", start_date=datetime(2026, 1, 1), schedule="* * * * *", catchup=False, max_active_runs=1,
         default_args={**DEFAULT, "retries": 0}, tags=["hgd"], dagrun_timeout=timedelta(minutes=5)) as nrt:
    PythonOperator(task_id="run_nrt", python_callable=call_proc, op_args=["PRC_C360_NRT", 20000, 50])

with DAG("hgd_c360_maintenance", start_date=datetime(2026, 1, 1), schedule="*/30 * * * *", catchup=False, max_active_runs=1,
         default_args=DEFAULT, tags=["hgd"]) as maint:
    m = PythonOperator(task_id="refresh_map_dbhc", python_callable=call_proc, op_args=["PRC_REFRESH_MAP_DBHC"])
    d = PythonOperator(task_id="refresh_datamart", python_callable=call_proc, op_args=["PRC_REFRESH_DATAMART"])
    h = PythonOperator(task_id="queue_health", python_callable=check_queue_health)
    m >> d >> h

with DAG("hgd_c360_recon_daily", start_date=datetime(2026, 1, 1), schedule="0 2 * * *", catchup=False, max_active_runs=1,
         default_args=DEFAULT, tags=["hgd"]) as recon:
    BashOperator(task_id="reconcile", bash_command=f"{PY} hgd.recon.reconcile --sample 1000")
