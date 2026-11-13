# Lối tắt các bước chính (Windows: chạy lệnh tương ứng trong từng target).
PY=python

up:                ## hạ tầng lõi
	docker compose up -d oracle zookeeper kafka connect
oracle-archivelog: ## bật ARCHIVELOG (1 lần, sau khi Oracle healthy)
	bash docker/oracle/enable_archivelog.sh
db-init:           ## tạo user, bảng, package, view, procedure
	$(PY) -m hgd.tools.init_db
seed:              ## nạp 100.000 nhân khẩu giả lập + danh mục
	$(PY) -m hgd.generator.seed --people 100000 --truncate
initial-load:      ## dựng lần đầu (CDC source -> Full Load -> index/trigger -> sink -> Map -> C360 Full Load)
	$(PY) -m hgd.sync.mirror_ops prepare
	$(PY) -m hgd.sync.cdc_ops source-create
	$(PY) -m hgd.sync.full_load_spark --tables all
	$(PY) -m hgd.sync.mirror_ops create-index
	$(PY) -m hgd.sync.mirror_ops create-trigger
	$(PY) -m hgd.sync.cdc_ops sink-create
	$(PY) -m hgd.tools.call_proc PRC_REFRESH_MAP_DBHC
	$(PY) -m hgd.tools.call_proc PRC_C360_FULL_LOAD
	$(PY) -m hgd.tools.call_proc PRC_REFRESH_DATAMART
initial-load-py:   ## như initial-load nhưng Full Load bằng Python thuần (không cần Spark/Java/Maven)
	$(PY) -m hgd.sync.mirror_ops prepare
	$(PY) -m hgd.sync.full_load_python --tables all
	$(PY) -m hgd.sync.mirror_ops create-index
	$(PY) -m hgd.sync.mirror_ops create-trigger
	$(PY) -m hgd.tools.call_proc PRC_REFRESH_MAP_DBHC
	$(PY) -m hgd.tools.call_proc PRC_C360_FULL_LOAD
	$(PY) -m hgd.tools.call_proc PRC_REFRESH_DATAMART
nrt-once:          ## chạy tay 1 lượt NRT (khi chưa bật Airflow/Scheduler)
	$(PY) -m hgd.tools.call_proc PRC_C360_NRT
delta:
	$(PY) -m hgd.generator.delta --scenario all
verify:
	$(PY) -m hgd.tools.verify_delta out/delta_manifest.json
recon:
	$(PY) -m hgd.recon.reconcile
api:
	uvicorn hgd.api.app:app --port 8000
bi:
	streamlit run hgd/bi/dashboard.py
test:              ## test không cần Oracle
	$(PY) -m pytest tests -q
