# Pipeline HGD → Customer 360 (4 tuần)

Triển khai trọn bộ yêu cầu `Yeucau_DataPipeline_HGD_C360_4Tuan.md`: sinh dữ liệu giả có dữ liệu bẩn → Full Load (Spark) + CDC (Debezium/Kafka) sang Mirror → chuẩn hóa C360 (Full Load + Near Real-time) → đối soát → Data Mart/BI → REST API → Airflow.

> **Đọc trước:** [docs/QA.md](docs/QA.md) – chỗ file dữ liệu thật **khác** tài liệu yêu cầu (cấu trúc DM_DBHC, mã xã tái sử dụng, không có ca MO_HO…) và quyết định đã áp dụng.

## Cấu trúc
```
sql/          DDL + package + view + procedure (chạy theo số thứ tự, xem hgd/tools/init_db.py)
hgd/
  generator/  seed.py (nạp nền) · delta.py (biến động + manifest kỳ vọng) · people.py · geo.py
  sync/       full_load_spark.py · cdc_ops.py (Debezium) · mirror_ops.py
  c360/       reference.py  (bản tham chiếu Python dùng cho test/đối soát mẫu)
  recon/      reconcile.py  (đối soát 3 tầng + kiểm mẫu + thống kê DQ)
  api/        FastAPI (4 endpoint) · bi/ dashboard Streamlit · tools/ (init_db, run_sql, call_proc, verify_delta, create_api_key)
airflow/dags/ 4 DAG    docker/ + docker-compose.yml    data/dm/ 3 file danh mục thật    docs/ thiết kế, QA, kế hoạch test
tests/        67 test không cần Oracle
```

## Chạy nhanh (lab, Docker + Python 3.10+)
```bash
cp .env.example .env && pip install -r requirements.txt
make test                      # kiểm tra logic (không cần DB)

make up                        # Oracle XE 21c + Kafka + Debezium Connect (chờ Oracle healthy ~2-3 phút)
make oracle-archivelog         # bật ARCHIVELOG (1 lần) – bắt buộc cho CDC
make db-init                   # tạo user/bảng/package/view/procedure
make seed                      # 100k nhân khẩu giả + 3 danh mục thật
make initial-load              # source CDC → Spark Full Load → index/trigger → sink → Map → C360 Full Load → Data Mart
make nrt-once                  # (hoặc bật DAG hgd_c360_nrt / DBMS_SCHEDULER, chỉ chọn 1)

make delta                     # sinh biến động trên nguồn (chờ 1-2 phút)
make verify                    # C360 khớp kỳ vọng? ;  make recon  # đối soát
python -m hgd.tools.create_api_key --name "Test"   &&   make api   # REST API :8000/docs
make bi                        # dashboard :8501
docker compose --profile orchestration up -d airflow    # Airflow :8080 (DAG: initial_load, nrt, maintenance, recon_daily)
```
Trình tự `initial-load` **không được đảo**: CDC nguồn trước Full Load (không mất thay đổi) → index → trigger → sink sau cùng. Giải thích: `docs/DETAIL_DESIGN.md` §5.

## Windows / PowerShell (không có `make`)
Chạy trực tiếp các lệnh trong `Makefile`. Nếu mạng chặn Maven (không tải được driver Oracle cho Spark / image `connect`): dùng
`python -m hgd.sync.full_load_python --tables all` thay cho bước Spark (target `initial-load-py`). Dashboard/API chạy từ thư mục gốc project.
Lưu ý: `SYSDATE` của Oracle trong Docker là UTC; `sync_date` của API là giờ DB.

## CDC (Debezium) khi mạng chặn Maven
Tải 2 file vào `docker/connect/libs/`: `ojdbc8.jar` (ojdbc8 **21.6.0.0.1**, không dùng ojdbc11 23.x) và `jdbc-plugin.tar.gz`
(debezium-connector-jdbc 2.7.3.Final-plugin) từ một kho Maven truy cập được; sau đó `docker compose up -d --build connect`.
Thứ tự: bật ARCHIVELOG + supplemental logging DB → `cdc_ops source-create` → nạp Mirror → index → trigger → `sink-create` → Full Load C360.

## Tùy biến thường dùng
`seed --people N --mo-ho K --dup-rate 0.04` · `delta --scenario insert,reject --insert 50000 --chunk 2000 --sleep 0.5` · `FULLLOAD_PARTITIONS` trong `.env` · `PRC_C360_NRT(p_batch, p_max_seconds)`.

## Giới hạn đã biết
- Mã Oracle/Spark/Debezium/Airflow **chưa chạy với hệ thống thật** khi đóng gói (chỉ rà cú pháp; phần logic Python đã có test). Các bước nghiệm thu ở `docs/TEST_PLAN.md` mục B.
- Lab dùng 1 instance Oracle XE 21c cho 3 schema; production nên tách nguồn / Mirror-C360 và nâng cấu hình cho 138 triệu dòng (`docs/OPERATIONS.md`).
- Debezium Oracle yêu cầu ARCHIVELOG + user `c##dbzuser` (`sql/01_sys_debezium.sql`); script bật ARCHIVELOG khởi động lại DB trong container.
- API chưa báo bản ghi đã xóa (QA Q13).
