# HGD → Customer 360 Data Pipeline

Pipeline dữ liệu End-to-End cho bài toán Hộ gia đình – Nhân khẩu (HGD): sinh dữ liệu nguồn, đồng bộ Batch/CDC sang vùng Mirror, chuẩn hóa thành Customer 360, đối soát dữ liệu, cung cấp Data Mart/BI và REST API. Airflow được sử dụng để điều phối các tác vụ định kỳ.

## 1. Tổng quan kiến trúc

~~~text
                         ┌─────────────────────────────┐
                         │       Oracle - VSS          │
                         │  HGD_NHANKHAU + 3 danh mục  │
                         └──────────────┬──────────────┘
                                        │
                  ┌─────────────────────┴──────────────────────┐
                  │                                            │
           Full Load (Python/Spark)                    CDC (Debezium)
                  │                                      LogMiner
                  │                                            │
                  │                                         Kafka
                  │                                            │
                  └─────────────────────┬──────────────────────┘
                                        ▼
                         ┌─────────────────────────────┐
                         │       Oracle - MIRROR       │
                         │   Bản sao dữ liệu nguồn     │
                         └──────────────┬──────────────┘
                                        │
                              Trigger ghi MA_CD vào Queue
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │        Oracle - C360        │
                         │ Map DBHC → Chuẩn hóa → DQ   │
                         │ Full Load + Near Real-time  │
                         └──────────┬──────────┬───────┘
                                    │          │
                           ┌────────┘          └─────────┐
                           ▼                             ▼
                 Data Mart / Streamlit BI          FastAPI REST API
                           │
                           ▼
                  Dashboard & Monitoring

          Airflow: điều phối Full Load, NRT, maintenance, reconciliation
          Reconciliation: kiểm tra Source ↔ Mirror ↔ C360
~~~

### Các thành phần chính

| Thành phần | Công nghệ | Vai trò |
|---|---|---|
| Source / Mirror / C360 | Oracle Database XE 21c | Lưu dữ liệu nguồn giả lập, bản sao Mirror, dữ liệu công dân chuẩn hóa và các bảng điều khiển |
| Data Generator | Python, oracledb | Sinh dữ liệu nhân khẩu và dữ liệu danh mục; tạo các trường hợp dữ liệu lỗi, trùng hộ và biến động |
| Full Load | Python oracledb hoặc PySpark JDBC | Nạp dữ liệu ban đầu từ VSS sang Mirror theo lô |
| CDC | Debezium Oracle (LogMiner), Kafka, Debezium JDBC Sink | Bắt INSERT/UPDATE/DELETE và đồng bộ thay đổi sang Mirror |
| C360 ETL | PL/SQL, View, Procedure | Lọc nghiệp vụ, khử trùng, chuẩn hóa địa bàn/thông tin cá nhân và gắn cờ chất lượng |
| Near Real-time | Oracle Trigger, Queue, Procedure | Gom khóa công dân thay đổi và cập nhật C360 theo lô |
| Reconciliation | Python, SQL | Đối soát số lượng và chất lượng dữ liệu giữa các tầng |
| BI | Streamlit, Plotly, Oracle Data Mart | Dashboard nhân khẩu, chất lượng dữ liệu và giám sát đồng bộ |
| REST API | FastAPI, python-oracledb | Tra cứu công dân, tìm kiếm, tra cứu theo lô và lấy dữ liệu thay đổi |
| Orchestration | Apache Airflow | Lập lịch NRT, bảo trì Map/Data Mart và đối soát định kỳ |

## 2. Chức năng của dự án

### 2.1. Sinh dữ liệu nguồn

- Nạp 3 bảng danh mục từ các file CSV trong **data/dm/**.
- Sinh dữ liệu nhân khẩu giả lập với số lượng tùy chỉnh.
- Cài cắm dữ liệu thực tế và dữ liệu lỗi: công dân có nhiều hộ, hồ sơ chưa duyệt/bị từ chối, mã tỉnh/xã cũ, ngày sinh rút gọn hoặc sai lịch, số điện thoại và định danh không hợp lệ.
- Có các ca dữ liệu mẫu (golden cases) phục vụ kiểm thử.
- Sinh các biến động INSERT, UPDATE, DELETE và manifest kỳ vọng để kiểm tra kết quả sau CDC/NRT.

### 2.2. Đồng bộ Source → Mirror

- Full Load hỗ trợ Python thuần hoặc PySpark JDBC.
- Luồng CDC sử dụng Debezium Oracle LogMiner → Kafka → JDBC Sink.
- Khóa logic của bảng nhân khẩu là cặp **(MA_CD, MA_HO_GD)**, tránh ghi đè các dòng của cùng một công dân ở nhiều hộ.
- Full Load Python chia dữ liệu theo khoảng MA_CD, ghi nhận trạng thái từng khối và hỗ trợ **--resume** khi có sự cố.
- Mirror giữ dữ liệu gần với nguồn; việc chuẩn hóa được thực hiện ở tầng C360.

### 2.3. Chuẩn hóa Mirror → Customer 360

- Lọc bản ghi giảm/đóng mã và hồ sơ chưa được phê duyệt, bị từ chối hoặc đã hủy.
- Khử trùng theo MA_CD, chọn dòng hợp lệ mới nhất theo MODIFY_DATE, sau đó CREATED_DATE và ID.
- Làm sạch họ tên, ngày sinh, số điện thoại và số định danh.
- Chuẩn hóa mã tỉnh/xã sau sáp nhập; hỗ trợ map mã hiện hành, mã cũ và trường hợp xã mơ hồ.
- Giữ lại giá trị gốc để đối soát và đưa các lỗi vào DQ_FLAGS, không loại bỏ toàn bộ công dân chỉ vì dữ liệu có lỗi.
- Dùng chung View chuẩn hóa cho Full Load C360 và cập nhật Near Real-time.

### 2.4. Cập nhật Near Real-time, đối soát và khai thác

- Trigger trên Mirror chỉ ghi nhận khóa MA_CD vào Queue; Procedure xử lý Queue theo lô để MERGE hoặc DELETE dữ liệu C360.
- Có cơ chế khóa điều phối giữa Full Load và NRT, cùng snapshot Queue để tránh xóa nhầm các thay đổi phát sinh trong lúc Full Load.
- Reconciliation kiểm tra Source ↔ Mirror, Mirror ↔ C360, kiểm mẫu và thống kê cờ DQ.
- Data Mart phục vụ 3 nhóm Dashboard: nhân khẩu/định danh, chất lượng dữ liệu và giám sát đồng bộ.
- REST API hỗ trợ tra cứu theo mã, tra cứu theo lô, tìm kiếm họ tên/ngày sinh và lấy danh sách bản ghi thay đổi.

## 3. Cấu trúc repository

~~~text
hgd_c360_pipeline/
├── airflow/
│   └── dags/
│       └── hgd_c360_dags.py       # 4 DAG điều phối pipeline
│
├── data/
│   └── dm/                        # CSV danh mục Dân tộc, Quốc tịch, Địa bàn
│
├── docker/
│   ├── airflow/
│   │   └── Dockerfile              # Image Airflow
│   ├── connect/
│   │   ├── Dockerfile              # Debezium Connect + Oracle JDBC + JDBC Sink
│   │   └── libs/                   # Driver/plugin đặt local, không commit binary
│   └── oracle/
│       └── enable_archivelog.sh    # Bật ARCHIVELOG cho Oracle
│
├── docs/
│   ├── BASIC_DESIGN.md             # Thiết kế tổng thể
│   ├── DETAIL_DESIGN.md            # Thiết kế chi tiết DB, CDC, Full Load, Queue/NRT
│   ├── QA.md                       # Khác biệt dữ liệu thực tế và quyết định xử lý
│   ├── REVIEW_RESPONSE.md          # Phản hồi góp ý/review và thay đổi đã thực hiện
│   ├── TEST_PLAN.md                # Kịch bản kiểm thử và tiêu chí nghiệm thu
│   ├── BI_GUIDE.md                 # Hướng dẫn kết nối BI
│   └── OPERATIONS.md               # Vận hành và xử lý sự cố
│
├── hgd/
│   ├── api/
│   │   ├── app.py                  # FastAPI endpoints, xác thực API key
│   │   ├── repo.py                 # Truy vấn dữ liệu C360
│   │   └── audit.py                # Audit log bất đồng bộ
│   ├── bi/
│   │   └── dashboard.py            # Dashboard Streamlit
│   ├── c360/
│   │   └── reference.py            # Logic chuẩn hóa tham chiếu dùng cho kiểm thử
│   ├── common/
│   │   └── vn_text.py              # Chuẩn hóa văn bản tiếng Việt
│   ├── generator/
│   │   ├── seed.py                 # Sinh/nạp dữ liệu ban đầu
│   │   ├── people.py               # Sinh nhân khẩu và golden cases
│   │   ├── geo.py                  # Tạo dữ liệu địa bàn giả lập
│   │   └── delta.py                # Sinh biến động và manifest kỳ vọng
│   ├── recon/
│   │   └── reconcile.py            # Đối soát ba tầng dữ liệu
│   ├── sync/
│   │   ├── full_load_python.py     # Full Load bằng python-oracledb
│   │   ├── full_load_spark.py      # Full Load bằng PySpark JDBC
│   │   ├── loadplan.py             # Chia khối và theo dõi tiến độ Full Load
│   │   ├── cdc_ops.py              # Tạo/quản lý Debezium Source và Sink
│   │   └── mirror_ops.py           # Chuẩn bị Mirror, tạo Index và Trigger
│   ├── tools/
│   │   ├── init_db.py              # Khởi tạo các schema và đối tượng DB
│   │   ├── call_proc.py            # Gọi Stored Procedure
│   │   ├── run_sql.py              # Chạy file SQL theo schema
│   │   ├── create_api_key.py       # Tạo API key
│   │   └── verify_delta.py         # Kiểm tra kết quả theo manifest
│   ├── config.py                  # Đọc cấu hình từ .env
│   └── db.py                      # Kết nối Oracle và thực thi SQL
│
├── sql/
│   ├── 00_sys_users.sql            # Tạo user/schema
│   ├── 01_sys_debezium.sql         # Quyền và cấu hình user Debezium
│   ├── 02_grants.sql               # Cấp quyền giữa các schema
│   ├── 10_vss_source.sql           # Bảng nguồn giả lập
│   ├── 20_c360_tables.sql          # Bảng C360, Queue, control, log, API...
│   ├── 21_mirror_tables.sql        # Bảng Mirror
│   ├── 22_mirror_trigger.sql       # Trigger đưa khóa vào Queue
│   ├── 23_mirror_sync_ts.sql       # Dấu thời gian đồng bộ Mirror
│   ├── 30_pkg_util.sql             # Hàm/package tiện ích
│   ├── 31_map_dbhc.sql             # View và procedure Map địa bàn
│   ├── 32_view_normalized.sql      # View chuẩn hóa dùng chung
│   ├── 33_procs_sync.sql           # Procedure Full Load và NRT
│   ├── 34_scheduler_job.sql        # Scheduler NRT tùy chọn
│   ├── 40_datamart_bi.sql          # Data Mart phục vụ BI
│   └── 41_mirror_lag_view.sql      # View giám sát độ trễ Mirror
│
├── tests/                         # Unit test cho generator, reference, API, load plan...
├── out/                           # Manifest và kết quả chạy cục bộ
├── docker-compose.yml
├── Makefile                       # Lệnh tắt cho môi trường có make
├── requirements.txt
├── .env.example
└── README.md
~~~

## 4. Hướng dẫn cài đặt và chạy trên Windows

Các lệnh bên dưới dùng **PowerShell**, chạy tại thư mục gốc repository. Với Windows, hướng dẫn sử dụng Full Load Python để không bắt buộc cài Java/Spark. Các bước Spark và Airflow được tách riêng ở phần sau.

### 4.1. Yêu cầu môi trường

- Windows 10/11, Git và Docker Desktop (đã bật WSL2 backend).
- Python 3.10 hoặc 3.11.
- Docker Desktop có đủ tài nguyên; Docker Compose hiện tại cần khoảng 8 GB RAM trống nếu chạy toàn bộ dịch vụ.
- Có kết nối Internet để tải các Docker image và cài package.
- Git Bash hoặc WSL để chạy script bật ARCHIVELOG. Có hướng dẫn thay thế bằng PowerShell ở bước 4.4.

### 4.2. Clone repository, tạo môi trường Python

~~~powershell
git clone https://github.com/Mnn2kkk6/hgd_c360_pipeline.git
cd hgd_c360_pipeline

py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install "uvicorn[standard]"
Copy-Item .env.example .env
~~~

Nếu PowerShell chặn kích hoạt virtual environment, chạy lệnh sau trong cửa sổ hiện tại rồi kích hoạt lại:

~~~powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
~~~

Mở file **.env** và kiểm tra các thông số kết nối. Với Python chạy trực tiếp trên Windows host, cấu hình mặc định trong .env.example phù hợp:

- **ORA_HOST=localhost**: Python kết nối Oracle qua cổng host 1521.
- **CDC_ORA_HOST=oracle**: hostname mà Debezium Connect container sử dụng để kết nối Oracle trong Docker network.
- **KAFKA_CONNECT_URL=http://localhost:8083**: Python trên Windows gọi Kafka Connect qua cổng publish ra host.
- **ORA_SERVICE=XEPDB1**, **ORA_ROOT_SERVICE=XE**: service của PDB và CDB.
- Các mật khẩu lab có thể giữ nguyên khi chạy local; cần đổi nếu triển khai môi trường dùng chung.

Không commit file .env hoặc thông tin xác thực lên Git.

### 4.3. Chuẩn bị Debezium connector và Oracle JDBC driver

Dockerfile của Connect hiện COPY plugin và driver từ thư mục local. Vì vậy, trước khi build service **connect**, cần chuẩn bị đủ hai file sau:

~~~text
docker/connect/libs/
├── ojdbc8.jar          # Oracle JDBC 21.6.0.0.1
└── jdbc-plugin.tar.gz  # Debezium JDBC connector plugin 2.7.3.Final
~~~

Đặt đúng tên file như trên. Hai file binary không được lưu trong repository. Lưu ý driver **ojdbc8 21.6** được sử dụng cho Debezium 2.7; không thay bằng ojdbc11 23.x cho cấu hình Connect hiện tại vì có thể gặp lỗi xác thực Oracle.

### 4.4. Khởi động Oracle, Kafka và bật ARCHIVELOG

Khởi động các dịch vụ nền trước:

~~~powershell
docker compose up -d oracle zookeeper kafka
docker compose ps
~~~

Đợi Oracle khởi động xong và có trạng thái **healthy**:

~~~powershell
docker inspect --format='{{.State.Health.Status}}' hgd-oracle
~~~

Debezium Oracle LogMiner yêu cầu Oracle bật ARCHIVELOG và supplemental logging. Sau khi Oracle đã healthy, chạy từ Git Bash/WSL tại thư mục gốc project:

~~~bash
bash docker/oracle/enable_archivelog.sh
~~~

Nếu muốn thao tác hoàn toàn trong PowerShell, có thể chạy tương đương:

~~~powershell
docker exec hgd-oracle bash -c "mkdir -p /opt/oracle/oradata/recovery_area"

@'
ALTER SYSTEM SET db_recovery_file_dest_size = 10G;
ALTER SYSTEM SET db_recovery_file_dest = '/opt/oracle/oradata/recovery_area';
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ARCHIVE LOG LIST
'@ | docker exec -i hgd-oracle sqlplus -s / as sysdba
~~~

Sau đó build và khởi động Kafka Connect:

~~~powershell
docker compose build connect
docker compose up -d connect
docker compose ps
~~~

Kiểm tra Connect REST API đã sẵn sàng:

~~~powershell
Invoke-RestMethod http://localhost:8083/
~~~

### 4.5. Khởi tạo database và sinh dữ liệu nguồn

Khởi tạo các schema VSS, MIRROR, C360 cùng bảng, package, view và procedure:

~~~powershell
python -m hgd.tools.init_db
~~~

Nạp 100.000 nhân khẩu giả lập và 3 bảng danh mục:

~~~powershell
python -m hgd.generator.seed --people 100000 --seed 42 --truncate
~~~

Có thể thay đổi số lượng nhân khẩu, ví dụ:

~~~powershell
python -m hgd.generator.seed --people 50000 --seed 42 --truncate
~~~

**--truncate** xóa dữ liệu nhân khẩu hiện có ở bảng nguồn trước khi nạp lại. Chỉ dùng khi muốn tạo lại dữ liệu lab; không chạy tùy tiện trên môi trường đang kiểm thử biến động.

### 4.6. Chạy Full Load và khởi tạo C360 lần đầu

**Quan trọng: không đảo thứ tự các bước dưới đây.** CDC Source phải được tạo trước Full Load để Kafka bắt đầu ghi nhận thay đổi trong thời gian nạp. Sink và Trigger chỉ bật sau khi hoàn tất Full Load C360.

Chạy lần lượt các lệnh sau trong PowerShell:

~~~powershell
# 1. Chuẩn bị Mirror trước khi nạp
python -m hgd.sync.mirror_ops prepare

# 2. Tạo Debezium Oracle Source (LogMiner -> Kafka)
python -m hgd.sync.cdc_ops source-create

# 3. Full Load từ VSS sang MIRROR bằng Python
python -m hgd.sync.full_load_python --tables all --threads 4 --batch 5000

# 4. Tạo Unique Index và index phục vụ truy vấn trên Mirror
python -m hgd.sync.mirror_ops create-index

# 5. Làm mới Map địa bàn và nạp bảng C360 lần đầu
python -m hgd.tools.call_proc PRC_REFRESH_MAP_DBHC
python -m hgd.tools.call_proc PRC_C360_FULL_LOAD

# 6. Bật Trigger Queue, sau đó tạo Debezium JDBC Sink (Kafka -> Mirror)
python -m hgd.sync.mirror_ops create-trigger
python -m hgd.sync.cdc_ops sink-create

# 7. Làm mới Data Mart
python -m hgd.tools.call_proc PRC_REFRESH_DATAMART

# 8. Kiểm tra trạng thái hai connector
python -m hgd.sync.cdc_ops status
~~~

Full Load Python hỗ trợ tiếp tục sau sự cố. Ví dụ:

~~~powershell
python -m hgd.sync.full_load_python --tables main --resume
~~~

Lệnh **--resume** dùng khi kế hoạch Full Load trước đó còn khối RUNNING/FAILED; không dùng thay cho một lần nạp mới có chủ đích.

### 4.7. Kiểm thử luồng CDC → Near Real-time C360

Sau khi hai connector hoạt động, sinh dữ liệu biến động trên bảng nguồn:

~~~powershell
python -m hgd.generator.delta --scenario all --insert 5000 --update 300 --chunk 1000 --sleep 0.2
~~~

Các kịch bản gồm: thêm công dân, thêm hộ mới cho công dân hiện hữu, bổ sung CCCD, sửa xã/ngày sinh, duyệt hồ sơ, từ chối hồ sơ, đổi MA_CD, xóa hộ mới nhất và xóa toàn bộ bản ghi của một công dân.

Đợi CDC/Sink đồng bộ thay đổi về Mirror, sau đó gọi procedure NRT để xử lý Queue:

~~~powershell
Start-Sleep -Seconds 60
python -m hgd.tools.call_proc PRC_C360_NRT 20000 50
~~~

Nếu Queue còn dữ liệu, có thể gọi procedure thêm lần nữa hoặc sử dụng DAG NRT ở Airflow. Kiểm tra kết quả so với manifest do Delta Generator tạo và chạy đối soát:

~~~powershell
python -m hgd.tools.verify_delta out/delta_manifest.json
python -m hgd.recon.reconcile --sample 500
~~~

Kết quả manifest và báo cáo đối soát giúp kiểm tra dữ liệu đã hội tụ giữa Source, Mirror và C360 hay chưa. Chi tiết các ca kiểm thử xem tại [docs/TEST_PLAN.md](docs/TEST_PLAN.md).

## 5. Chạy REST API và Dashboard BI

Hai dịch vụ này chạy từ thư mục gốc project, trong môi trường Python đã kích hoạt và sau khi đã có dữ liệu C360/Data Mart.

### 5.1. REST API

Tạo API key. Key chỉ được in ra một lần, hãy lưu lại để sử dụng trong request:

~~~powershell
python -m hgd.tools.create_api_key --name "Local test" --scopes lookup,batch,search,changes
~~~

Khởi động API:

~~~powershell
uvicorn hgd.api.app:app --host 127.0.0.1 --port 8000
~~~

Mở Swagger UI tại: http://127.0.0.1:8000/docs

Các endpoint hiện có:

| Method | Endpoint | Mục đích |
|---|---|---|
| GET | /api/v1/ca-nhan/{ma}?loai=auto | Tra cứu một công dân theo mã BHXH, mã cũ hoặc số định danh |
| POST | /api/v1/ca-nhan/batch | Tra cứu theo lô, tối đa 1.000 mã |
| GET | /api/v1/ca-nhan?ho_ten=...&ngay_sinh=... | Tìm kiếm theo họ tên và ngày sinh |
| GET | /api/v1/thay-doi?cursor=...&limit=... | Lấy danh sách bản ghi thay đổi theo cursor |
| GET | /health | Kiểm tra trạng thái API |

Các endpoint nghiệp vụ yêu cầu header **X-API-Key**. API key được lưu dưới dạng SHA-256 hash trong database; scope có thể giới hạn theo **lookup**, **batch**, **search**, **changes**.

### 5.2. Dashboard BI

Mở một PowerShell terminal khác tại thư mục gốc và chạy:

~~~powershell
streamlit run hgd/bi/dashboard.py --server.port 8501
~~~

Dashboard mặc định có 3 tab:

1. **Nhân khẩu & định danh:** số lượng công dân theo địa bàn, tỷ lệ có số định danh, cơ cấu tuổi và giới tính.
2. **Chất lượng dữ liệu:** thống kê các loại lỗi trong DQ_FLAGS và tỷ lệ lỗi theo địa phương.
3. **Giám sát đồng bộ:** Queue backlog, lịch sử NRT, độ trễ Mirror, lỗi đồng bộ và kết quả đối soát.

Có thể kết nối Superset, Metabase hoặc Power BI trực tiếp vào các bảng/view Data Mart; xem [docs/BI_GUIDE.md](docs/BI_GUIDE.md).

## 6. Airflow và các dịch vụ giám sát (tùy chọn)

Airflow không bắt buộc để chạy thử từng bước thủ công. Chỉ khởi động sau khi các dịch vụ lõi hoạt động ổn định.

### 6.1. Cấu hình kết nối khi chạy trong Docker

Python chạy từ Windows host sử dụng **KAFKA_CONNECT_URL=http://localhost:8083**. Tuy nhiên, task chạy bên trong container Airflow phải gọi service Connect qua Docker network. Trong **docker-compose.yml**, phần **airflow.environment** cần có:

~~~yaml
environment:
  ORA_HOST: oracle
  KAFKA_CONNECT_URL: http://connect:8083
  PYTHONPATH: /opt/project
~~~

Giữ **KAFKA_CONNECT_URL=http://localhost:8083** trong file .env cho các lệnh Python chạy trực tiếp trên Windows. Hai địa chỉ khác nhau vì localhost bên trong container Airflow chính là container Airflow, không phải container Connect.

### 6.2. Khởi động Airflow

~~~powershell
docker compose --profile orchestration up -d airflow
docker compose logs -f airflow
~~~

Mở giao diện tại http://localhost:8080. Các DAG được khai báo trong **airflow/dags/hgd_c360_dags.py**:

| DAG | Lịch chạy | Vai trò |
|---|---|---|
| hgd_initial_load | Thủ công | Dựng lại Source CDC, Full Load Mirror, Map, C360, Trigger, Sink, Data Mart và đối soát |
| hgd_c360_nrt | Mỗi phút | Xử lý Queue và cập nhật C360 |
| hgd_c360_maintenance | Mỗi 30 phút | Refresh Map, Data Mart và kiểm tra Queue backlog |
| hgd_c360_recon_daily | 02:00 hằng ngày | Đối soát dữ liệu định kỳ |

Nếu đã khởi tạo pipeline thủ công ở phần 4.6, không cần chạy lại DAG **hgd_initial_load** trừ khi chủ đích dựng lại dữ liệu. Có thể bật DAG NRT và các DAG bảo trì/đối soát để vận hành định kỳ.

### 6.3. Kafka UI (tùy chọn)

~~~powershell
docker compose --profile tools up -d kafka-ui
~~~

Mở http://localhost:8088 để quan sát Kafka topics và trạng thái Kafka Connect.

**Lưu ý:** Chỉ chọn một cơ chế lập lịch NRT: Airflow DAG **hgd_c360_nrt** hoặc Oracle DBMS_SCHEDULER trong **sql/34_scheduler_job.sql**. Không bật đồng thời cả hai.

## 7. Chạy Full Load bằng Spark (tùy chọn)

Nếu muốn sử dụng PySpark thay cho Full Load Python:

- Cài Java tương thích với PySpark 3.5 và cấu hình JAVA_HOME.
- Đảm bảo Spark có thể tải Oracle JDBC driver từ Maven, hoặc cấu hình driver JAR cục bộ theo hướng dẫn vận hành.
- Kiểm tra FULLLOAD_PARTITIONS, FULLLOAD_FETCHSIZE, FULLLOAD_BATCHSIZE trong .env.

Thay bước Full Load Python ở mục 4.6 bằng:

~~~powershell
python -m hgd.sync.full_load_spark --tables all
~~~

Chỉ chạy **một** phương án Full Load cho mỗi lần khởi tạo: Python hoặc Spark. Nếu môi trường bị chặn Maven hoặc không muốn cài Java, tiếp tục dùng **full_load_python.py**.

## 8. Kiểm thử tự động

Các unit test không yêu cầu Oracle/Kafka:

~~~powershell
python -m pytest tests -q
~~~

Các nhóm kiểm thử chính:

- Generator: dữ liệu giả lập, khóa, tỷ lệ dữ liệu lỗi và golden cases.
- Reference: quy tắc chuẩn hóa ngày sinh, định danh, địa bàn, họ tên và cờ DQ.
- Load plan: chia khối, trạng thái từng khối và quy tắc resume.
- API: xác thực, phân quyền, tra cứu, batch, search, cursor và audit.
- Verify delta: đối chiếu kết quả với manifest kỳ vọng.

Kịch bản tích hợp cần chạy cùng Oracle và Kafka được mô tả trong [docs/TEST_PLAN.md](docs/TEST_PLAN.md).

## 9. Tài liệu thiết kế và vận hành

| Tài liệu | Nội dung |
|---|---|
| [Basic Design](docs/BASIC_DESIGN.md) | Mục tiêu, kiến trúc và nguyên tắc thiết kế |
| [Detail Design](docs/DETAIL_DESIGN.md) | Schema, quy tắc chuẩn hóa, Map DBHC, Queue/NRT, Full Load và CDC |
| [QA](docs/QA.md) | Khác biệt giữa mô tả nghiệp vụ và dữ liệu danh mục thực tế |
| [Review Response](docs/REVIEW_RESPONSE.md) | Các góp ý review và cách xử lý trong code |
| [Test Plan](docs/TEST_PLAN.md) | Kịch bản kiểm thử, ca golden và tiêu chí đạt |
| [BI Guide](docs/BI_GUIDE.md) | Nguồn dữ liệu và hướng dẫn kết nối công cụ BI |
| [Operations](docs/OPERATIONS.md) | Xử lý sự cố, giám sát và lưu ý hiệu năng |

## 10. Lưu ý và giới hạn

- Đây là môi trường lab sử dụng Oracle XE 21c và dữ liệu nhân khẩu giả lập; 3 bảng danh mục được nạp từ các CSV trong **data/dm/**. Không có dữ liệu giao dịch thật của hệ thống PROD trong repository.
- Mặc định seed 100.000 nhân khẩu để thuận tiện thử nghiệm. Quy mô 138 triệu dòng trong yêu cầu nghiệp vụ cần hạ tầng Oracle/Spark/Kafka phù hợp và phải benchmark riêng; không suy rộng hiệu năng từ môi trường XE local.
- Unit test không thay thế kiểm thử tích hợp. Các kịch bản Oracle, Kafka/Debezium, Spark và Airflow cần được chạy trên môi trường có đủ dịch vụ theo **docs/TEST_PLAN.md**.
- Full Load Mirror hỗ trợ resume theo khối. Chỉ dùng **--resume** để tiếp tục đúng kế hoạch nạp còn dang dở.
- **docker compose down** dừng và xóa container nhưng giữ volume dữ liệu. **Không dùng docker compose down -v** trừ khi muốn xóa toàn bộ dữ liệu Oracle/Kafka của môi trường lab.
- Đổi mật khẩu mặc định và API key trước khi chia sẻ môi trường; không đưa .env, JAR, dữ liệu sinh lớn hoặc file log cá nhân lên Git.

## 11. Hình ảnh minh họa

<img width="1900" height="946" alt="Screenshot 2026-11-13 145147" src="https://github.com/user-attachments/assets/abdde8ca-8577-49bf-8e26-60df02e35e1d" />
<img width="1919" height="644" alt="Screenshot 2026-11-13 145227" src="https://github.com/user-attachments/assets/7a5596ad-f394-46ab-9ab1-00f7dfb00d89" />
<img width="1918" height="939" alt="Screenshot 2026-11-13 145214" src="https://github.com/user-attachments/assets/8fca3b64-4850-47d6-a25d-f1fdba01fd0b" />
