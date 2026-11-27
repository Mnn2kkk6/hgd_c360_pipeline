# Tuần 3 – Đối soát, Dashboard BI và REST API

Tài liệu này chốt phạm vi triển khai Tuần 3 theo `Yeucau_DataPipeline_HGD_C360_4Tuan.md`. Phạm vi gồm ba tầng Source → Mirror → C360, ba nhóm Dashboard BI và bốn API tra cứu.

## 1. Trạng thái triển khai trong repository

| Yêu cầu | Thành phần | Trạng thái code |
|---|---|---|
| Đối soát tổng số dòng Source ↔ Mirror cho 4 bảng | `hgd/recon/reconcile.py` | Đã triển khai |
| Đối soát theo mã tỉnh | `hgd/recon/reconcile.py` | Đã triển khai |
| Đối soát số công dân hợp lệ Mirror ↔ C360 | `hgd/recon/reconcile.py` | Đã triển khai |
| Kiểm tra C360 thiếu/thừa, trùng khóa | `hgd/recon/reconcile.py` | Đã triển khai |
| So sánh mẫu Oracle với logic tham chiếu Python | `hgd/recon/reconcile.py` | Đã triển khai |
| Thống kê bản ghi sạch và từng cờ DQ | `hgd/recon/reconcile.py` | Đã triển khai |
| Data Mart thống kê dân số/định danh | `sql/40_datamart_bi.sql` | Đã triển khai |
| Data Mart chất lượng dữ liệu | `sql/40_datamart_bi.sql` | Đã triển khai |
| View giám sát Queue, NRT và Sync Log | `sql/40_datamart_bi.sql`, `sql/41_mirror_lag_view.sql` | Đã triển khai |
| Dashboard 3 nhóm | `hgd/bi/dashboard.py` | Đã triển khai |
| API tra cứu đơn, batch, tìm kiếm, thay đổi | `hgd/api/app.py` | Đã triển khai theo đúng endpoint bên dưới |
| X-API-Key, SHA-256, scope | `hgd/api/app.py`, `hgd/api/repo.py`, `sql/20_c360_tables.sql` | Đã triển khai |
| Audit log bất đồng bộ theo lô | `hgd/api/audit.py`, `hgd/api/repo.py` | Đã triển khai; shutdown xả hết queue |
| Index hỗ trợ tra cứu API | `sql/20_c360_tables.sql` | Đã triển khai |

**Lưu ý nghiệm thu:** trạng thái “Đã triển khai” ở bảng trên xác nhận thành phần và logic đã có trong source code. Nó không thay thế kết quả chạy tích hợp trên Oracle/Kafka thật. Kết quả môi trường phải được ghi nhận sau khi chạy các bước ở mục 4.

## 2. REST API – endpoint chuẩn theo yêu cầu

Tất cả endpoint nghiệp vụ yêu cầu header `X-API-Key`. Tạo key bằng lệnh:

```powershell
python -m hgd.tools.create_api_key --name "Local test" --scopes lookup,batch,search,changes
```

Key thô chỉ hiển thị một lần; lưu an toàn và không commit key hoặc file `.env`.

| API | Endpoint | Scope | Quy tắc |
|---|---|---|---|
| Tra cứu cá nhân | `GET /api/v1/ca-nhan/{ma}` | `lookup` | Tự dò MA_SO_BHXH → MA_SO_BHXH_CU → SO_DINH_DANH; trả 404 nếu không thấy, 409 nếu mơ hồ |
| Tra cứu theo lô | `POST /api/v1/ca-nhan/tra-cuu-lo` | `batch` | Tối đa 1.000 mã; trả kết quả FOUND / CONFLICT / NOT_FOUND theo từng mã |
| Tìm kiếm tên + ngày sinh | `GET /api/v1/ca-nhan/tim-kiem` | `search` | Họ tên có dấu/không dấu; ngày sinh YYYY, YYYYMM hoặc YYYYMMDD |
| Lấy bản ghi thay đổi | `GET /api/v1/ca-nhan/thay-doi` | `changes` | Phân trang theo cursor (SYNC_DATE, ID); chỉ trả dữ liệu đã qua độ trễ an toàn |

Các URL cũ vẫn được giữ để không phá vỡ script tích hợp đã viết trước đó:

- `POST /api/v1/ca-nhan/batch` → alias của `/api/v1/ca-nhan/tra-cuu-lo`.
- `GET /api/v1/ca-nhan?ho_ten=...` → alias của `/api/v1/ca-nhan/tim-kiem`.
- `GET /api/v1/thay-doi` → alias của `/api/v1/ca-nhan/thay-doi`.

### Ví dụ gọi API trên PowerShell

Khởi động API:

```powershell
uvicorn hgd.api.app:app --host 127.0.0.1 --port 8000
```

Gán API key vào biến môi trường của phiên PowerShell hiện tại:

```powershell
$headers = @{ "X-API-Key" = "<API_KEY_DA_TAO>" }
```

Tra cứu một công dân:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/ca-nhan/0120456789" -Headers $headers
```

Tra cứu theo lô:

```powershell
$body = @{ ma = @("0120456789", "0133000001") } | ConvertTo-Json
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/ca-nhan/tra-cuu-lo" -Method Post -Headers $headers -ContentType "application/json" -Body $body
```

Tìm theo họ tên + ngày sinh:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/ca-nhan/tim-kiem?ho_ten=Nguyen%20Van%20An&ngay_sinh=199508" -Headers $headers
```

Lấy thay đổi:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/ca-nhan/thay-doi?limit=1000" -Headers $headers
```

Lần gọi tiếp theo truyền `next_cursor` nhận được ở lần trước vào query string `cursor`. Chỉ tăng cursor sau khi phía nhận đã xử lý thành công toàn bộ trang hiện tại.

Swagger UI: `http://127.0.0.1:8000/docs`.

## 3. Dashboard BI

Cài package theo `requirements.txt`, bảo đảm Data Mart đã được tạo và nạp dữ liệu, sau đó chạy:

```powershell
python -m hgd.tools.call_proc PRC_REFRESH_DATAMART
streamlit run hgd/bi/dashboard.py --server.port 8501
```

Dashboard có ba nhóm:

1. **Nhân khẩu & định danh:** tổng công dân, tỷ lệ có định danh, phân bổ tỉnh/xã, tuổi, giới tính và tình trạng sống.
2. **Data Quality:** số lượng và tỷ lệ theo từng cờ lỗi, tỷ lệ lỗi theo tỉnh và heatmap cờ lỗi × tỉnh.
3. **Sync Monitoring:** Queue backlog, thời điểm ghi nhận Mirror gần nhất, số khóa NRT, số dòng merge/delete, thời gian xử lý, lô lỗi và kết quả đối soát.

Các bảng/view Data Mart dành cho công cụ BI ngoài:

- `DM_NHANKHAU_DIABAN`, `V_DM_DINH_DANH_TINH`
- `DM_DQ_TONG`, `DM_DQ_TINH`, `DM_DQ_FLAG_TINH`
- `V_SYNC_MONITOR`, `V_SYNC_MONITOR_PHUT`, `V_QUEUE_BACKLOG`
- `C360_SYNC_LOG`, `C360_RECON_RESULT`

Data Mart được làm mới bởi `PRC_REFRESH_DATAMART`; DAG maintenance hiện lập lịch 30 phút/lần. Khi chạy thủ công, refresh lại Data Mart sau khi có dữ liệu C360 mới.

## 4. Checklist chạy nghiệm thu Tuần 3

Chạy ở thư mục gốc repo, trong virtual environment đã cài `requirements.txt` và đã cấu hình `.env`.

### 4.1. Kiểm thử tự động không cần Oracle

```powershell
python -m pytest tests/test_api.py -q
python -m pytest tests -q
```

API tests bao phủ xác thực/scope, thứ tự dò mã, 404/409, batch 1.000 phần tử, tìm theo ngày sinh rút gọn, cursor/độ trễ an toàn, các endpoint theo đặc tả và việc xả nhiều batch audit khi shutdown.

### 4.2. Kiểm thử tích hợp cần Oracle

Đảm bảo đã chạy `python -m hgd.tools.init_db`, seed dữ liệu, Full Load Mirror, Full Load C360 và bật CDC/NRT theo README trước khi chạy:

```powershell
python -m hgd.recon.reconcile --sample 1000
python -m hgd.tools.call_proc PRC_REFRESH_DATAMART
```

Job đối soát trả mã thoát 1 khi có check FAIL. Kiểm tra bản ghi lịch sử:

```sql
SELECT RUN_ID, RUN_TS, CHECK_NAME, SCOPE, EXPECTED, ACTUAL, DIFF, STATUS, NOTE
FROM C360_RECON_RESULT
ORDER BY RUN_TS DESC, ID DESC;
```

Đối soát trực tiếp số công dân hợp lệ:

```sql
SELECT
  (SELECT COUNT(DISTINCT m.MA_CD)
     FROM MIRROR.VSS_HO_GD_NHANKHAU m
    WHERE NVL(m.TYPEID, 'ZZ') NOT IN ('GT','GM','GG','GK')
      AND NVL(m.TRANG_THAI_DUYET, 0) NOT IN (1,3,4)) AS MIRROR_HOP_LE,
  (SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN) AS C360_TONG,
  (SELECT COUNT(DISTINCT m.MA_CD)
     FROM MIRROR.VSS_HO_GD_NHANKHAU m
    WHERE NVL(m.TYPEID, 'ZZ') NOT IN ('GT','GM','GG','GK')
      AND NVL(m.TRANG_THAI_DUYET, 0) NOT IN (1,3,4))
  - (SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN) AS CHENH_LECH
FROM DUAL;
```

Tiêu chí đạt: `CHENH_LECH = 0`, không có công dân C360 thiếu/thừa hoặc trùng mã, và các mẫu kiểm tra chuẩn hóa không có sai khác. Nếu Queue còn backlog, trạng thái đối soát có thể là PENDING; xử lý hết backlog và chạy lại.

### 4.3. Kiểm thử Dashboard và API

- Mở `http://localhost:8501`; kiểm tra đủ 3 tab và dữ liệu sau refresh.
- Mở `http://127.0.0.1:8000/docs`; gọi đủ 4 endpoint với API key hợp lệ.
- Thử key thiếu scope để xác nhận 403; key không hợp lệ để xác nhận 401.
- Thử mã không tồn tại để xác nhận 404; mã cũ/định danh trùng để xác nhận 409 và danh sách ứng viên.
- Sau khi gọi API, kiểm tra `C360_API_AUDIT_LOG` có endpoint, HTTP status, thời gian xử lý và số kết quả.

```sql
SELECT LOG_TIME, KEY_NAME, ENDPOINT, HTTP_STATUS, RESULT_COUNT, DURATION_MS
FROM C360_API_AUDIT_LOG
ORDER BY LOG_TIME DESC
FETCH FIRST 50 ROWS ONLY;
```

## 5. Kết luận và giới hạn nghiệm thu

Phần code Tuần 3 đã có các thành phần chính, đồng thời API đã được bổ sung route đúng hợp đồng, test hồi quy cho route tĩnh và cơ chế flush audit nhiều batch. Dashboard đã bổ sung tình trạng sống/chết, hiển thị phần trăm DQ và số lô NRT lỗi.

**Chưa ghi nhận kết quả chạy tích hợp trong tài liệu này.** Trước khi báo cáo “đã nghiệm thu”, cần chạy các lệnh mục 4 trong môi trường Oracle/Kafka của bạn, lưu output đối soát, ảnh 3 tab Dashboard và kết quả gọi 4 API. Không suy diễn hiệu năng cho quy mô PROD 138 triệu dòng từ dữ liệu lab.
