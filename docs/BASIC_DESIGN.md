# Thiết kế cơ bản – Pipeline HGD → Customer 360

## 1. Mục tiêu
Đồng bộ dữ liệu Hộ gia đình – Nhân khẩu (~138 triệu dòng) từ DB nguồn sang vùng Mirror, chuẩn hóa thành bảng **C360** (mỗi công dân đúng 1 dòng), cập nhật gần thời gian thực (≤ 1–2 phút), có đối soát, dashboard BI và REST API.

## 2. Kiến trúc

```
 ┌───────────┐ LogMiner  ┌───────┐  JDBC sink   ┌─────────────── MIRROR ──────────────┐
 │ VSS nguồn │──Debezium─▶ Kafka ─▶ (upsert/del) ▶ VSS_HO_GD_NHANKHAU + 3 danh mục     │
 └─────┬─────┘           └───────┘               └────────┬─────────────────────────────┘
       │ Spark Full Load (ORA_HASH partition) ───────────▶ │ trigger: INSERT MA_CD → C360_QUEUE
       │                                                   ▼
       │                           ┌──────────────── C360 ─────────────────────────────┐
       │                           │ PRC_C360_NRT (mỗi phút)  PRC_C360_FULL_LOAD        │
       │                           │   └─ V_C360_NORMALIZED  (1 view = 1 nguồn logic)   │
       │                           │ C360_THONG_TIN_CA_NHAN ◀─ Map DBHC / danh mục      │
       │                           │ Data Mart DM_* · Recon · Sync log · API key/audit  │
       │                           └──────┬───────────────┬──────────────┬─────────────┘
       │                                  ▼               ▼              ▼
       └─ delta.py (biến động)        Dashboard BI     REST API      Đối soát
                         Airflow điều phối: initial_load · nrt (1 phút) · maintenance · recon_daily
```

## 3. Thành phần
| Thành phần | Công nghệ | Vai trò |
|---|---|---|
| Nguồn / Mirror / C360 | Oracle (3 schema `VSS`, `MIRROR`, `C360`) | Dữ liệu, chuẩn hóa, điều khiển |
| Full Load | PySpark JDBC, chia 8 phân vùng theo `ORA_HASH(MA_CD)` | Nạp ban đầu/nạp lại, an toàn chạy lại (TRUNCATE rồi nạp) |
| CDC | Debezium Oracle (LogMiner) → Kafka → Debezium JDBC Sink | Đẩy thay đổi sang Mirror theo khóa `(MA_CD, MA_HO_GD)` |
| Chuẩn hóa | PL/SQL package + view + procedure | Một nguồn logic cho Full Load và NRT |
| Điều phối | Airflow (hoặc DBMS_SCHEDULER) | Lịch chạy, cảnh báo |
| API | FastAPI + python-oracledb pool | 4 endpoint chỉ đọc, xác thực API key, audit log bất đồng bộ |
| BI | Streamlit + Data Mart (cũng nối được Superset/Metabase/Power BI) | 3 nhóm dashboard |

## 4. Nguyên tắc thiết kế
1. **Một nguồn logic duy nhất**: Full Load và NRT cùng đọc `V_C360_NORMALIZED`; sửa quy tắc một chỗ, cả hai đồng bộ.
2. **Mirror nguyên trạng, C360 mới chuẩn hóa**: Mirror không biến đổi, nên đối soát nguồn↔Mirror là đếm thuần túy.
3. **Lũy đẳng**: upsert theo khóa ở sink, MERGE theo `MA_SO_BHXH` ở NRT; chạy lại/phát lại không sinh dữ liệu sai.
4. **Không mất thay đổi khi Full Load**: bật CDC trước, nạp sau, sink bật sau cùng (Kafka giữ 7 ngày).
5. **Queue chỉ chứa khóa**: trigger rất nhẹ; dữ liệu thật đọc từ Mirror lúc xử lý (luôn mới nhất).
6. **Cờ chất lượng thay vì loại bỏ**: dữ liệu bẩn vẫn vào C360, kèm `DQ_FLAGS` để đo và sửa dần.

## 5. Luồng chính
**Dựng lần đầu**: tạo DB → seed nguồn → *source connector* (chỉ lấy schema) → Spark Full Load Mirror → tạo Unique Index → bật trigger → *sink connector* → làm mới Map → Full Load C360 → Data Mart → đối soát.
**Vận hành**: nguồn đổi → Debezium → Kafka → Mirror (trigger đẩy `MA_CD` vào Queue) → NRT mỗi phút MERGE/DELETE C360.

## 6. Phi chức năng
- Hiệu năng: Full Load song song theo `ORA_HASH`; NRT xử lý theo lô 20.000 dòng Queue, tối đa ~50 giây/lượt; chỉ mục phục vụ API (mã, mã cũ, định danh, tên không dấu + ngày sinh, con trỏ đồng bộ).
- An toàn: mật khẩu trong `.env`; API key chỉ lưu SHA-256; user C360 chỉ đọc nguồn trong lab.
- Quan sát: `C360_SYNC_LOG` (mỗi lô), `V_SYNC_MONITOR*`, `V_QUEUE_BACKLOG`, `C360_RECON_RESULT`, dashboard nhóm 3, cảnh báo Queue ứ trong DAG maintenance.
