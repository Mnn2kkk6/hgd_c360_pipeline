# Vận hành & xử lý sự cố

| Tình huống | Dấu hiệu | Xử lý |
|---|---|---|
| Queue ứ | `V_QUEUE_BACKLOG.SO_DONG_CHO` tăng; DAG `queue_health` đỏ | Kiểm tra `C360_SYNC_LOG` (ERROR/SKIPPED). Tăng `p_batch`, hoặc chạy `make nrt-once` nhiều lần. Kiểm tra `C360_SYNC_CONTROL.PAUSE_FLAG` |
| NRT luôn `SKIPPED` | `MESSAGE='PAUSE_FLAG=1'` | Full Load đang chạy; nếu đã dừng hẳn: `FULL_FAILED` → xử lý rồi `PRC_C360_RESUME` hoặc chạy lại `PRC_C360_FULL_LOAD` |
| NRT `ERROR` | `C360_SYNC_LOG.MESSAGE` có backtrace | Queue **không bị xóa** khi lỗi → sửa nguyên nhân (vd view, quyền) là tự bù ở lượt sau |
| Danh mục DBHC đổi | — | DAG maintenance làm mới Map mỗi 30 phút; làm ngay: `call_proc PRC_REFRESH_MAP_DBHC`. Công dân cũ **không** tự tính lại: nếu cần, `PRC_C360_FULL_LOAD` |
| Connector lỗi | `cdc_ops status` → FAILED | Xem `trace`; thường do thiếu ARCHIVELOG, quyền Debezium, hoặc bảng Mirror thiếu cột. Sau khi sửa: restart task qua REST `/connectors/<tên>/restart` |
| Sink chậm | Lag consumer trong Kafka UI | Tăng `batch.size`, `tasks.max` (cần topic nhiều partition); kiểm tra trigger/Queue |
| Docker/máy khởi động lại, Kafka không lên (`NodeExists` trong log) | `docker ps -a` thấy `hgd-kafka` Exited; Connect báo `No resolvable bootstrap urls` | ZooKeeper còn giữ đăng ký cũ của broker. Chờ ~30 giây rồi `docker start hgd-kafka`, sau đó `docker start hgd-connect`; source connector tự đọc tiếp từ vị trí đã lưu. Dữ liệu Kafka nằm ở volume nên không mất |
| Cần dựng lại Mirror | — | DAG `hgd_initial_load` (hoặc `make initial-load`); xóa connector cũ trước: `cdc_ops delete hgd-mirror-sink` |
| Nguồn có đợt biến động lớn | LogMiner trễ | Giảm tốc `delta.py --chunk/--sleep`; tăng `log.mining.batch.size.*`; theo dõi `V_QUEUE_BACKLOG` |

**Điều chỉnh hiệu năng khi tải 138 triệu dòng**: `FULLLOAD_PARTITIONS` 16–32 (tùy số core/CPU nguồn), tạo C360 với index `UNUSABLE` rồi rebuild sau Full Load, tăng `p_parallel` của `PRC_C360_FULL_LOAD`, đặt Mirror/C360 ở tablespace NOLOGGING khi nạp lại, kiểm tra kế hoạch thực thi của `V_C360_NORMALIZED` (cần đẩy predicate `MA_CD IN (…)` xuống lớp `valid`).

**Bảo mật**: đổi mật khẩu mặc định ở `sql/00_sys_users.sql` và `.env`; production tách instance nguồn/Mirror, user recon dùng kết nối riêng chỉ-đọc; đặt API sau TLS/gateway; xoay vòng API key.
