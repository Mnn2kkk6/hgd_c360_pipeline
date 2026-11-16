# Phản hồi góp ý của mentor

| # | Góp ý | Nhận định | Đã làm | File | Kiểm chứng |
|---|---|---|---|---|---|
| 1 | `raise SystemExit` khi lệch số dòng làm gãy chuỗi lệnh trước khi tạo index/bật sink | **Đúng.** Nguồn còn giao dịch lúc nạp; CDC đã bật trước nên lệch là bình thường và sẽ được bù | Lệch → cảnh báo, chuỗi tiếp tục. Đầy đủ dữ liệu do trạng thái từng khối bảo đảm. `--strict` tùy chọn | `hgd/sync/loadplan.py` (`count_verdict`), `full_load_python.py`, `full_load_spark.py` | `tests/test_loadplan.py::test_count_verdict_does_not_abort_on_live_source` |
| 2a | `ORA_HASH` buộc quét toàn bảng N lần | **Đúng** | Chia khối theo khoảng `MA_CD` từ mẫu `SAMPLE`; mỗi dòng nguồn đọc 1 lần (Spark dùng `predicates`) | `loadplan.py`, `full_load_*.py` | `test_split_points_*`, `test_ranges_cover_everything...`. Chưa đo trên bảng trăm triệu dòng; lưu ý clustering factor, xem DETAIL_DESIGN §5 |
| 2b | Luôn `TRUNCATE`, sự cố mất hết phần đã nạp | **Đúng** | Sổ `MIRROR_LOAD_CHUNK`; `--resume` chỉ nạp khối chưa `DONE`, dọn phần ghi dở của khối `RUNNING/FAILED` | `sql/23_mirror_sync_ts.sql`, `loadplan.py`, `full_load_*.py` | `test_cleanup_rule`; kịch bản T14 (`--stop-after` rồi `--resume`) |
| 3 | Mirror thiếu thời điểm đồng bộ và index `MODIFY_DATE` | **Đúng** | Cột `SYNC_TS`, index `MODIFY_DATE` + `SYNC_TS` (tạo sau Full Load), view `V_MIRROR_SYNC_LAG`, hiển thị ở `reconcile` và dashboard | `sql/22`, `23`, `41`, `mirror_ops.py`, `reconcile.py`, `dashboard.py` | T16 |
| 4 | Trigger + sink bật trước Full Load C360 làm Queue chứa dữ liệu thừa | **Đúng một phần.** Dữ liệu không sai: `PRC_C360_FULL_LOAD` chụp snapshot Queue và chỉ xóa đúng các ID đó, các thay đổi phát sinh trong lúc nạp được NRT xử lý sau. Nhưng thứ tự cũ vẫn làm Queue phình và phức tạp không cần thiết | Đổi thứ tự: Full Load C360 → trigger → sink. Snapshot Queue vẫn giữ để chạy lại an toàn khi đã vận hành | `Makefile`, `airflow/dags/hgd_c360_dags.py`, `docs/DETAIL_DESIGN.md` §5 | T6, T8, T9 |

Chưa kiểm chứng ở quy mô lớn: hiệu năng đọc theo khoảng khóa trên bảng trăm triệu dòng, và `full_load_spark.py` (chưa chạy với Spark thật).
