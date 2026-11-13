# Kế hoạch kiểm thử

## A. Test tự động không cần Oracle (`make test` → `pytest tests`)
| File | Nội dung |
|---|---|
| `test_reference.py` | Quy tắc ngày sinh, điện thoại, khóa tỉnh/xã, định danh, bỏ dấu; **bảng TRANSLATE trong SQL tương đương Python**; Map HIEN_HANH/SAP_NHAP/MO_HO + ưu tiên; cờ địa chỉ; lọc rồi khử trùng |
| `test_generator.py` | 36 tỉnh/3.322 xã khớp file; MO_HO cài đúng; khóa `(MA_CD, MA_HO_GD)` duy nhất; **mọi loại cờ DQ đều xuất hiện**; tỷ lệ dòng bị lọc 1,5–4,5%; ≥ 400 công dân nhiều hộ; 10 ca "golden" |
| `test_api.py` | Xác thực/phân quyền, 3 tầng tra cứu, 409 kèm ứng viên, batch, tìm theo ngày sinh rút gọn, con trỏ + lag, audit |
| `test_verify_delta.py` | Luật so khớp manifest |

## B. Kiểm thử tích hợp (cần Oracle + Kafka)
Chạy theo thứ tự; mỗi bước có tiêu chí đạt.

| # | Kịch bản | Cách chạy | Tiêu chí đạt |
|---|---|---|---|
| T1 | Dựng DB | `make db-init` | Không lỗi; `USER_OBJECTS` không có đối tượng INVALID (`SELECT object_name FROM user_objects WHERE status='INVALID'`) |
| T2 | Nạp nguồn | `make seed` | ~104k dòng nhân khẩu + 59/243/15.325 dòng danh mục |
| T3 | Full Load Mirror | `initial-load` các bước Spark | Số dòng nguồn = Mirror cho 4 bảng; unique index tạo được (không trùng khóa) |
| T4 | Full Load lặp lại | Chạy lại Spark | Số dòng **không đổi** (không nhân đôi) |
| T5 | Map | `PRC_REFRESH_MAP_DBHC` | `HIEN_HANH`=36 tỉnh + 3.322 xã; có dòng `SAP_NHAP` và `MO_HO` (6 mã giả) |
| T6 | Full Load C360 | `PRC_C360_FULL_LOAD` | Số dòng = số `MA_CD` hợp lệ khác nhau ở Mirror; ca golden đúng (xem dưới) |
| T7 | Tính tương đương | `hgd.recon.reconcile --sample 1000` | `SAMPLE_REFERENCE` PASS (Oracle = bản tham chiếu Python) |
| T8 | NRT | `make delta` (chờ 1–2 phút) rồi `make verify` | Tất cả kịch bản PASS; `C360_QUEUE` về 0 |
| T9 | Đua giữa NRT và Full Load | Chạy `delta` trong lúc gọi `PRC_C360_FULL_LOAD` | Không lỗi; sau Full Load + 2 phút `verify` PASS, recon PASS |
| T10 | Phục hồi sink | `cdc_ops sink-pause`, chạy delta, `sink-resume` | Mirror bắt kịp; recon PASS |
| T11 | Lỗi NRT | Cố ý làm view lỗi → `C360_SYNC_LOG.STATUS='ERROR'`, **Queue không bị xóa** | Sửa view → lô sau xử lý bù |
| T12 | API | `uvicorn …`, tạo key, gọi 4 endpoint | Khớp mô tả; 409 với mã cũ trùng; audit ghi vào `C360_API_AUDIT_LOG` |
| T13 | BI | `make bi` sau `PRC_REFRESH_DATAMART` | 3 tab hiển thị số liệu |

### Ca golden (tạo bởi `seed`, kỳ vọng kiểm bằng `test_generator.py::test_golden_expectations`)
| MA_CD | Tình huống | Kỳ vọng ở C360 |
|---|---|---|
| `0120456789` | 3 hộ: A cũ, B mới nhất, C bị loại `TYPEID=GT` | `MA_HO_GD=GOLD_HO_B`, họ tên đã gọn khoảng trắng, có định danh `001095012345` |
| `0133000001` | Dữ liệu sạch | `DQ_FLAGS` NULL, `NGAY_SINH=19950815` |
| `0133000002` / `…03` | `TYPEID=GG` / `TRANG_THAI_DUYET=1` | Không có trong C360 |
| `0133000004` | Tỉnh `79TTT` nhưng xã thuộc Hà Nội | Cờ `TINH_LECH_XA` |
| `0133000005` | Ngày sinh `19950230` | `NGAY_SINH` NULL + `NGAY_SINH_LOI` |
| `0133000006` | SĐT `0912 345 678`, quốc tịch `233`, dân tộc `9999` | SĐT `0912345678`, `QUOC_TICH_MA=VN`, cờ `DAN_TOC_KHONG_MAP` |
| `0133000007` | Xã `4`, tỉnh `1`, SĐT `84912345678`, chỉ có CMND 9 số | `MA_XA=00004`, SĐT chuẩn, cờ `KHONG_SO_DINH_DANH` |

## C. Kịch bản biến động của `delta.py` và kỳ vọng
`insert` (mới hợp lệ có; mới bị lọc không có; người cũ sang hộ mới → `MA_HO_GD` mới) · `add_cccd` (có định danh, hết cờ) · `fix_xa` (xã/tỉnh đúng, hết cờ) · `fix_ngay_sinh` · `approve` (xuất hiện) · `reject` (`TRANG_THAI_DUYET=4` hoặc `TYPEID=GG` → biến mất) · `change_ma` (mã mới có `MA_SO_BHXH_CU`, mã cũ biến mất) · `delete_latest` (nhiều hộ: quay về hộ trước đó) · `delete_all` (biến mất). Các kịch bản dùng tập công dân tách biệt (`used`) để kỳ vọng không chồng chéo.

## D. Điều chưa kiểm chứng trong môi trường phát triển
Mã Oracle (SQL/PL/SQL), Spark, Debezium, Airflow, dashboard được viết và rà soát cú pháp nhưng **chưa chạy với Oracle/Kafka thật** khi đóng gói; mục B là phần cần chạy để nghiệm thu.
