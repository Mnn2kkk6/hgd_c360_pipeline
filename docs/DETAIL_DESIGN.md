# Thiết kế chi tiết

## 1. Đối tượng cơ sở dữ liệu
| Schema | Đối tượng | File |
|---|---|---|
| VSS (nguồn) | `VSS_HO_GD_NHANKHAU` (111 cột, partition `MA_TINH`), `VSS_DM_DANTOC`, `VSS_DM_QUOC_TICH`, `VSS_DM_DBHC`; unique index `(MA_CD, MA_HO_GD)`; supplemental logging | `sql/10_vss_source.sql` |
| MIRROR | 4 bảng phẳng cùng tên; unique index tạo **sau** Full Load; trigger `TRG_HGD_NK_QUEUE` | `sql/21`, `sql/22` |
| C360 | `C360_THONG_TIN_CA_NHAN`, `C360_MAP_DBHC_TINH/XA`, `C360_QUEUE`, `C360_QUEUE_SNAP`, 2 GTT, `C360_SYNC_CONTROL`, `C360_SYNC_LOG`, `C360_RECON_RESULT`, `C360_DQ_FLAG_DEF`, `C360_API_KEY`, `C360_API_AUDIT_LOG` | `sql/20` |
| C360 | `PKG_C360_UTIL`, view `C360_V_DBHC_*`, `PRC_REFRESH_MAP_DBHC`, `V_C360_NORMALIZED`, `PRC_C360_NRT`, `PRC_C360_FULL_LOAD`, `PRC_C360_RESUME`, Data Mart | `sql/30–34`, `sql/40` |

## 2. Quy tắc chuẩn hóa (trong `V_C360_NORMALIZED`)
1. **Lọc**: bỏ dòng có `TYPEID ∈ {GT,GM,GG,GK}` hoặc `TRANG_THAI_DUYET ∈ {1,3,4}` (NULL = hợp lệ).
2. **Khử trùng**: mỗi `MA_CD` giữ dòng hợp lệ có `MODIFY_DATE` mới nhất (đồng hạng: `CREATED_DATE`, rồi `ID`), thực hiện **sau** lọc.
3. **Họ tên**: gộp khoảng trắng; `HO_TEN_KHONG_DAU` = `UPPER(TRANSLATE(...))` (bảng 336 ký tự sinh từ Unicode, kiểm tra tương đương bản Python trong test).
4. **Ngày sinh**: số → `YYYYMMDD`/`YYYYMM`/`YYYY`; `YYYY0000`, `YYYYMM00` rút gọn; sai lịch/ngoài [1900, nay] → NULL + `NGAY_SINH_LOI`.
5. **Điện thoại**: bỏ ký tự phi số, xử lý `84`/`840`/9 số, hợp lệ `^0[0-9]{9,10}$`, nếu không → NULL + `SDT_LOI` (khi có nhập).
6. **Định danh**: `SO_CCCD` 12 số, nếu không thì `SO_CMTND` 12 số, nếu không NULL + `KHONG_SO_DINH_DANH`.
7. **Quốc tịch**: giá trị toàn số → tra `ID`, ngược lại tra `MA`; không map → `QUOC_TICH_KHONG_MAP` (khi có nhập). **Dân tộc**: join `DM_DAN_TOC_ID` → `ID`; không map → `DAN_TOC_KHONG_MAP`.
8. **Địa bàn** (3 nhóm: thường trú, liên hệ `_LH`, khai sinh `_KS`): khóa xã = `LPAD(5)`, khóa tỉnh = bỏ `TTT` + `LPAD(3)`; tra 2 bảng Map.
   - Tỉnh cuối = tỉnh của xã nếu có, ngược lại tỉnh khai báo đã map.
   - `NGUON_MAP_XA` ∈ HIEN_HANH / SAP_NHAP / MO_HO. MO_HO: `MA_XA` = NULL (không đoán), giữ `MA_TINH` nếu các xã mới cùng tỉnh.
   - Cờ: `TINH_KHONG_MAP`, `XA_KHONG_MAP`, `XA_MO_HO`, `TINH_LECH_XA` (+ hậu tố `_LH`/`_KS`).
9. **DQ_FLAGS**: các mã cách nhau `;`; bản ghi sạch hoàn toàn = NULL.

## 3. Bảng Map địa bàn
- Nguồn: `MIRROR.VSS_DM_DBHC` lọc đang hiệu lực. `MA_DBHC_SN` (`'00013;00004'`) tách bằng `REGEXP_SUBSTR` + `CONNECT BY` **một lần** khi làm mới Map (không tách lúc join).
- Ưu tiên: HIEN_HANH > SAP_NHAP (mã cũ → đúng 1 đơn vị mới) > MO_HO (≥ 2 đơn vị mới).
- Làm mới: `DELETE` + `INSERT` + `COMMIT` trong **một transaction** (không TRUNCATE – DDL tự commit và làm Map rỗng giữa chừng; NRT song song vẫn đọc snapshot cũ cho tới COMMIT).

## 4. Queue, NRT, Full Load
**Trigger** (`AFTER I/U/D FOR EACH ROW`) chỉ chèn khóa `MA_CD` vào `C360_QUEUE`; khi UPDATE đổi `MA_CD` chèn cả mã mới lẫn mã cũ.

**Cắt lô an toàn (`v_cut`)** – ID identity có thể commit không theo thứ tự nên *không* xóa bằng `ID <= MAX`. NRT chốt đúng tập ID đã thấy vào `C360_TMP_IDS`, xử lý, rồi xóa **chỉ** các ID đó. Dòng commit muộn (kể cả ID nhỏ hơn) không thuộc tập → giữ cho lô sau. Mỗi lô: `INSERT TMP_IDS` → `TMP_KEYS` → `MERGE` từ view → `DELETE` công dân không còn dòng hợp lệ → xóa Queue → `COMMIT`.

**Khóa điều phối**: `SELECT … FROM C360_SYNC_CONTROL FOR UPDATE SKIP LOCKED`. NRT không lấy được khóa (đang Full Load hoặc phiên NRT khác) → ghi `SKIPPED` và thoát. `PAUSE_FLAG=1` khi Full Load chạy.

**Full Load C360**: chờ lô NRT hiện tại → `PAUSE_FLAG=1` → chụp snapshot ID Queue (`C360_QUEUE_SNAP`) → `TRUNCATE` + `INSERT /*+ APPEND PARALLEL */ … SELECT` từ view → xóa đúng các ID thuộc snapshot → nhả cờ. Thay đổi phát sinh trong lúc Full Load nằm ngoài snapshot nên được NRT xử lý ngay sau đó. Lỗi → giữ `PAUSE_FLAG=1`, trạng thái `FULL_FAILED`; sau khi xử lý gọi `PRC_C360_FULL_LOAD` lại hoặc `PRC_C360_RESUME`.

## 5. Full Load Mirror & CDC
1. `mirror_ops prepare`: tắt trigger, bỏ unique index.
2. `cdc_ops source-create`: Debezium Oracle (`snapshot.mode=no_data`, `message.key.columns` cho cả 4 bảng).
3. `full_load_spark`: `TRUNCATE` Mirror → đọc Oracle theo `ORA_HASH(MA_CD)` thành 8 phân vùng → ghi JDBC batch → đối chiếu số dòng.
4. `mirror_ops create-index` (lỗi `ORA-01452` nếu trùng khóa) → `create-trigger`.
5. `cdc_ops sink-create`: Debezium JDBC Sink, `upsert` + `delete.enabled`, khóa = `record_key`.

Vì sao hội tụ đúng: Kafka đã nhận mọi thay đổi từ trước Full Load; sau Full Load sink phát lại theo thứ tự, mỗi thay đổi là upsert/delete theo khóa nên trạng thái cuối bằng trạng thái nguồn mới nhất (chỉ có thể tạm thời "cũ hơn" trong lúc phát lại).

## 6. API
| Endpoint | Scope | Ghi chú |
|---|---|---|
| `GET /api/v1/ca-nhan/{ma}?loai=` | lookup | Tầng: `MA_SO_BHXH` → `MA_SO_BHXH_CU` → `SO_DINH_DANH`. 200 / 404 / **409** (≥ 2 khớp, trả danh sách ứng viên) |
| `POST /api/v1/ca-nhan/batch` | batch | ≤ 1.000 mã; mỗi mã FOUND / NOT_FOUND / CONFLICT; truy vấn gộp theo tầng (IN ≤ 900) |
| `GET /api/v1/ca-nhan?ho_ten=&ngay_sinh=&kieu=` | search | Họ tên không dấu; ngày sinh `YYYY`/`YYYYMM`/`YYYYMMDD` khớp cả bản ghi chỉ có năm/tháng |
| `GET /api/v1/thay-doi?cursor=&limit=` | changes | Con trỏ `(SYNC_DATE, ID)`, chỉ lấy bản ghi đã "yên" ≥ `API_SYNC_LAG_SECONDS` để không sót bản ghi commit muộn |

Audit log ghi bất đồng bộ theo lô (thread riêng, hàng đợi giới hạn).

## 7. Data Mart & BI
`PRC_REFRESH_DATAMART` dựng `DM_NHANKHAU_DIABAN`, `DM_DQ_FLAG_TINH`, `DM_DQ_TINH`, `DM_DQ_TONG`; view `V_DM_DINH_DANH_TINH`, `V_SYNC_MONITOR`, `V_SYNC_MONITOR_PHUT`, `V_QUEUE_BACKLOG`. Dashboard: nhóm 1 (nhân khẩu/định danh theo địa bàn), nhóm 2 (chất lượng dữ liệu), nhóm 3 (giám sát đồng bộ).

## 8. Đối soát (`hgd.recon.reconcile`)
Nguồn↔Mirror (tổng + theo mã tỉnh thô), Mirror↔C360 (đếm, thừa, thiếu, trùng khóa, backlog Queue), **kiểm mẫu** so Oracle với bản tham chiếu Python (ưu tiên công dân nhiều hộ), thống kê cờ DQ. Lệch khi Queue còn dòng → `PENDING` (chưa kết luận); lệch khi Queue rỗng → `FAIL`.
