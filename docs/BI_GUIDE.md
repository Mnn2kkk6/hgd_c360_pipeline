# Kết nối BI

Dashboard dựng sẵn: `streamlit run hgd/bi/dashboard.py` (đọc Data Mart, cache 60 giây).

Nối Superset / Metabase / Power BI trực tiếp (user `C360`, service `XEPDB1`): ví dụ SQLAlchemy `oracle+oracledb://C360:<mật_khẩu>@<host>:1521/?service_name=XEPDB1`.
| Nhóm | Nguồn dữ liệu | Biểu đồ gợi ý |
|---|---|---|
| 1. Nhân khẩu & định danh | `DM_NHANKHAU_DIABAN`, `V_DM_DINH_DANH_TINH` | Cột theo tỉnh, tỷ lệ có định danh, tháp tuổi–giới |
| 2. Chất lượng dữ liệu | `DM_DQ_TONG`, `DM_DQ_TINH`, `DM_DQ_FLAG_TINH` | Cột theo loại cờ, heatmap cờ × tỉnh, tỷ lệ lỗi theo tỉnh |
| 3. Giám sát đồng bộ | `V_SYNC_MONITOR_PHUT`, `V_QUEUE_BACKLOG`, `C360_SYNC_LOG`, `C360_RECON_RESULT` | Đường theo phút, backlog, lỗi gần đây, lệch đối soát |
Data Mart làm mới bằng `PRC_REFRESH_DATAMART` (DAG maintenance, 30 phút).
