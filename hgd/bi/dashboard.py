"""Dashboard BI (Streamlit) đọc Data Mart trên C360.  Chạy:  streamlit run hgd/bi/dashboard.py
3 nhóm: (1) Nhân khẩu & định danh theo địa bàn  (2) Chất lượng dữ liệu  (3) Giám sát đồng bộ.
Các view/bảng DM_* / V_* cũng kết nối thẳng được từ Superset/Metabase/Power BI (xem docs/BI_GUIDE.md)."""
import sys
from pathlib import Path

# Streamlit chỉ thêm thư mục của file này vào sys.path -> thêm gốc project để import được package hgd
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.express as px
import streamlit as st

from hgd import db

st.set_page_config(page_title="HGD C360", layout="wide")


@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    conn = db.connect("c360")
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    df.columns = [c.lower() for c in df.columns]
    return df


st.title("Customer 360 – Hộ gia đình / Nhân khẩu")
t1, t2, t3 = st.tabs(["Nhân khẩu & định danh", "Chất lượng dữ liệu", "Giám sát đồng bộ"])

with t1:
    tinh = q("SELECT * FROM V_DM_DINH_DANH_TINH ORDER BY TONG DESC")
    c1, c2, c3 = st.columns(3)
    c1.metric("Tổng công dân", f"{int(tinh.tong.sum()):,}")
    c2.metric("Có số định danh", f"{int(tinh.co_dinh_danh.sum()):,}")
    c3.metric("Tỷ lệ có định danh", f"{100 * tinh.co_dinh_danh.sum() / max(tinh.tong.sum(), 1):.1f}%")
    st.plotly_chart(px.bar(tinh.head(20), x="ten_tinh", y="tong", title="Top 20 tỉnh/thành theo số công dân"), use_container_width=True)
    st.plotly_chart(px.bar(tinh.sort_values("ty_le_co_pct").head(20), x="ten_tinh", y="ty_le_co_pct", title="20 tỉnh có tỷ lệ định danh thấp nhất (%)"), use_container_width=True)
    age = q("SELECT NHOM_TUOI, GIOI_TINH, SUM(SO_CONG_DAN) AS SO FROM DM_NHANKHAU_DIABAN GROUP BY NHOM_TUOI, GIOI_TINH")
    st.plotly_chart(px.bar(age, x="nhom_tuoi", y="so", color="gioi_tinh", barmode="group", title="Cơ cấu tuổi – giới"), use_container_width=True)
    xa = q("SELECT TEN_TINH, TEN_XA, SUM(SO_CONG_DAN) AS SO FROM DM_NHANKHAU_DIABAN WHERE MA_XA IS NOT NULL GROUP BY TEN_TINH, TEN_XA ORDER BY SO DESC FETCH FIRST 30 ROWS ONLY")
    st.dataframe(xa, use_container_width=True)

with t2:
    tong = q("SELECT * FROM DM_DQ_TONG ORDER BY SO_BAN_GHI DESC")
    sach = tong[tong.flag_code == "SACH_HOAN_TOAN"]
    if len(sach):
        st.metric("Bản ghi sạch hoàn toàn", f"{100 * float(sach.ty_le.iloc[0]):.1f}%")
    st.plotly_chart(px.bar(tong[tong.flag_code != "SACH_HOAN_TOAN"], x="flag_code", y="so_ban_ghi", color="nhom", title="Số bản ghi theo loại cờ lỗi"), use_container_width=True)
    dq = q("SELECT * FROM DM_DQ_TINH WHERE TONG > 0 ORDER BY TY_LE_LOI DESC")
    st.plotly_chart(px.bar(dq.head(20), x="ten_tinh", y="ty_le_loi", title="20 tỉnh có tỷ lệ bản ghi lỗi cao nhất"), use_container_width=True)
    ft = q("SELECT FLAG_CODE, TEN_TINH, SO_BAN_GHI FROM DM_DQ_FLAG_TINH")
    if len(ft):
        st.plotly_chart(px.density_heatmap(ft, x="ten_tinh", y="flag_code", z="so_ban_ghi", histfunc="sum", title="Cờ lỗi theo tỉnh"), use_container_width=True)

with t3:
    bl = q("SELECT * FROM V_QUEUE_BACKLOG")
    last = q("SELECT MAX(START_TIME) AS LAST_OK FROM C360_SYNC_LOG WHERE JOB_TYPE='NRT' AND STATUS='OK'")
    c1, c2 = st.columns(2)
    c1.metric("Dòng đang chờ trong Queue", int(bl.so_dong_cho.iloc[0]))
    c2.metric("Lô NRT thành công gần nhất", str(last.last_ok.iloc[0]))
    m = q("SELECT * FROM V_SYNC_MONITOR_PHUT WHERE PHUT >= SYSDATE - 1/24 ORDER BY PHUT")
    if len(m):
        st.plotly_chart(px.line(m, x="phut", y=["so_khoa_bien_dong", "so_dong_merge", "so_dong_delete"], title="Khối lượng xử lý mỗi phút (1 giờ qua)"), use_container_width=True)
        st.plotly_chart(px.bar(m, x="phut", y="thoi_gian_xu_ly_giay", title="Thời gian xử lý (giây)"), use_container_width=True)
    err = q("SELECT START_TIME, JOB_TYPE, STATUS, MESSAGE FROM C360_SYNC_LOG WHERE STATUS = 'ERROR' ORDER BY START_TIME DESC FETCH FIRST 20 ROWS ONLY")
    st.subheader("Lỗi gần đây")
    st.dataframe(err, use_container_width=True)
    rc = q("SELECT RUN_TS, CHECK_NAME, SCOPE, EXPECTED, ACTUAL, DIFF, STATUS FROM C360_RECON_RESULT WHERE STATUS IN ('FAIL','PENDING') ORDER BY RUN_TS DESC FETCH FIRST 20 ROWS ONLY")
    st.subheader("Đối soát lệch gần đây")
    st.dataframe(rc, use_container_width=True)
