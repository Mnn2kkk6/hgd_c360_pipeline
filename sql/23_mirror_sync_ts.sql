-- Chạy bằng user MIRROR (sau sql/21). Chạy lại nhiều lần an toàn (db.py bỏ qua lỗi "đã tồn tại").
-- 1) Thời điểm bản ghi được đồng bộ về Mirror: điền mặc định khi INSERT (Full Load/CDC) và cập nhật bởi trigger khi UPDATE.
--    Dùng để đo độ trễ đồng bộ (xem view C360.V_MIRROR_SYNC_LAG) và truy vấn đối soát theo khoảng thời gian.
ALTER TABLE VSS_HO_GD_NHANKHAU ADD (SYNC_TS TIMESTAMP DEFAULT SYSTIMESTAMP)
/
-- 2) Sổ tiến độ Full Load theo khối (dùng cho --resume; loader cũng tự tạo nếu thiếu)
CREATE TABLE MIRROR_LOAD_CHUNK (
    TABLE_NAME   VARCHAR2(60)  NOT NULL,
    CHUNK_ID     NUMBER(6)     NOT NULL,
    LO_KEY       VARCHAR2(60),
    HI_KEY       VARCHAR2(60),
    STATUS       VARCHAR2(10)  DEFAULT 'PENDING' NOT NULL,
    ROWS_LOADED  NUMBER,
    STARTED_AT   TIMESTAMP,
    FINISHED_AT  TIMESTAMP,
    ERROR_MSG    VARCHAR2(500),
    CONSTRAINT PK_MIRROR_LOAD_CHUNK PRIMARY KEY (TABLE_NAME, CHUNK_ID)
)
/
-- Các index (UX_M_HGD_NK_MACD_HOGD, IX_M_HGD_NK_MODIFY, IX_M_HGD_NK_SYNC) KHÔNG tạo ở đây: tạo SAU Full Load
-- bằng `python -m hgd.sync.mirror_ops create-index` để nạp direct-path nhanh.
