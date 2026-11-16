-- Chạy bằng user MIRROR, SAU Full Load Mirror và SAU Full Load C360 (xem docs/DETAIL_DESIGN.md §5):
-- bật trigger sau khi C360 đã dựng xong nên Queue không bị đẩy dữ liệu thừa trong lúc nạp nền.
-- BEFORE ROW: (1) gán SYNC_TS = thời điểm đồng bộ; (2) chèn khóa MA_CD vào Queue cùng transaction (nguyên tử).
-- Khi UPDATE đổi MA_CD: đẩy CẢ mã mới LẪN mã cũ để C360 tạo mã mới và xóa sạch mã cũ.
CREATE OR REPLACE TRIGGER TRG_HGD_NK_QUEUE
BEFORE INSERT OR UPDATE OR DELETE ON VSS_HO_GD_NHANKHAU
FOR EACH ROW
BEGIN
  IF INSERTING THEN
    :NEW.SYNC_TS := SYSTIMESTAMP;
    INSERT INTO C360.C360_QUEUE (MA_CD, ACTION) VALUES (:NEW.MA_CD, 'I');
  ELSIF DELETING THEN
    INSERT INTO C360.C360_QUEUE (MA_CD, ACTION) VALUES (:OLD.MA_CD, 'D');
  ELSE
    :NEW.SYNC_TS := SYSTIMESTAMP;
    INSERT INTO C360.C360_QUEUE (MA_CD, ACTION) VALUES (:NEW.MA_CD, 'U');
    IF :OLD.MA_CD <> :NEW.MA_CD THEN
      INSERT INTO C360.C360_QUEUE (MA_CD, ACTION) VALUES (:OLD.MA_CD, 'U');
    END IF;
  END IF;
END;
/
