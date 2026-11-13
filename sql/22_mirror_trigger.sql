-- Chạy bằng user MIRROR, SAU Full Load Mirror (tránh Full Load đẩy hàng chục triệu dòng vào Queue).
-- Trigger RẤT NHẸ: chỉ INSERT khóa MA_CD vào Queue (cùng transaction với dòng Mirror => nguyên tử).
-- Khi UPDATE đổi MA_CD: đẩy CẢ mã mới LẪN mã cũ để C360 tạo mã mới và xóa sạch mã cũ.
CREATE OR REPLACE TRIGGER TRG_HGD_NK_QUEUE
AFTER INSERT OR UPDATE OR DELETE ON VSS_HO_GD_NHANKHAU
FOR EACH ROW
BEGIN
  IF INSERTING THEN
    INSERT INTO C360.C360_QUEUE (MA_CD, ACTION) VALUES (:NEW.MA_CD, 'I');
  ELSIF DELETING THEN
    INSERT INTO C360.C360_QUEUE (MA_CD, ACTION) VALUES (:OLD.MA_CD, 'D');
  ELSE
    INSERT INTO C360.C360_QUEUE (MA_CD, ACTION) VALUES (:NEW.MA_CD, 'U');
    IF :OLD.MA_CD <> :NEW.MA_CD THEN
      INSERT INTO C360.C360_QUEUE (MA_CD, ACTION) VALUES (:OLD.MA_CD, 'U');
    END IF;
  END IF;
END;
/
