-- Chạy bằng user C360. Package hàm chuẩn hóa dùng chung (DETERMINISTIC) - MỘT NGUỒN LOGIC DUY NHẤT cho Full Load và Near Real-time.
CREATE OR REPLACE PACKAGE PKG_C360_UTIL AS
  FUNCTION norm_tinh_key (p IN VARCHAR2) RETURN VARCHAR2 DETERMINISTIC;
  FUNCTION norm_xa_key   (p IN VARCHAR2) RETURN VARCHAR2 DETERMINISTIC;
  FUNCTION norm_ngay_sinh(p IN NUMBER)   RETURN VARCHAR2;   -- phụ thuộc SYSDATE nên KHÔNG khai DETERMINISTIC
  FUNCTION norm_phone    (p IN VARCHAR2) RETURN VARCHAR2 DETERMINISTIC;
  FUNCTION norm_name     (p IN VARCHAR2) RETURN VARCHAR2 DETERMINISTIC;
  FUNCTION khong_dau     (p IN VARCHAR2) RETURN VARCHAR2 DETERMINISTIC;
  FUNCTION pick_dinh_danh(p_cccd IN VARCHAR2, p_cmnd IN VARCHAR2) RETURN VARCHAR2 DETERMINISTIC;
  FUNCTION to_num_safe   (p IN VARCHAR2) RETURN NUMBER DETERMINISTIC;
END PKG_C360_UTIL;
/
CREATE OR REPLACE PACKAGE BODY PKG_C360_UTIL AS

  c_vn_from CONSTANT VARCHAR2(2000) := 'ÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïñòóôõöùúûüýÿĀāĂăĄąĆćĈĉĊċČčĎďĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĨĩĪīĬĭĮįİĴĵĶķĹĺĻļĽľŃńŅņŇňŌōŎŏŐőŔŕŖŗŘřŚśŜŝŞşŠšŢţŤťŨũŪūŬŭŮůŰűŲųŴŵŶŷŸŹźŻżŽžƠơƯưǍǎǏǐǑǒǓǔǕǖǗǘǙǚǛǜǞǟǠǡǦǧǨǩǪǫǬǭǰǴǵǸǹǺǻȀȁȂȃȄȅȆȇȈȉȊȋȌȍȎȏȐȑȒȓȔȕȖȗȘșȚțȞȟȦȧȨȩȪȫȬȭȮȯȰȱȲȳẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹĐđ';
  c_vn_to   CONSTANT VARCHAR2(2000) := 'AAAAAACEEEEIIIINOOOOOUUUUYaaaaaaceeeeiiiinooooouuuuyyAaAaAaCcCcCcCcDdEeEeEeEeEeGgGgGgGgHhIiIiIiIiIJjKkLlLlLlNnNnNnOoOoOoRrRrRrSsSsSsSsTtTtUuUuUuUuUuUuWwYyYZzZzZzOoUuAaIiOoUuUuUuUuUuAaAaGgKkOoOojGgNnAaAaAaEeEeIiIiOoOoRrRrUuUuSsTtHhAaEeOoOoOoOoYyAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYyDd';

  -- Mã tỉnh nguồn -> khóa 3 số. 'NNTTT' | '0NN' | 'NN' | 'N' -> '0NN'. Rỗng/0/sai định dạng -> NULL.
  FUNCTION norm_tinh_key(p IN VARCHAR2) RETURN VARCHAR2 IS
    PRAGMA UDF;
    v VARCHAR2(50);
  BEGIN
    v := UPPER(TRIM(p));
    IF v IS NULL THEN RETURN NULL; END IF;
    v := REGEXP_REPLACE(v, 'TTT$', '');
    IF NOT REGEXP_LIKE(v, '^[0-9]{1,3}$') THEN RETURN NULL; END IF;
    IF TO_NUMBER(v) = 0 THEN RETURN NULL; END IF;
    RETURN LPAD(v, 3, '0');
  END;

  -- Mã xã nguồn -> khóa 5 số (khôi phục số 0 đầu do ép kiểu số). Rỗng/0/sai -> NULL.
  FUNCTION norm_xa_key(p IN VARCHAR2) RETURN VARCHAR2 IS
    PRAGMA UDF;
    v VARCHAR2(50);
  BEGIN
    v := TRIM(p);
    IF v IS NULL THEN RETURN NULL; END IF;
    IF NOT REGEXP_LIKE(v, '^[0-9]{1,5}$') THEN RETURN NULL; END IF;
    IF TO_NUMBER(v) = 0 THEN RETURN NULL; END IF;
    RETURN LPAD(v, 5, '0');
  END;

  -- NGAY_SINH NUMBER -> 'YYYYMMDD' | 'YYYYMM' | 'YYYY' | NULL (sai lịch / ngoài [1900, hiện tại])
  FUNCTION norm_ngay_sinh(p IN NUMBER) RETURN VARCHAR2 IS
    PRAGMA UDF;
    s VARCHAR2(30); y NUMBER; m NUMBER; d NUMBER;
    cur_y NUMBER := EXTRACT(YEAR FROM SYSDATE);
    v_first DATE; v_full VARCHAR2(8);
  BEGIN
    IF p IS NULL OR p <> TRUNC(p) OR p <= 0 THEN RETURN NULL; END IF;
    s := TO_CHAR(p, 'FM99999999999999999999');
    IF LENGTH(s) = 4 THEN
      y := TO_NUMBER(s); m := 0; d := 0;
    ELSIF LENGTH(s) = 6 THEN
      y := TO_NUMBER(SUBSTR(s,1,4)); m := TO_NUMBER(SUBSTR(s,5,2)); d := 0;
    ELSIF LENGTH(s) = 8 THEN
      y := TO_NUMBER(SUBSTR(s,1,4)); m := TO_NUMBER(SUBSTR(s,5,2)); d := TO_NUMBER(SUBSTR(s,7,2));
    ELSE
      RETURN NULL;
    END IF;
    IF y < 1900 OR y > cur_y THEN RETURN NULL; END IF;
    IF m = 0 THEN
      IF d <> 0 THEN RETURN NULL; END IF;
      RETURN TO_CHAR(y);
    END IF;
    IF m > 12 THEN RETURN NULL; END IF;
    IF d = 0 THEN
      IF y = cur_y AND m > EXTRACT(MONTH FROM SYSDATE) THEN RETURN NULL; END IF;
      RETURN TO_CHAR(y) || LPAD(m, 2, '0');
    END IF;
    v_first := TO_DATE(TO_CHAR(y) || LPAD(m, 2, '0') || '01', 'YYYYMMDD');
    IF d > TO_NUMBER(TO_CHAR(LAST_DAY(v_first), 'DD')) THEN RETURN NULL; END IF;
    v_full := TO_CHAR(y) || LPAD(m, 2, '0') || LPAD(d, 2, '0');
    IF TO_DATE(v_full, 'YYYYMMDD') > TRUNC(SYSDATE) THEN RETURN NULL; END IF;
    RETURN v_full;
  END;

  -- Số điện thoại -> dạng bắt đầu bằng 0, 10 hoặc 11 số; không chuẩn hóa được -> NULL
  FUNCTION norm_phone(p IN VARCHAR2) RETURN VARCHAR2 IS
    PRAGMA UDF;
    v VARCHAR2(100);
  BEGIN
    v := REGEXP_REPLACE(p, '[^0-9]', '');
    IF v IS NULL THEN RETURN NULL; END IF;
    IF SUBSTR(v,1,2) = '84' AND LENGTH(v) IN (11, 12) THEN
      IF SUBSTR(v,3,1) = '0' THEN v := SUBSTR(v,3); ELSE v := '0' || SUBSTR(v,3); END IF;
    ELSIF SUBSTR(v,1,1) <> '0' AND LENGTH(v) = 9 THEN
      v := '0' || v;
    END IF;
    IF REGEXP_LIKE(v, '^0[0-9]{9,10}$') THEN RETURN v; END IF;
    RETURN NULL;
  END;

  -- Cắt khoảng trắng thừa (đầu/giữa/cuối)
  FUNCTION norm_name(p IN VARCHAR2) RETURN VARCHAR2 IS
    PRAGMA UDF;
  BEGIN
    RETURN TRIM(REGEXP_REPLACE(p, '[[:space:]]+', ' '));
  END;

  -- Không dấu + viết hoa (đã chuẩn hóa khoảng trắng)
  FUNCTION khong_dau(p IN VARCHAR2) RETURN VARCHAR2 IS
    PRAGMA UDF;
  BEGIN
    RETURN UPPER(TRANSLATE(norm_name(p), c_vn_from, c_vn_to));
  END;

  -- Số định danh 12 số: ưu tiên SO_CCCD, không có thì SO_CMTND
  FUNCTION pick_dinh_danh(p_cccd IN VARCHAR2, p_cmnd IN VARCHAR2) RETURN VARCHAR2 IS
    PRAGMA UDF;
  BEGIN
    IF REGEXP_LIKE(TRIM(p_cccd), '^[0-9]{12}$') THEN RETURN TRIM(p_cccd); END IF;
    IF REGEXP_LIKE(TRIM(p_cmnd), '^[0-9]{12}$') THEN RETURN TRIM(p_cmnd); END IF;
    RETURN NULL;
  END;

  FUNCTION to_num_safe(p IN VARCHAR2) RETURN NUMBER IS
    PRAGMA UDF;
  BEGIN
    IF REGEXP_LIKE(TRIM(p), '^[0-9]{1,18}$') THEN RETURN TO_NUMBER(TRIM(p)); END IF;
    RETURN NULL;
  END;

END PKG_C360_UTIL;
/
