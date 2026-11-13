-- Chạy bằng user C360. VIEW CHUẨN HÓA DUY NHẤT: Full Load và Near Real-time đều đọc từ view này.
-- Lớp 1 (valid): lọc nghiệp vụ + khử trùng theo MA_CD (dòng mới nhất).
-- Lớp 2 (s1): khóa địa bàn chuẩn. Lớp 3 (s2): join Map/danh mục. Lớp 4: làm sạch + gán DQ_FLAGS.
CREATE OR REPLACE VIEW V_C360_NORMALIZED AS
WITH valid AS (
  SELECT m.*,
         ROW_NUMBER() OVER (PARTITION BY m.MA_CD
                            ORDER BY m.MODIFY_DATE DESC NULLS LAST, m.CREATED_DATE DESC NULLS LAST, m.ID DESC NULLS LAST) AS RN
  FROM   MIRROR.VSS_HO_GD_NHANKHAU m
  WHERE  NVL(m.TYPEID, 'ZZ') NOT IN ('GT','GM','GG','GK')
  AND    NVL(m.TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
),
s1 AS (
  SELECT v.MA_CD, v.MA_CD_OLD, v.MA_HO_GD, v.QH_CHU_HO, v.HO_TEN, v.GIOI_TINH, v.NGAY_SINH, v.IS_ALIVE, v.DEAD_DATE,
         v.SO_CCCD, v.SO_CMTND, v.PHONE_NUMBER, v.EMAIL_ACC, v.QUOC_TICH, v.DM_DAN_TOC_ID,
         v.DIA_CHI, v.DC_LIEN_LAC, v.MODIFY_DATE,
       v.MA_TINH AS RAW_TINH_TT,
       v.MA_XA AS RAW_XA_TT,
       v.MA_TINH_LH AS RAW_TINH_LH,
       v.MA_XA_LH AS RAW_XA_LH,
       v.MA_TINH_KS AS RAW_TINH_KS,
       v.MA_XA_KS AS RAW_XA_KS,
       PKG_C360_UTIL.norm_tinh_key(v.MA_TINH) AS TK_TT,
       PKG_C360_UTIL.norm_xa_key(v.MA_XA)     AS XK_TT,
       PKG_C360_UTIL.norm_tinh_key(v.MA_TINH_LH) AS TK_LH,
       PKG_C360_UTIL.norm_xa_key(v.MA_XA_LH)     AS XK_LH,
       PKG_C360_UTIL.norm_tinh_key(v.MA_TINH_KS) AS TK_KS,
       PKG_C360_UTIL.norm_xa_key(v.MA_XA_KS)     AS XK_KS
  FROM   valid v
  WHERE  v.RN = 1
),
s2 AS (
  SELECT s.*,
       COALESCE(mxTT.MA_TINH, mtTT.MA_TINH)                                   AS F_MA_TINH_TT,
       CASE WHEN mxTT.MA_TINH IS NOT NULL THEN mxTT.TEN_TINH ELSE mtTT.TEN_TINH END AS F_TEN_TINH_TT,
       mxTT.MA_XA AS F_MA_XA_TT, mxTT.TEN_XA AS F_TEN_XA_TT, mxTT.NGUON_MAP AS F_NGUON_TT,
       CASE WHEN mxTT.MA_TINH IS NOT NULL AND mtTT.MA_TINH IS NOT NULL AND mxTT.MA_TINH <> mtTT.MA_TINH THEN 1 ELSE 0 END AS F_LECH_TT,
       COALESCE(mxLH.MA_TINH, mtLH.MA_TINH)                                   AS F_MA_TINH_LH,
       CASE WHEN mxLH.MA_TINH IS NOT NULL THEN mxLH.TEN_TINH ELSE mtLH.TEN_TINH END AS F_TEN_TINH_LH,
       mxLH.MA_XA AS F_MA_XA_LH, mxLH.TEN_XA AS F_TEN_XA_LH, mxLH.NGUON_MAP AS F_NGUON_LH,
       CASE WHEN mxLH.MA_TINH IS NOT NULL AND mtLH.MA_TINH IS NOT NULL AND mxLH.MA_TINH <> mtLH.MA_TINH THEN 1 ELSE 0 END AS F_LECH_LH,
       COALESCE(mxKS.MA_TINH, mtKS.MA_TINH)                                   AS F_MA_TINH_KS,
       CASE WHEN mxKS.MA_TINH IS NOT NULL THEN mxKS.TEN_TINH ELSE mtKS.TEN_TINH END AS F_TEN_TINH_KS,
       mxKS.MA_XA AS F_MA_XA_KS, mxKS.TEN_XA AS F_TEN_XA_KS, mxKS.NGUON_MAP AS F_NGUON_KS,
       CASE WHEN mxKS.MA_TINH IS NOT NULL AND mtKS.MA_TINH IS NOT NULL AND mxKS.MA_TINH <> mtKS.MA_TINH THEN 1 ELSE 0 END AS F_LECH_KS,
         NVL(q1.MA, q2.MA)   AS QT_MA,
         NVL(q1.TEN, q2.TEN) AS QT_TEN,
         dt.MA AS DT_MA, dt.TEN AS DT_TEN, dt.ID AS DT_ID
  FROM   s1 s
LEFT JOIN C360_MAP_DBHC_XA   mxTT ON mxTT.MA_XA_NGUON    = s.XK_TT
LEFT JOIN C360_MAP_DBHC_TINH mtTT ON mtTT.MA_TINH_NGUON = s.TK_TT
LEFT JOIN C360_MAP_DBHC_XA   mxLH ON mxLH.MA_XA_NGUON    = s.XK_LH
LEFT JOIN C360_MAP_DBHC_TINH mtLH ON mtLH.MA_TINH_NGUON = s.TK_LH
LEFT JOIN C360_MAP_DBHC_XA   mxKS ON mxKS.MA_XA_NGUON    = s.XK_KS
LEFT JOIN C360_MAP_DBHC_TINH mtKS ON mtKS.MA_TINH_NGUON = s.TK_KS
  LEFT JOIN MIRROR.VSS_DM_QUOC_TICH q1 ON q1.MA = UPPER(TRIM(s.QUOC_TICH))
  LEFT JOIN MIRROR.VSS_DM_QUOC_TICH q2 ON q2.ID = PKG_C360_UTIL.to_num_safe(s.QUOC_TICH)
  LEFT JOIN MIRROR.VSS_DM_DANTOC    dt ON dt.ID = s.DM_DAN_TOC_ID
)
SELECT x.MA_CD                                              AS MA_SO_BHXH,
       x.MA_CD_OLD                                          AS MA_SO_BHXH_CU,
       PKG_C360_UTIL.pick_dinh_danh(x.SO_CCCD, x.SO_CMTND)  AS SO_DINH_DANH,
       PKG_C360_UTIL.norm_name(x.HO_TEN)                    AS HO_TEN,
       PKG_C360_UTIL.khong_dau(x.HO_TEN)                    AS HO_TEN_KHONG_DAU,
       x.GIOI_TINH,
       PKG_C360_UTIL.norm_ngay_sinh(x.NGAY_SINH)            AS NGAY_SINH,
       x.NGAY_SINH                                          AS NGAY_SINH_GOC,
       x.IS_ALIVE, x.DEAD_DATE,
       PKG_C360_UTIL.norm_phone(x.PHONE_NUMBER)             AS SO_DIEN_THOAI,
       x.PHONE_NUMBER                                       AS SO_DIEN_THOAI_GOC,
       x.EMAIL_ACC                                          AS EMAIL,
       x.QT_MA                                              AS QUOC_TICH_MA,
       x.QT_TEN                                             AS QUOC_TICH_TEN,
       x.QUOC_TICH                                          AS QUOC_TICH_GOC,
       x.DT_ID                                              AS DAN_TOC_ID,
       x.DT_MA                                              AS DAN_TOC_MA,
       x.DT_TEN                                             AS DAN_TOC_TEN,
       x.MA_HO_GD, x.QH_CHU_HO,
       x.F_MA_TINH_TT   AS MA_TINH,
       x.F_TEN_TINH_TT  AS TEN_TINH,
       x.F_MA_XA_TT     AS MA_XA,
       x.F_TEN_XA_TT    AS TEN_XA,
       x.RAW_TINH_TT    AS MA_TINH_GOC,
       x.RAW_XA_TT      AS MA_XA_GOC,
       x.F_NGUON_TT     AS NGUON_MAP_XA,
       x.F_MA_TINH_LH   AS MA_TINH_LH,
       x.F_TEN_TINH_LH  AS TEN_TINH_LH,
       x.F_MA_XA_LH     AS MA_XA_LH,
       x.F_TEN_XA_LH    AS TEN_XA_LH,
       x.RAW_TINH_LH    AS MA_TINH_LH_GOC,
       x.RAW_XA_LH      AS MA_XA_LH_GOC,
       x.F_NGUON_LH     AS NGUON_MAP_XA_LH,
       x.F_MA_TINH_KS   AS MA_TINH_KS,
       x.F_TEN_TINH_KS  AS TEN_TINH_KS,
       x.F_MA_XA_KS     AS MA_XA_KS,
       x.F_TEN_XA_KS    AS TEN_XA_KS,
       x.RAW_TINH_KS    AS MA_TINH_KS_GOC,
       x.RAW_XA_KS      AS MA_XA_KS_GOC,
       x.F_NGUON_KS     AS NGUON_MAP_XA_KS,
       x.DIA_CHI, x.DC_LIEN_LAC,
       SUBSTR(LTRIM(
       CASE WHEN x.F_MA_TINH_TT IS NULL THEN ';TINH_KHONG_MAP' END ||
       CASE WHEN x.F_NGUON_TT IS NULL THEN ';XA_KHONG_MAP' END ||
       CASE WHEN x.F_NGUON_TT = 'MO_HO' THEN ';XA_MO_HO' END ||
       CASE WHEN x.F_LECH_TT = 1 THEN ';TINH_LECH_XA' END ||
       CASE WHEN (x.RAW_TINH_LH IS NOT NULL OR x.RAW_XA_LH IS NOT NULL) AND x.F_MA_TINH_LH IS NULL THEN ';TINH_KHONG_MAP_LH' END ||
       CASE WHEN x.RAW_XA_LH IS NOT NULL AND x.F_NGUON_LH IS NULL THEN ';XA_KHONG_MAP_LH' END ||
       CASE WHEN x.F_NGUON_LH = 'MO_HO' THEN ';XA_MO_HO_LH' END ||
       CASE WHEN x.F_LECH_LH = 1 THEN ';TINH_LECH_XA_LH' END ||
       CASE WHEN (x.RAW_TINH_KS IS NOT NULL OR x.RAW_XA_KS IS NOT NULL) AND x.F_MA_TINH_KS IS NULL THEN ';TINH_KHONG_MAP_KS' END ||
       CASE WHEN x.RAW_XA_KS IS NOT NULL AND x.F_NGUON_KS IS NULL THEN ';XA_KHONG_MAP_KS' END ||
       CASE WHEN x.F_NGUON_KS = 'MO_HO' THEN ';XA_MO_HO_KS' END ||
       CASE WHEN x.F_LECH_KS = 1 THEN ';TINH_LECH_XA_KS' END ||
       CASE WHEN PKG_C360_UTIL.norm_ngay_sinh(x.NGAY_SINH) IS NULL THEN ';NGAY_SINH_LOI' END ||
       CASE WHEN x.PHONE_NUMBER IS NOT NULL AND PKG_C360_UTIL.norm_phone(x.PHONE_NUMBER) IS NULL THEN ';SDT_LOI' END ||
       CASE WHEN PKG_C360_UTIL.pick_dinh_danh(x.SO_CCCD, x.SO_CMTND) IS NULL THEN ';KHONG_SO_DINH_DANH' END ||
       CASE WHEN TRIM(x.QUOC_TICH) IS NOT NULL AND x.QT_MA IS NULL THEN ';QUOC_TICH_KHONG_MAP' END ||
       CASE WHEN x.DM_DAN_TOC_ID IS NOT NULL AND x.DT_ID IS NULL THEN ';DAN_TOC_KHONG_MAP' END
       , ';'), 1, 600)                                      AS DQ_FLAGS,
       x.MODIFY_DATE                                        AS MODIFY_DATE_NGUON,
       SYSDATE                                              AS SYNC_DATE
FROM   s2 x
/
