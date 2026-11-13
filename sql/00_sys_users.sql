-- Chạy bằng SYS vào PDB (XEPDB1): python -m hgd.tools.run_sql sql/00_sys_users.sql --as sys-pdb
-- 3 schema: VSS (nguồn giả lập), MIRROR (vùng Mirror), C360 (chuẩn hóa + điều khiển + báo cáo + API)
-- Mật khẩu lab; đổi theo .env khi triển khai thật.
CREATE USER VSS IDENTIFIED BY vss_pwd QUOTA UNLIMITED ON USERS
/
CREATE USER MIRROR IDENTIFIED BY mirror_pwd QUOTA UNLIMITED ON USERS
/
CREATE USER C360 IDENTIFIED BY c360_pwd QUOTA UNLIMITED ON USERS
/
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, CREATE TRIGGER, CREATE SEQUENCE, CREATE JOB TO VSS
/
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, CREATE TRIGGER, CREATE SEQUENCE TO MIRROR
/
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, CREATE TRIGGER, CREATE SEQUENCE, CREATE JOB TO C360
/
-- C360 chỉ ĐỌC nguồn để đối soát (lab dùng chung 1 instance; production dùng kết nối riêng)
-- Quyền SELECT trên bảng VSS cấp sau khi bảng được tạo: xem cuối sql/10_vss_source.sql -> chạy lại 02_grants.sql
