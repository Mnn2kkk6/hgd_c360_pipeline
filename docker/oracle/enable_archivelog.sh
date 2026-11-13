#!/usr/bin/env bash
# Bật ARCHIVELOG cho Oracle XE trong container (bắt buộc cho Debezium LogMiner). Chạy 1 lần từ máy host:
#   bash docker/oracle/enable_archivelog.sh
set -euo pipefail
docker exec hgd-oracle bash -c "mkdir -p /opt/oracle/oradata/recovery_area"
docker exec -i hgd-oracle sqlplus -s / as sysdba <<'SQL'
ALTER SYSTEM SET db_recovery_file_dest_size = 10G;
ALTER SYSTEM SET db_recovery_file_dest = '/opt/oracle/oradata/recovery_area';
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ARCHIVE LOG LIST
SQL
