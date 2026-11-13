# Debezium Connect + driver Oracle + JDBC Sink connector, lấy từ file đã tải sẵn trong docker/connect/libs/
# (mạng chặn Maven khi build). Driver PHẢI là ojdbc8 21.x: ojdbc11 23.x làm Debezium 2.7 gửi mật khẩu rỗng (ORA-01005).
# Không rm file tar: user của image không xóa được file COPY của root trong /tmp.
FROM quay.io/debezium/connect:2.7.3.Final
COPY libs/ojdbc8.jar /kafka/connect/debezium-connector-oracle/ojdbc8.jar
COPY libs/jdbc-plugin.tar.gz /tmp/jdbc-plugin.tar.gz
RUN tar xzf /tmp/jdbc-plugin.tar.gz -C /kafka/connect \
 && cp /kafka/connect/debezium-connector-oracle/ojdbc8.jar /kafka/connect/debezium-connector-jdbc/ojdbc8.jar
