-- Tùy chọn: lập lịch NRT trong Oracle (chọn 1 trong 2: DBMS_SCHEDULER hoặc Airflow, KHÔNG bật cả hai cùng lúc)
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'JOB_C360_NRT',
    job_type        => 'STORED_PROCEDURE',
    job_action      => 'PRC_C360_NRT',
    repeat_interval => 'FREQ=MINUTELY;INTERVAL=1',
    enabled         => TRUE,
    comments        => 'Near real-time Mirror -> C360');
END;
/
