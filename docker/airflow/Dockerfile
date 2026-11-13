FROM apache/airflow:2.10.3-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir oracledb pyspark==3.5.3 pandas requests
