FROM apache/airflow:3.2.0-python3.12

USER root

RUN apt-get update && \
    apt-get install -y build-essential && \
    rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /opt/airflow

# Runtime dependencies for DAG code, aligned with pyproject.toml / uv.lock:
# yfinance (candle fetch), kafka client (reconciliation), clickhouse/minio/pyarrow/numpy
# (storage + features), jsonschema (contracts).
RUN pip install --upgrade pip && \
    pip install \
        aiokafka \
        clickhouse-connect \
        jsonschema \
        kafka-python-ng \
        minio \
        numpy \
        pyarrow \
        yfinance

ENV PYTHONPATH=/opt/airflow
