FROM apache/airflow:3.2.0-python3.12

USER root

RUN apt-get update && \
    apt-get install -y build-essential && \
    rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /opt/airflow

COPY requirements.txt ./
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

