FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --no-cache-dir \
    aiokafka jsonschema kafka-python-ng yfinance numpy

COPY ingestion/ ./ingestion/
COPY entrypoint/ ./entrypoint/
COPY schemas/ ./schemas/
COPY utils/ ./utils/

ENV PYTHONPATH=/app
ENV KAFKA_BOOTSTRAP_SERVERS=broker:29092

CMD ["python", "-m", "entrypoint.tick_service"]
