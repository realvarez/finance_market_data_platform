# Setup Guide

## Prerequisites

- **Docker** and **Docker Compose** (v2+)
- **Python 3.12**
- **uv** package manager ([install](https://docs.astral.sh/uv/getting-started/installation/))

## Clone and Install

```bash
git clone <repository-url> market_platform
cd market_platform
uv sync
```

## Start the Platform

```bash
docker compose up -d
```

This starts all services. First boot may take 2–3 minutes while Airflow initializes its database.

### Verify Services

```bash
docker compose ps
```

All services should show `healthy` or `running`.

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | — (dev mode: all-admins, no login) |
| Kafka Control Center | http://localhost:9021 | — |
| Schema Registry | http://localhost:8081 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| ClickHouse HTTP | http://localhost:8123 | default / (empty) |
| Spark Master UI | http://localhost:8082 | — |

See [services.md](services.md) for full port map.

## Initialize Kafka Topics

Topics are created automatically by the `kafka-init` service on first startup. To recreate manually:

```bash
docker compose exec broker bash /infra/kafka/init-topics.sh
```

## Start Tick Ingestion

The tick service runs as a Docker Compose service and starts automatically:

```bash
docker compose up -d tick-ingestion
docker compose logs -f tick-ingestion
```

Configure symbols via environment in `docker-compose.yml`:

```yaml
environment:
  SYMBOLS: "NVDA,AAPL,SPY"
```

## Enable Airflow DAGs

1. Open http://localhost:8080 (no login required in dev mode)
2. Unpause the candle and reconciliation DAGs

## Run Spark Streaming Locally

The `candle-builder` service submits the streaming job automatically on startup:

```bash
docker compose up -d candle-builder
docker compose logs -f candle-builder
```

To submit manually instead (e.g. for development):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/stream_candle_builder.py
```

## Run Tests

```bash
uv run pytest tests/ -v
```

## Stop the Platform

```bash
docker compose down
```

To remove persistent data volumes:

```bash
docker compose down -v
```

## Troubleshooting

### Kafka topics not created

```bash
docker compose logs kafka-init
docker compose restart kafka-init
```

### Airflow DAG import errors

Ensure project code is mounted into the Airflow container. Check logs:

```bash
docker compose logs airflow | grep -i error
```

### Tick service not connecting

Verify Kafka is healthy:

```bash
docker compose exec broker kafka-topics --list --bootstrap-server localhost:9092
```

Check tick service logs:

```bash
docker compose logs tick-ingestion
```
