# Market Platform

End-to-end market data pipeline that ingests Yahoo Finance ticks and candles, processes them through Kafka and Spark, stores data in MinIO and ClickHouse, and feeds a trading bot with features and signals.

## Architecture

```
Yahoo WebSocket → Tick Service → Kafka → Spark → Calculated Candles → Reconciliation → ClickHouse/MinIO
Yahoo History   → Airflow DAGs → Kafka ↗                                              → Features → Signals
```

See [context/ARCHITECTURE.md](context/ARCHITECTURE.md) for the full system diagram.

## Quick Start

```bash
# Install dependencies
uv sync

# Start all services
docker compose up -d

# Verify services
docker compose ps

# View tick ingestion logs
docker compose logs -f tick-ingestion
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | — (dev mode: all-admins, no login) |
| Kafka Control Center | http://localhost:9021 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| ClickHouse | http://localhost:8123 | default / (empty) |
| Spark Master | http://localhost:8082 | — |

## Documentation

- [Setup Guide](docs/setup.md)
- [Development Guide](docs/development.md)
- [Kafka Topics](docs/kafka-topics.md)
- [Data Schemas](docs/schemas.md)
- [Services Reference](docs/services.md)
- [Full Roadmap](context/ROADMAP.md)
- [Current State](context/CURRENT_STATE.md)

## Project Structure

```
airflow/dags/     Thin orchestration DAGs
ingestion/        Yahoo clients, normalization, Kafka producers
streaming/        Spark Structured Streaming, reconciliation
storage/          MinIO + ClickHouse sinks
analysis/         Feature engineering, data quality
entrypoint/       Long-running service entrypoints
schemas/v1/       JSON Schema data contracts
infra/            Topic init, ClickHouse DDL
tests/            Unit and integration tests
```

## Kafka Commands

### List topics

```bash
docker compose exec broker kafka-topics --list --bootstrap-server localhost:9092
```

### Create a topic

```bash
docker compose exec broker kafka-topics \
  --create --bootstrap-server localhost:9092 \
  --topic <topic_name> --partitions 3 --replication-factor 1
```

### Consume messages

```bash
docker compose exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic market.ticks --from-beginning --max-messages 5
```

## Run Tests

```bash
uv run pytest tests/ -v
uv run ruff check .
```

## Technology Stack

Python 3.12 · uv · Airflow 3.2 · Kafka · Spark · MinIO · ClickHouse · yfinance
