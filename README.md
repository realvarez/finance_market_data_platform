# Market Platform

Market data platform that ingests Yahoo Finance ticks and candles, processes them through Kafka and
Spark, and stores them in MinIO and ClickHouse for feature engineering and eventual trading.

## Architecture

```
Yahoo WebSocket → Tick Service → Kafka → Spark → Calculated Candles ─┐
                                                │                   ├→ Reconciliation → ClickHouse/MinIO
Yahoo History   → Airflow DAGs → Raw Candles ────┘                   │         ↓
                                                                  ClickHouse   Features → Signals
```

Ingestion, streaming, storage, and analysis are separate services communicating over Kafka. Airflow
runs only finite scheduled jobs; the WebSocket ingestion and Spark streaming services are long-running
compose services.

See [context/ARCHITECTURE.md](context/ARCHITECTURE.md) for the full system diagram.

> **Status:** ingestion and streaming work. **Reconciliation is not currently operating** — see
> [context/CURRENT_STATE.md](context/CURRENT_STATE.md). Trading work is gated on fixing it
> ([ADR-010](context/DECISIONS.md#adr-010-verification-gates-the-trading-stages)).

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

Python services mount the source tree, so a code edit needs only `docker compose restart <service>`.
Rebuild the images only after changing `pyproject.toml` or `uv.lock` — see
[docs/development.md](docs/development.md#code-changes-and-container-images).

## Service URLs

| Service | URL | Credentials | Profile |
|---------|-----|-------------|---------|
| Airflow | http://localhost:8080 | — (dev mode: all-admins, no login) | default |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin | default |
| ClickHouse | http://localhost:8123 | clickhouse / clickhouse | default |
| AKHQ (Kafka UI) | http://localhost:9021 | — | `ui` |
| Spark Master | http://localhost:8082 | — | `spark-cluster` |

AKHQ and the Spark cluster are optional profiles and are **not** started by `docker compose up -d`:

```bash
docker compose --profile ui up -d            # add the Kafka topic browser
docker compose --profile spark-cluster up -d # add multi-container Spark
```

The default stack runs Spark as a single `candle-builder` container in `local[2]` mode. Full port and
resource map: [docs/services.md](docs/services.md).

## Documentation

Reference:
- [Setup Guide](docs/setup.md)
- [Development Guide](docs/development.md)
- [Services Reference](docs/services.md)
- [Kafka Topics](docs/kafka-topics.md)
- [Data Schemas](docs/schemas.md)

Project direction:
- [Objective](context/OBJECTIVE.md) — goal, constraints, success criteria
- [Current State](context/CURRENT_STATE.md) — what works, what is broken, technical debt
- [Roadmap](context/ROADMAP.md) — phases, milestones, verification gate
- [Architecture](context/ARCHITECTURE.md) — system design and component responsibilities
- [Decisions](context/DECISIONS.md) — architecture decision records (ADRs)

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
tests/            Unit tests
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
