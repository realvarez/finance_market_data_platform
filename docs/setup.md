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

| Service | URL | Credentials | Profile |
|---------|-----|-------------|---------|
| Airflow | http://localhost:8080 | — (dev mode: all-admins, no login) | default |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin | default |
| ClickHouse HTTP | http://localhost:8123 | clickhouse / clickhouse | default |
| AKHQ (Kafka UI) | http://localhost:9021 | — | `ui` |
| Spark Master UI | http://localhost:8082 | — | `spark-cluster` |

AKHQ and Spark Master are optional and are **not** started by `docker compose up -d`. Start them with
`--profile ui` or `--profile spark-cluster` respectively. There is no Schema Registry — schema
validation is client-side via `jsonschema`.

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

> **Known issue:** the reconciliation DAG currently reconciles nothing — it reads Kafka topic offsets
> with a 10-second window on a 5-minute schedule. It is being rewritten to compare candles in
> ClickHouse. See [context/CURRENT_STATE.md](../context/CURRENT_STATE.md) and
> [ADR-009](../context/DECISIONS.md#adr-009-clickhouse-is-the-source-of-truth-for-reconciliation).

## Run Spark Streaming Locally

The `candle-builder` service submits the streaming job automatically on startup:

```bash
docker compose up -d candle-builder
docker compose logs -f candle-builder
```

To submit manually inside the same container instead (e.g. for development):

```bash
docker compose exec candle-builder /opt/spark/bin/spark-submit \
  --master local[2] \
  --driver-memory 768m \
  --conf spark.sql.shuffle.partitions=2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.780 \
  /opt/spark/apps/stream_candle_builder.py
```

To run against a distributed Spark cluster instead, start it with `--profile spark-cluster` and
submit with `--master spark://spark-master:7077`. The cluster profile is off by default.

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
