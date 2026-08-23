# Development Guide

## Project Layout

```
market_platform/
├── ingestion/       # Yahoo clients, normalization, Kafka producers
├── streaming/       # Spark jobs, reconciliation
├── storage/         # MinIO + ClickHouse sinks
├── analysis/        # Features, data quality
├── entrypoint/      # Long-running service entrypoints
├── airflow/dags/    # Thin orchestration DAGs
├── schemas/v1/      # JSON Schema contracts
├── infra/           # Topic init, ClickHouse DDL
└── tests/           # Unit and integration tests
```

## Running Ingestion Locally

### Tick service (standalone)

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SYMBOLS=NVDA,AAPL
uv run python -m entrypoint.tick_service
```

### Historical candles (CLI)

```bash
uv run python -c "
from ingestion.candles import fetch_latest_candles
fetch_latest_candles(symbol='NVDA', interval='1m', overlap_minutes=5)
"
```

### Spark candle builder

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
uv run python -m streaming.stream_candle_builder
```

## Adding a New Symbol

1. Update `SYMBOLS` in `docker-compose.yml` for the tick service:

```yaml
tick-ingestion:
  environment:
    SYMBOLS: "NVDA,AAPL,SPY,MSFT"
```

2. Update Airflow DAG params or set an Airflow Variable:

```python
# In dag_candles.py params
"symbols": Param(default="NVDA,AAPL,SPY", type="string")
```

3. Restart the tick service:

```bash
docker compose up -d tick-ingestion
```

## Testing Kafka Integration

### Produce a test tick

```bash
uv run python -c "
import asyncio, json
from aiokafka import AIOKafkaProducer

async def main():
    p = AIOKafkaProducer(bootstrap_servers=['localhost:9092'])
    await p.start()
    msg = {
        'event_id': 'TEST20260812143000',
        'symbol': 'TEST',
        'timestamp': '2026-08-12T14:30:00Z',
        'ingestion_timestamp': '2026-08-12T14:30:00Z',
        'price': 100.0,
        'source': 'test'
    }
    await p.send('market.ticks', json.dumps(msg).encode(), key=b'TEST')
    await p.stop()

asyncio.run(main())
"
```

### Consume and verify

```bash
docker compose exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic market.ticks \
  --from-beginning --max-messages 1
```

## Writing Tests

Tests live in `tests/`. Run with:

```bash
uv run pytest tests/ -v
```

Example test structure:

```python
# tests/test_schema_validator.py
from ingestion.schema_validator import validate_tick


def test_valid_tick():
    tick = {
        "event_id": "AAPL20260812143000",
        "symbol": "AAPL",
        "timestamp": "2026-08-12T14:30:00Z",
        "ingestion_timestamp": "2026-08-12T14:30:00Z",
        "price": 227.50,
        "source": "yahoo_finance",
    }
    result = validate_tick(tick)
    assert result["symbol"] == "AAPL"
```

## Linting

```bash
uv run ruff check .
uv run ruff format .
```

## Adding a New Airflow DAG

1. Create business logic in the appropriate module (e.g. `analysis/my_job.py`)
2. Create a thin DAG in `airflow/dags/`:

```python
from airflow.sdk import dag, task
import pendulum
from analysis.my_job import run_my_job


@task()
def my_task():
    run_my_job()


@dag(
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["analysis"],
)
def my_dag():
    my_task()


my_dag()
```

3. Restart Airflow or wait for DAG refresh (~30s)

## Environment Variables

| Variable | Default | Used By |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `broker:29092` | All Kafka clients |
| `SYMBOLS` | `NVDA,AAPL,SPY` | Tick service |
| `MINIO_ENDPOINT` | `minio:9000` | Storage sinks |
| `MINIO_ACCESS_KEY` | `minioadmin` | Storage sinks |
| `MINIO_SECRET_KEY` | `minioadmin` | Storage sinks |
| `CLICKHOUSE_HOST` | `clickhouse` | Storage sinks |
| `CLICKHOUSE_PORT` | `8123` | Storage sinks |

## Code Conventions

- Business logic in importable modules, not in DAG files
- Validate all outbound Kafka messages against schemas
- Use `ingestion.config` for shared configuration
- Log errors to `market.errors` topic, not just stderr
- Use ISO 8601 timestamps with timezone (UTC)
