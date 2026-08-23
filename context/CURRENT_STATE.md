# Current State

Last updated: August 2026

## Summary

The repository is an early prototype (~Phase 1–4 partial). Kafka and Airflow run in Docker Compose. Basic Yahoo Finance ingestion exists but is duplicated, inconsistently named, and partially broken.

## What Works

| Component | Location | Notes |
|-----------|----------|-------|
| Docker Compose stack | `docker-compose.yml` | Kafka, Schema Registry, Control Center, Airflow, Postgres |
| Tick ingestion (WebSocket → Kafka) | `airflow/dags/dag_ticks.py` | Works but runs as infinite Airflow task (anti-pattern) |
| Candle ingestion (History → Kafka) | `airflow/dags/dag_candles.py` | Hourly schedule, hardcoded NVDA + fixed dates |
| Ingestion modules (partial) | `ingestion/` | Not wired to Airflow; `service.py` is broken |
| Spark skeleton | `streaming/stream_candle_builder.py` | Incomplete; missing topic arg, no aggregation |
| Sample payloads | `schemas/market_tick.json`, `schemas/market_candle.json` | Examples only, not formal schemas |

## What Does Not Exist Yet

- MinIO, ClickHouse, Spark in Docker Compose
- Standalone tick ingestion service
- Formal versioned schemas
- Storage layer (`storage/` is empty)
- Reconciliation process
- Feature engineering
- Trading, API, dashboard layers
- Tests, CI, linting (Ruff)
- Topic auto-provisioning

## Topic Naming Map (Legacy → Standard)

| Legacy (remove) | Standard (use) |
|-----------------|----------------|
| `market-ticks` | `market.ticks` |
| `market-candle-1m`, `market.candle1m.official` | `market.candles.raw` (with `interval` field in payload) |
| — | `market.candles.calculated` |
| — | `market.candles.reconciled` |
| — | `market.signals` |
| — | `market.errors` |

## File Ownership

| Path | Owner / Purpose |
|------|-----------------|
| `ingestion/yahoo_client.py` | WebSocket client, message normalization |
| `ingestion/ticks.py` | Tick publishing logic (target) |
| `ingestion/candles.py` | Historical candle fetch (target) |
| `ingestion/kafka_utils.py` | Shared Kafka helpers |
| `ingestion/config.py` | Environment-based configuration |
| `entrypoint/tick_service.py` | Standalone tick service entrypoint |
| `airflow/dags/*.py` | Thin orchestration only |
| `streaming/stream_candle_builder.py` | Spark OHLCV aggregation |
| `streaming/reconciliation.py` | Candle comparison |
| `storage/sinks.py` | MinIO + ClickHouse writes |
| `analysis/features.py` | Indicator computation |
| `analysis/data_quality.py` | Validation checks |
| `schemas/*.schema.json` | Versioned data contracts |
| `infra/kafka/init-topics.sh` | Topic bootstrap script |

## Technical Debt

1. **Three parallel tick implementations** — `airflow/dags/dag_ticks.py`, `ingestion/market_ticks.py`, `ingestion/service.py`
2. **Two parallel candle implementations** — `airflow/dags/dag_candles.py`, `ingestion/market_candles.py`
3. **WebSocket in Airflow** — Infinite loop inside a task; should be standalone service
4. **Hardcoded config** — NVDA symbol, fixed date ranges, mixed bootstrap servers (`broker:29092` vs `0.0.0.0:9092`)
5. **No persistent Kafka volumes** — Data lost on container restart
6. **Topics created manually** — No init script
7. **`stream_candle_builder.py`** — Calls `connect_to_kafka(spark)` without topic argument

## Resolved (Post-Implementation)

After completing the current sprint plan:

- [ ] Topic names unified across codebase
- [ ] Ingestion consolidated in `ingestion/`
- [ ] Standalone tick service in Docker Compose
- [ ] Candle DAGs use incremental fetch + correct schedule
- [ ] MinIO + ClickHouse in compose
- [ ] Spark streaming produces candles
- [ ] Reconciliation process operational
- [ ] Data quality + basic features in `analysis/`
