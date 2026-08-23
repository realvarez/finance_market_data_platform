# Current State

Last updated: August 2026

## Summary

The data platform (Milestones A–D, ~Phases 1–13) is implemented and running end-to-end on paper:
ingestion → Kafka → Spark candles → reconciliation → ClickHouse/MinIO → features. Milestone E
(Analytics & Trading: signals → backtesting → risk → paper trading → portfolio → API → dashboard)
has not started. dbt (P12), broker integration (P20), ML (P24), and cloud (P26) are intentionally
deferred.

## What Works

| Component | Location | Notes |
|-----------|----------|-------|
| Docker Compose stack | `docker-compose.yml` | Kafka (KRaft), Schema Registry, Control Center, MinIO, ClickHouse, Spark master/worker, tick-ingestion, Postgres + Airflow |
| Topic auto-provisioning | `infra/kafka/init-topics.sh` via `kafka-init` service | All 6 standard topics |
| Standalone tick service | `entrypoint/tick_service.py`, `ingestion/ticks.py` | WebSocket → validate → `market.ticks`; graceful SIGTERM shutdown |
| Candle ingestion DAGs | `airflow/dags/dag_candles.py` → `ingestion/candles.py` | Incremental fetch with overlap, post-close schedules |
| Spark OHLCV aggregation | `streaming/stream_candle_builder.py` | Event-time windows + watermarks → `market.candles.calculated` (manual spark-submit; see Debt #2) |
| Reconciliation | `streaming/reconciliation.py` via `dag_reconciliation.py` | Compare raw vs calculated → `market.candles.reconciled` → ClickHouse/MinIO |
| Data quality checks | `analysis/data_quality.py` via `dag_data_quality.py` | Completeness, uniqueness, OHLC validity, freshness, consistency |
| Feature engineering | `analysis/features.py` | Returns, candle anatomy, EMA 9/21, RSI 14 (Wilder), rolling std → ClickHouse `market_features` |
| Storage layer | `storage/minio_client.py`, `storage/clickhouse_client.py`, `storage/sinks.py` | Parquet partitioned `symbol/year/month/day`; MergeTree tables |
| Versioned schemas | `schemas/v1/*.schema.json` | Tick, candle, signal, order, trade, position (Draft 2020-12) |
| Unit tests | `tests/` | Features math, schema contracts, reconciliation logic (48 tests) |

## Next Steps (approved plan)

Staged path to paper trading + dashboard. Each stage ends with tests + commit.

1. **Stage 0 — Stabilize** *(in progress)*: snapshot commit, test suite, dependency-drift fixes,
   doc corrections, port conflict fixes.
2. **Stage 1 — Close data-layer gaps**: finish Phase 13 features (ATR, relative volume, VWAP,
   SPY/QQQ context) + `query_features()`; continuous Kafka→MinIO tick sink service; wire Spark
   candle builder as a compose service; ClickHouse DDL for signals/orders/trades/positions;
   historical backfill DAG + Airflow Variables for symbols.
3. **Stage 2 — Signal engine (Phase 14)**: new `trading/` package per ADR-008 — strategy interface,
   EMA-crossover and momentum-breakout strategies, standalone signal-engine service publishing
   validated MarketSignals to `market.signals`.
4. **Stage 3 — Backtesting (Phases 15–16)**: event-driven replay of historical data using the same
   strategy interface; strict look-ahead-bias prevention; return/Sharpe/drawdown/win-rate metrics;
   CLI entrypoint.
5. **Stage 4 — Risk + paper trading + portfolio (Phases 17–19)**: position sizing and exposure
   limits, paper broker simulating fills at next candle open, portfolio tracking with PnL persisted
   to ClickHouse.
6. **Stage 5 — API + dashboard (Phases 21–22)**: FastAPI read layer over ClickHouse; Streamlit
   dashboard (candles, signals overlay, positions, equity curve).
7. **Stage 6 — Ops hardening**: structured logging/counters, healthchecks, GitHub Actions CI
   (ruff + pytest), final docs pass.

Out of scope for this plan: dbt (P12), live broker (P20), ML (P24), cloud deployment (P26).

## Technical Debt

1. **No continuous Kafka→MinIO tick sink** — ticks reach MinIO only via sinks called by other jobs;
   backtest replay needs the raw tick archive (ADR-002). Planned Stage 1.
2. **Spark candle builder requires manual spark-submit** — master/worker sit idle unless run by hand.
   Planned Stage 1.
3. **Dev credentials committed** — minioadmin/minioadmin, postgres airflow/airflow, Airflow secret
   key `'developer-secret-key'`. Acceptable locally; must rotate before any shared/cloud deploy.
4. **Reconnect uses fixed 5s sleep** — roadmap calls for exponential backoff (`ingestion/ticks.py`).
5. **No Kafka integration tests** — unit tests cover pure logic only; produce/consume paths are
   untested against a live broker.

## Resolved (Post-Implementation)

- [x] Topic names unified across codebase
- [x] Ingestion consolidated in `ingestion/`
- [x] Standalone tick service in Docker Compose (ADR-003)
- [x] Candle DAGs use incremental fetch + correct schedule
- [x] MinIO + ClickHouse in compose
- [x] Spark streaming produces candles
- [x] Reconciliation process operational
- [x] Data quality + basic features in `analysis/`
- [x] Legacy parallel implementations removed (`dag_ticks.py`, flat schemas)
- [x] Unit test suite + Ruff clean
