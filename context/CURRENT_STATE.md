# Current State

Last updated: October 2026

## Summary

The pipeline now runs end to end **through storage**: Yahoo Finance ticks flow into `market.ticks`,
Spark aggregates them into 1-minute and 5-minute calculated candles on `market.candles.calculated`,
and Yahoo's official candles reach `market.candles.raw` on schedule. A `candle-sink` service
consumes both candle topics and persists them to ClickHouse and MinIO. Calculated candles are landing
in ClickHouse under `source='spark_streaming'` and in MinIO under `calculated/candles/`.

**What is still missing is validation, not data.** Reconciliation does not run, so nothing compares
the calculated candles against Yahoo's. Feature engineering therefore still finds no reconciled
candles and silently falls back to raw Yahoo ones, which means every indicator it produces is
computed from unvalidated data.

- **Feature engineering produces values, but unvalidated ones.** `analysis/features.py` reads
  reconciled candles, finds none, and falls back to `source='yahoo_finance'` — silently. Features
  are only as trustworthy as the reconciliation that never happened.
- **Data quality checks still run against the raw fallback.** `analysis/data_quality.py` queries the
  same way, so its verdicts describe unreconciled candles and go to a log nobody reads.

Reconciliation now works — it compares both series in ClickHouse over an explicit window and was
verified producing all four statuses (`matched`, `corrected`, `raw_only`, `calculated_only`) against
the live stack. What remains is instrumentation: nothing measures the coverage that reconciliation
reports. Earlier revisions of
this document claimed the whole pipeline ran "end-to-end" — that was wrong, and it persisted because
nothing in the repository could check it. There is still no integration test, no CI, and no health
check. The corrective work is gated by
[ADR-010](DECISIONS.md#adr-010-verification-gates-the-trading-stages).

## What Works

| Component | Location | Notes |
|-----------|----------|-------|
| Docker Compose stack | `docker-compose.yml` | Kafka (KRaft), MinIO, ClickHouse, Spark `local[2]`, tick-ingestion, tick-sink, candle-sink, Postgres + Airflow. Cgroup-bounded to ~3.3 GB |
| Topic auto-provisioning | `infra/kafka/init-topics.sh` via `kafka-init` | All 6 standard topics |
| Standalone tick service | `entrypoint/tick_service.py`, `ingestion/ticks.py` | WebSocket → validate → `market.ticks`; graceful SIGTERM shutdown |
| Historical candle DAGs | `airflow/dags/dag_candles.py` → `ingestion/candles.py` | Incremental fetch with overlap, post-close schedules. Created paused in compose — unpause in the Airflow UI to get `yahoo_finance` candles |
| Backfill DAG | `airflow/dags/dag_backfill.py` | Chunked historical range fetch, manual trigger |
| Spark OHLCV aggregation | `streaming/stream_candle_builder.py` as `candle-builder` service | Event-time windows + watermark → `market.candles.calculated` |
| Continuous tick sink | `storage/tick_sink.py` as `tick-sink` service | `market.ticks` → MinIO Parquet + ClickHouse, batched, with backoff |
| Continuous candle sink | `storage/candle_sink.py` as `candle-sink` service | `market.candles.raw` + `.calculated` → ClickHouse + MinIO, routed per topic, validated against the candle schema with failures routed to `market.errors` |
| Storage layer | `storage/minio_client.py`, `storage/clickhouse_client.py`, `storage/sinks.py` | Parquet and ClickHouse writers exist and work, but only `tick-sink` and reconciliation call them |
| Data quality checks | `analysis/data_quality.py` via `dag_data_quality.py` | Completeness, uniqueness, OHLC validity, freshness, consistency |
| Feature engineering | `analysis/features.py` | Returns, candle anatomy, EMA 9/21, RSI 14, ATR 14, rolling std, relative volume, VWAP, SPY/QQQ context |
| Versioned schemas | `schemas/v1/*.schema.json` | Six Draft 2020-12 schemas, all `additionalProperties: false` |
| Unit tests | `tests/` | 70 passing |

## What Does Not Work

| Component | Location | Status |
|-----------|----------|--------|
| Reconciled data consumption | `analysis/features.py` | **Silently degraded** — see Debt #1 |
| Verification | — | **Absent** — no `verify_pipeline`, no integration tests, no CI, no metrics |
| Signal engine | — | Not started. Schema, ClickHouse table, and `sink_signals` exist with no producer |
| Backtesting | — | Not started |
| Risk engine | — | Not started. Schema and table only |
| Paper broker / portfolio | — | Not started. Tables only |
| FastAPI / Streamlit / monitoring | — | Not started |

## Technical Debt

Ranked by impact on the project's stated objective.

1. **The feature layer still falls back to unreconciled candles silently.**
   `analysis/features.py:15` prefers `source='reconciled'` and falls back to `source='yahoo_finance'`
   when reconciled returns empty. Reconciliation now produces rows, but the fallback should log
   loudly so a regression is visible rather than silent. The fallback itself is reasonable
   (reconciliation legitimately has not run yet on a fresh stack); failing quietly is not.

2. **Spark has no duplicate-event or late-event handling.** `stream_candle_builder.py` sets a 10-second
   watermark and drops anything later, which is a defensible policy but an undocumented and untested
   one. `first`/`last` resolve by *ingestion* order rather than event-time order, so `open` and `close`
   can be wrong under shuffle. Neither duplicate nor late-event behavior has a test.

3. **Reconnect uses a fixed 5-second sleep.** `ingestion/ticks.py:40` wraps the WebSocket in a retry
   loop with a constant delay; the sinks implement exponential backoff, so the services behave
   differently under the same broker failure.

4. **Data-quality metrics are never emitted.** `analysis/data_quality.py` computes checks and logs
   them. Nothing is trended, so a slowly degrading pipeline is indistinguishable from a healthy one.

5. **No integration tests and no CI.** All tests are unit tests over pure logic, plus one that skips
   without a live ClickHouse. Four test files (`test_compose_*.py`, `test_config_validation.py`)
   assert on `docker-compose.yml` as a data structure — `mem_limit == "1280m"` — which locks
   configuration values without testing behavior and makes compose edits brittle. No GitHub Actions
   workflow exists.

6. **Dev credentials are committed.** `minioadmin/minioadmin`, `postgres airflow/airflow`, Airflow
   secret key `'developer-secret-key'`. `.env` parameterization landed (commit `e3236e7`) but the
   defaults are still weak. Acceptable locally; must rotate before any shared or cloud deployment.

7. **Market-context features are empty by decision.** `analysis/features.py:97` reads SPY and QQQ,
   which are not in the configured symbol list. `compute_market_context` therefore returns nulls.
   This is expected until those symbols are subscribed — not a regression.

## Resolved

- [x] Topic names unified across codebase
- [x] Ingestion consolidated in `ingestion/`
- [x] Standalone tick service in Docker Compose ([ADR-003](DECISIONS.md#adr-003-standalone-tick-service-not-airflow-task))
- [x] Candle DAGs use incremental fetch + correct post-close schedule
- [x] MinIO + ClickHouse in compose ([ADR-002](DECISIONS.md#adr-002-clickhouse--minio-dual-storage))
- [x] Spark streaming produces calculated candles as a compose service (commit `a70415a`) — previously listed as debt
- [x] Continuous Kafka→MinIO/ClickHouse tick sink as a compose service (commit `671eb41`) — previously listed as debt
- [x] **Reconciliation rewritten against ClickHouse** ([ADR-009](DECISIONS.md#adr-009-clickhouse-is-the-source-of-truth-for-reconciliation)) — compares both series as a set over an explicit window; Kafka is no longer the read path. Verified producing `matched`, `corrected`, `raw_only` and `calculated_only` against the live stack, and idempotent across repeated runs over an overlapping window
- [x] **Absolute price tolerance in reconciliation** — was proportional, which accepted a $6.00 error on a $600 stock
- [x] **Continuous Kafka→MinIO/ClickHouse candle sink** for `market.candles.raw` and `.calculated`, routed per topic and schema-validated with failures routed to `market.errors`
- [x] **Idempotent candle storage** — `market_candles` is `ReplacingMergeTree(created_at)` ordered by `(symbol, interval, timestamp, source)`, read with `FINAL` ([ADR-011](DECISIONS.md#adr-011-candle-storage-is-idempotent-by-key))
- [x] ClickHouse made usable — the 512MB internal cap sat below the server's own baseline RSS and it could not answer even `SELECT 1`
- [x] Python services mount the source, so `docker compose up` no longer runs stale images
- [x] Complete Phase 13 feature set (ATR, relative volume, VWAP, market context)
- [x] ClickHouse DDL for signals/orders/trades/positions
- [x] Historical backfill DAG and Airflow Variables for symbols
- [x] Legacy parallel implementations removed (`dag_ticks.py`, flat schemas)
- [x] Unit test suite, Ruff clean
- [x] Compose resource budget reduced from ~8.5 GB to ~3.3 GB

## Next Steps

**Gate — must pass before any trading work.** ([ADR-010](DECISIONS.md#adr-010-verification-gates-the-trading-stages))

1. Rewrite reconciliation to compare candles in ClickHouse over an explicit time window ([ADR-009](DECISIONS.md#adr-009-clickhouse-is-the-source-of-truth-for-reconciliation)). Both series are now stored, so the comparison finally has real inputs.
2. Correct the price tolerance to absolute.
3. Build `verify_pipeline` — one command that reports tick arrival, calculated/raw/reconciled coverage,
   duplicate keys, OHLC violations, ingestion latency, and feature freshness, exiting non-zero on failure.
4. Make the feature-layer fallback loud.
5. Prove it: with the stack running, `verify_pipeline` exits 0 and reconciled coverage sits near 1.0 for
   a completed window.

> **Operational note:** the Airflow DAGs are created paused
> (`DAGS_ARE_PAUSED_AT_CREATION=true`), so `market.candles.raw` stays empty until they are unpaused in
> the Airflow UI. Until then only `spark_streaming` candles exist, and BTC-USD is the only subscribed
> symbol producing ticks outside US market hours.

**Only after the gate passes:**

- Stage 2 — signal engine (Phase 14): `trading/` package, strategy interface, EMA-crossover and
  momentum-breakout strategies, standalone service publishing validated signals to `market.signals`.
- Stage 3 — backtesting (Phases 15–16): event-driven replay through the same strategy interface,
  look-ahead-bias prevention, return/Sharpe/drawdown/win-rate metrics.
- Stage 4 — risk engine, paper trading, portfolio (Phases 17–19).
- Stage 5 — API and dashboard (Phases 21–22).
- Stage 6 — ops hardening: structured logging, healthchecks, GitHub Actions CI.

**Deferred, by decision:** dbt (Phase 12), Prometheus/Grafana (Phase 23), live broker (Phase 20),
ML (Phase 24), cloud deployment (Phase 26). The metrics tables and integration tests rejected in the
October 2026 architecture review are recorded here rather than lost — revisit once the platform is
trustworthy and trading begins.