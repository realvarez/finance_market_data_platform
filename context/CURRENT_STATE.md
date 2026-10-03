# Current State

Last updated: October 2026

## Summary

The ingestion and streaming path works *up to Kafka*: Yahoo Finance ticks flow into `market.ticks`,
and Spark aggregates them into 1-minute and 5-minute calculated candles on `market.candles.calculated`.
Yahoo's official candles reach `market.candles.raw` on schedule.

**The path ends there.** Nothing consumes either candle topic to persist it. `storage/sinks.py` exposes
`sink_candles`, but its only caller is the reconciliation job writing reconciled rows — so
`market_candles` in ClickHouse is empty, and no candle Parquet has ever been written to MinIO.

Three consequences follow, and they are worse than "the data is unvalidated":

- **Feature engineering has never run against real candle data.** `analysis/features.py` reads
  reconciled candles, falls back to raw Yahoo candles, and finds neither — because neither is stored.
  Every indicator it has produced is null. The fallback is silent, so this was invisible.
- **Data quality checks run against an empty table.** `analysis/data_quality.py` queries reconciled,
  falls back to Yahoo, gets nothing, and reports `completeness: failed` — correctly, but into a log
  nobody reads, with no alerting.
- **Reconciliation has no input to compare.** It consumes the two topics, finds nothing on a 5-minute
  schedule, and publishes nothing.

So the platform is Kafka-complete and storage-empty. Milestone C (storage) and Milestone D (data
quality) are both effectively unbuilt, despite the documentation having described them as delivered.

Earlier revisions of this document claimed the platform ran "end-to-end". That claim was wrong, and
it persisted because nothing in the repository could check it. There is no integration test, no CI, no
health check, and no metric that would have surfaced any of this. The corrective work is gated by
[ADR-010](DECISIONS.md#adr-010-verification-gates-the-trading-stages).

## What Works

| Component | Location | Notes |
|-----------|----------|-------|
| Docker Compose stack | `docker-compose.yml` | Kafka (KRaft), MinIO, ClickHouse, Spark `local[2]`, tick-ingestion, tick-sink, Postgres + Airflow. Cgroup-bounded to ~3.2 GB |
| Topic auto-provisioning | `infra/kafka/init-topics.sh` via `kafka-init` | All 6 standard topics |
| Standalone tick service | `entrypoint/tick_service.py`, `ingestion/ticks.py` | WebSocket → validate → `market.ticks`; graceful SIGTERM shutdown |
| Historical candle DAGs | `airflow/dags/dag_candles.py` → `ingestion/candles.py` | Incremental fetch with overlap, post-close schedules. **Kafka only — result is never persisted** |
| Backfill DAG | `airflow/dags/dag_backfill.py` | Chunked historical range fetch, manual trigger. **Kafka only** |
| Spark OHLCV aggregation | `streaming/stream_candle_builder.py` as `candle-builder` service | Event-time windows + watermark → `market.candles.calculated`. **Kafka only** |
| Continuous tick sink | `storage/tick_sink.py` as `tick-sink` service | `market.ticks` → MinIO Parquet + ClickHouse, batched, with backoff |
| Storage layer | `storage/minio_client.py`, `storage/clickhouse_client.py`, `storage/sinks.py` | Parquet and ClickHouse writers exist and work, but only `tick-sink` and reconciliation call them |
| Data quality checks | `analysis/data_quality.py` via `dag_data_quality.py` | Completeness, uniqueness, OHLC validity, freshness, consistency |
| Feature engineering | `analysis/features.py` | Returns, candle anatomy, EMA 9/21, RSI 14, ATR 14, rolling std, relative volume, VWAP, SPY/QQQ context |
| Versioned schemas | `schemas/v1/*.schema.json` | Six Draft 2020-12 schemas, all `additionalProperties: false` |
| Unit tests | `tests/` | 70 passing |

## What Does Not Work

| Component | Location | Status |
|-----------|----------|--------|
| **Candle persistence** | `storage/sinks.py`, no caller | **Missing entirely** — see Debt #1. No candle sink service exists; ClickHouse `market_candles` is empty and MinIO has no candle Parquet |
| Reconciliation | `streaming/reconciliation.py` | **Broken** — see Debt #2. Rewrite specified by [ADR-009](DECISIONS.md#adr-009-clickhouse-is-the-source-of-truth-for-reconciliation) |
| Reconciled data consumption | `analysis/features.py` | **Silently degraded** — see Debt #3. ADR-007 is not satisfied in practice |
| Verification | — | **Absent** — no `verify_pipeline`, no integration tests, no CI, no metrics |
| Signal engine | — | Not started. Schema, ClickHouse table, and `sink_signals` exist with no producer |
| Backtesting | — | Not started |
| Risk engine | — | Not started. Schema and table only |
| Paper broker / portfolio | — | Not started. Tables only |
| FastAPI / Streamlit / monitoring | — | Not started |

## Technical Debt

Ranked by impact on the project's stated objective.

1. **Candles are never persisted.** `storage/sinks.py:21` defines `sink_candles`, and
   `storage/clickhouse_client.py:81` defines `insert_candles`, but the only call site of either is
   `streaming/reconciliation.py:105` — writing reconciled rows. No service consumes
   `market.candles.raw` or `market.candles.calculated` to write storage. `tick-sink` does this for
   `market.ticks`; the equivalent candle sink was never built.

   Result: `market_candles` in ClickHouse is empty, no candle Parquet exists in MinIO, feature
   engineering reads an empty table and yields nulls, and reconciliation has nothing to compare.
   This blocks [ADR-009](DECISIONS.md#adr-009-clickhouse-is-the-source-of-truth-for-reconciliation),
   which assumes both series are already stored.

2. **Reconciliation reads Kafka offsets and usually reconciles nothing.** `streaming/reconciliation.py:18`
   opens `KafkaConsumer` with no `group_id`, `auto_offset_reset="latest"`, and
   `consumer_timeout_ms=10000`. On a 5-minute DAG it only observes messages produced during its own
   ~10-second window, and it drains the raw topic for the full 10s before starting on calculated — the
   two windows do not overlap. Most runs reconcile zero candles, publish nothing, and skip the
   ClickHouse and MinIO sinks. This defeats ADR-007 and is the single defect blocking Milestone D.
   Resolution: [ADR-009](DECISIONS.md#adr-009-clickhouse-is-the-source-of-truth-for-reconciliation).

3. **The silent fallback to unreconciled candles masked all of the above.** `analysis/features.py:15`
   prefers `source='reconciled'` and falls back to `source='yahoo_finance'` when reconciled returns empty.
   Because neither source is ever stored (Debt #1), this fallback fired on every run and silently
   produced null features — no error, no alert, and documentation that described the platform as
   working. The fallback itself is reasonable (reconciliation legitimately has not run yet on a fresh
   stack); failing loudly is not.

4. **`market_candles` cannot hold the same candle key twice.** Plain `MergeTree` ordered by
   `(symbol, interval, timestamp)`; `query_candles` uses `LIMIT`, so duplicates crowd out distinct
   candles. Currently harmless because nothing is written. It becomes a data-corrupting bug the
   moment Debt #1 is fixed, since reconciliation re-runs over overlapping windows.
   Resolution: [ADR-011](DECISIONS.md#adr-011-candle-storage-is-idempotent-by-key).

5. **Price tolerance in reconciliation is proportional, not absolute.** `_within_tolerance` computes
   `tolerance * max(|a|, |b|, 1)`, so with `PRICE_TOLERANCE = 0.01` a \$600 stock is accepted within
   **\$6.00** of the official value. A reconciliation that cannot detect a \$5 error is worse than no
   reconciliation, because it reports `matched` and suppresses the alert.

6. **Spark has no duplicate-event or late-event handling.** `stream_candle_builder.py` sets a 10-second
   watermark and drops anything later, which is a defensible policy but an undocumented and untested
   one. `first`/`last` resolve by *ingestion* order rather than event-time order, so `open` and `close`
   can be wrong under shuffle. Neither duplicate nor late-event behavior has a test.

7. **Reconnect uses a fixed 5-second sleep.** `ingestion/ticks.py:40` wraps the WebSocket in a retry
   loop with a constant delay; `storage/tick_sink.py` does implement exponential backoff, so the two
   services behave differently under the same broker failure.

8. **Data-quality metrics are never emitted.** `analysis/data_quality.py` computes checks and logs
   them. Nothing is trended, so a slowly degrading pipeline is indistinguishable from a healthy one.

9. **No integration tests and no CI.** All 70 tests are unit tests over pure logic. Four of the nine
   test files (`test_compose_*.py`, `test_config_validation.py`) assert on `docker-compose.yml` as a
   data structure — `mem_limit == "768m"` — which locks configuration values without testing behavior.
   These would not have caught Debt #1 and make compose edits needlessly brittle. No GitHub Actions
   workflow exists.

10. **Dev credentials are committed.** `minioadmin/minioadmin`, `postgres airflow/airflow`, Airflow
    secret key `'developer-secret-key'`. `.env` parameterization landed (commit `e3236e7`) but the
    defaults are still weak. Acceptable locally; must rotate before any shared or cloud deployment.

## Resolved

- [x] Topic names unified across codebase
- [x] Ingestion consolidated in `ingestion/`
- [x] Standalone tick service in Docker Compose ([ADR-003](DECISIONS.md#adr-003-standalone-tick-service-not-airflow-task))
- [x] Candle DAGs use incremental fetch + correct post-close schedule
- [x] MinIO + ClickHouse in compose ([ADR-002](DECISIONS.md#adr-002-clickhouse--minio-dual-storage))
- [x] Spark streaming produces calculated candles as a compose service (commit `a70415a`) — previously listed as debt
- [x] Continuous Kafka→MinIO/ClickHouse tick sink as a compose service (commit `671eb41`) — previously listed as debt
- [x] Complete Phase 13 feature set (ATR, relative volume, VWAP, SPY/QQQ context)
- [x] ClickHouse DDL for signals/orders/trades/positions
- [x] Historical backfill DAG and Airflow Variables for symbols
- [x] Legacy parallel implementations removed (`dag_ticks.py`, flat schemas)
- [x] Unit test suite, Ruff clean
- [x] Compose resource budget reduced from ~8.5 GB to ~3.2 GB

## Next Steps

**Gate — must pass before any trading work.** ([ADR-010](DECISIONS.md#adr-010-verification-gates-the-trading-stages))

1. Build a **candle sink** service consuming `market.candles.raw` and `market.candles.calculated` into
   MinIO and ClickHouse, mirroring `storage/tick_sink.py`. Without it there is nothing to reconcile.
2. Make `market_candles` idempotent by key ([ADR-011](DECISIONS.md#adr-011-candle-storage-is-idempotent-by-key)).
3. Rewrite reconciliation to compare candles in ClickHouse over an explicit time window ([ADR-009](DECISIONS.md#adr-009-clickhouse-is-the-source-of-truth-for-reconciliation)).
4. Correct the price tolerance to absolute.
5. Build `verify_pipeline` — one command that reports tick arrival, calculated/raw/reconciled coverage,
   duplicate keys, OHLC violations, ingestion latency, and feature freshness, exiting non-zero on failure.
6. Make the feature-layer fallback loud.
7. Prove it: with the stack running, `verify_pipeline` exits 0 and reconciled coverage sits near 1.0 for
   a completed window.

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