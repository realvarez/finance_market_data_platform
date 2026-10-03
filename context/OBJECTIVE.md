# Project Objective

## Goal

Build an end-to-end market data and trading platform that ingests market information from Yahoo Finance, processes it through a streaming pipeline, and feeds a bot capable of making trading decisions.

## Core Capabilities

1. **Ingestion** — Real-time ticks via WebSocket and historical candles via REST API
2. **Streaming** — Kafka as the central event bus; Spark Structured Streaming for candle generation
3. **Storage** — MinIO (S3-compatible data lake) for raw/historical Parquet; ClickHouse for analytical queries
4. **Reconciliation** — Compare calculated vs official candles; detect gaps and inconsistencies
5. **Analytics** — Feature engineering (returns, EMA, RSI, MACD, ATR, VWAP, etc.)
6. **Trading** — Rule-based signals, risk management, paper trading, eventual live broker integration
7. **Orchestration** — Airflow for scheduled/finite workflows; standalone services for long-running processes

## Constraints

- Local development runs entirely in Docker Compose
- Airflow DAGs must be thin orchestration layers — business logic lives in Python modules
- Real-time WebSocket ingestion must not run as an infinite Airflow task
- No trading logic in ingestion services
- Rule-based strategies before machine learning
- Backtesting must mirror live system behavior (no look-ahead bias)

## Success Criteria

Criteria are expressed as measurements, not intentions. Each one is something `verify_pipeline` can
report, which is what keeps this table honest
([ADR-010](DECISIONS.md#adr-010-verification-gates-the-trading-stages)).

| Milestone | Criteria | Status |
|-----------|----------|--------|
| A — Foundation | `docker compose up` works; topics auto-created; schemas validated | Met |
| B — Ingestion | Ticks flow via standalone service; candles fetched incrementally; DAGs contain no business logic | Met |
| C — Storage & Streaming | Spark produces 1m/5m candles; data in MinIO and ClickHouse | Met, except event-time correctness of open/close (Phase 6) |
| D — Data Quality | Reconciled coverage ≥ 99% of official candles over one completed session; zero duplicate candle keys; OHLC validity violations = 0; freshness ≤ 2 min during market hours; p95 ingestion latency ≤ 5 s | **Not met** — reconciliation does not run |
| E — Trading | Backtest runs against reconciled data with look-ahead guard active; paper bot generates signals and simulates trades; `Strategy → Signal → Risk → Order` enforced in code | Blocked by D |
| F — Ops | CI passes on every push; pipeline health observable without manual inspection | Not started |

## The Reliability Bar

The first objective is not a trading signal — it is a market-data platform that can demonstrably
collect, process, store, validate, and replay market data correctly. "Correctly" means the
following, measured over a completed trading session:

| Property | Threshold | Why it matters |
|----------|-----------|----------------|
| **Completeness** | ≥ 99% of official candles present in the reconciled set | A strategy trained on gaps learns the gaps |
| **Uniqueness** | Zero duplicate `(symbol, interval, timestamp)` keys | Duplicates corrupt every downstream indicator |
| **OHLC validity** | Zero candles where `low > min(open, close)` or `high < max(open, close)` or `volume < 0` | A candle that violates its own bounds is a processing bug, not a market event |
| **Consistency** | Every calculated candle matched or explicitly corrected against the official value | Silent divergence is worse than a detected one |
| **Freshness** | Latest tick ≤ 2 min old during market hours | Stale data produces confident, wrong signals |
| **Latency** | p95 of `ingestion_timestamp − market timestamp` ≤ 5 s | Bounds what "real-time" actually means here |
| **Replayability** | Any historical window can be re-derived from stored raw ticks | The precondition for trustworthy backtesting |

## Definition of Done

A milestone is not complete because the code was written or because it appears in the compose file.
It is complete when `verify_pipeline` exits 0 and reports the criteria above as met, against a real
running stack and a real completed market session.

This definition exists because the platform previously reached Milestone D with reconciliation
silently non-functional, and no artifact in the repository was capable of noticing. See
[CURRENT_STATE.md](CURRENT_STATE.md) for that account and
[ROADMAP.md](ROADMAP.md) for the verification gate that replaces it.

## Out of Scope (Initial Phases)

- dbt transformation layer (Phase 12) — the analytical need is not complex enough to justify it yet
- Prometheus/Grafana monitoring stack (Phase 23) — a single `verify_pipeline` command answers the same
  question more cheaply until trading is live
- Live broker integration (Phase 20)
- Machine learning models (Phase 24)
- Cloud deployment (Phase 26)
