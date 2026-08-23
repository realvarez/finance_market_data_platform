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

| Milestone | Criteria |
|-----------|----------|
| A — Foundation | `docker compose up` works; topics auto-created; schemas validated |
| B — Ingestion | Ticks flow via standalone service; candles fetched incrementally; DAGs contain no business logic |
| C — Storage & Streaming | Spark produces 1m/5m candles; data in MinIO and ClickHouse |
| D — Data Quality | Reconciliation detects gaps; reconciled candles queryable |
| E — Trading | Backtest runs; paper bot generates signals and simulates trades |
| F — Ops | CI passes; Grafana shows pipeline health |

## Out of Scope (Initial Phases)

- Live broker integration (Phase 20)
- Machine learning models (Phase 24)
- Cloud deployment (Phase 26)
