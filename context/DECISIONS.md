# Architecture Decision Records

## ADR-001: Kafka as Central Event Bus

**Status:** Accepted

**Context:** Market data flows from multiple sources (WebSocket ticks, REST candles) to multiple consumers (Spark, ClickHouse, MinIO, reconciliation).

**Decision:** Use Apache Kafka as the central event-streaming platform.

**Rationale:**
- Decouples producers from consumers
- Supports replay for backtesting and debugging
- Natural fit for real-time + batch processing
- Maps to Amazon MSK or Confluent Cloud in production

**Consequences:** All services communicate via well-defined topics. Schema Registry available for future Avro adoption.

---

## ADR-002: ClickHouse + MinIO Dual Storage

**Status:** Accepted

**Context:** Need both fast analytical queries (dashboards, indicators) and durable cheap storage (backtesting, replay).

**Decision:**
- **ClickHouse** — Analytical database for recent/historical queries, indicators, signals
- **MinIO** — S3-compatible data lake for raw Parquet files with Hive-style partitioning

**Rationale:**
- ClickHouse excels at time-series aggregations
- MinIO provides cloud-portable object storage (maps to AWS S3)
- Parquet is columnar and efficient for historical replay

**Consequences:** Dual-write from streaming/reconciliation sinks. Partition layout must be consistent.

---

## ADR-003: Standalone Tick Service (Not Airflow Task)

**Status:** Accepted

**Context:** Yahoo Finance WebSocket is a long-running connection. Running it as an Airflow task blocks a worker indefinitely.

**Decision:** Run tick ingestion as a standalone Docker Compose service (`entrypoint/tick_service.py`).

**Rationale:**
- Airflow is for scheduled, finite workflows
- WebSocket needs reconnect logic, heartbeat, graceful shutdown
- Independent scaling and restart without affecting orchestration

**Consequences:** Remove `dag_ticks.py` infinite task. Airflow only orchestrates candle fetch, reconciliation, and batch jobs.

---

## ADR-004: JSON Schema for Data Contracts

**Status:** Accepted

**Context:** Need shared contracts between ingestion, streaming, storage, and analysis layers.

**Decision:** Use JSON Schema (`.schema.json`) files in `schemas/v1/` with semantic versioning.

**Rationale:**
- Human-readable and tooling-friendly
- Python validation via `jsonschema` library
- Schema Registry available for future Avro migration if needed
- Lower barrier than Avro for early development

**Consequences:** All producers validate before publish; consumers validate on read. Sample payloads kept separate from schemas.

---

## ADR-005: Thin Airflow DAGs

**Status:** Accepted

**Context:** Business logic was embedded directly in DAG files, causing duplication with `ingestion/` modules.

**Decision:** DAG files only handle scheduling, retries, and calling functions from `ingestion/`, `streaming/`, `analysis/`.

**Rationale:**
- Testable business logic outside Airflow context
- Reusable from CLI, tests, and other orchestrators
- Clear separation of concerns

**Consequences:** Mount or install project code into Airflow container. DAGs import from `ingestion.candles`, not inline yfinance calls.

---

## ADR-006: Event-Time Candle Aggregation

**Status:** Accepted

**Context:** Spark can aggregate candles using processing time or event time.

**Decision:** Use event time with watermarks for candle generation.

**Rationale:**
- Matches when market events actually occurred
- Handles late-arriving ticks correctly
- Backtesting replay produces same candles as live system

**Consequences:** Requires accurate timestamps in MarketTick schema. Watermark delay configurable (default 10s).

---

## ADR-007: Reconciliation Before Analytics

**Status:** Accepted

**Context:** Calculated candles (from ticks) may differ from Yahoo official candles due to gaps, delays, or data quality issues.

**Decision:** Reconciliation compares calculated vs raw candles and publishes validated results to `market.candles.reconciled`. Downstream analytics consume reconciled data.

**Rationale:**
- Single source of truth for features and strategies
- Detects and documents data quality issues
- Allows preferring Yahoo official values when discrepancies exceed tolerance

**Consequences:** Feature engineering and signal engine read from reconciled topic / ClickHouse `source='reconciled'`.

---

## ADR-008: Rule-Based Strategies Before ML

**Status:** Accepted

**Context:** Trading bot needs signals. ML adds complexity and overfitting risk early on.

**Decision:** Implement deterministic rule-based strategies (EMA crossover, momentum, VWAP reversion) before any ML models.

**Rationale:**
- Easier to debug and validate
- Backtesting is straightforward
- ML (Phase 24) requires working infrastructure and measurable prediction targets

**Consequences:** Signal engine in `trading/` uses pure Python rules. MLflow introduced only in Phase 24.
