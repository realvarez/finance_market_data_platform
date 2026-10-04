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

---

## ADR-009: ClickHouse Is the Source of Truth for Reconciliation

**Status:** Accepted

**Context:** `streaming/reconciliation.py` compares calculated candles against official candles by consuming the `market.candles.raw` and `market.candles.calculated` topics directly. It opens a `KafkaConsumer` with no consumer group, `auto_offset_reset="latest"`, and `consumer_timeout_ms=10000`, and it drains the raw topic for ten seconds before starting on the calculated topic.

The DAG runs every five minutes. In practice this means each run only observes messages produced during its own short window, and the two windows do not even overlap. Most runs reconcile nothing, publish nothing, and skip the storage sinks entirely. ADR-007 promises downstream consumers a validated dataset; in practice none has ever been produced.

This also contradicts the project constraint "Do not treat Kafka as the permanent database." Kafka here is being used as a positional read cursor over a stream with finite retention — a query that ClickHouse already answers directly.

**Decision:** Reconciliation compares candles as a set over an explicit time window in ClickHouse, not by consuming topic offsets. It reads both sources for `[start, end)`, pairs rows on `(symbol, interval, timestamp)`, and writes results back to ClickHouse and MinIO. Kafka remains the transport that carries events between services; it is not the place reconciliation reads from.

**Rationale:**
- Durable and replayable — any historical window can be re-reconciled at any time, which Kafka offsets cannot offer once retention expires
- Idempotent — re-running over an overlapping window is a no-op rather than duplicate output
- Explicit — the window being reconciled is stated in the function signature and logged, rather than being an emergent property of process timing
- Correct layer — ClickHouse is already the analytical store (ADR-002); comparing sets is what it is for

**Consequences:**
- `_consume_topic` and the `KafkaConsumer` import are removed from `streaming/reconciliation.py`
- Reconciliation still *publishes* to `market.candles.reconciled`; the topic remains part of the contract, it is simply not the read path
- Re-running reconciliation over historical data becomes possible, which the Kafka approach never allowed
- Requires ADR-011 for idempotent storage
- **Blocked on candle persistence.** This decision assumes both series already exist in ClickHouse.
  Today nothing writes them: `sink_candles` has exactly one caller — reconciliation itself — so
  `market_candles` is empty and the comparison would have no inputs. A candle sink service
  (mirroring `storage/tick_sink.py`) must land first. This is Roadmap Phase 5 and the first item in
  the recommended order.

---

## ADR-010: Verification Gates the Trading Stages

**Status:** Accepted

**Context:** The platform reached Milestone D with documentation claiming it ran "end-to-end", while reconciliation had never successfully run. No artifact in the repository could demonstrate that the pipeline worked, so the defect was invisible for months. Four of the nine test files assert on `docker-compose.yml` as a data structure rather than on system behavior, so CI could not have caught it either.

The project's stated north star — build a reliable market-data platform first — was unenforceable, because nothing measured reliability.

**Decision:** A single `verify_pipeline` command must exit 0 before any trading stage begins. No signal engine, backtester, risk engine, paper broker, portfolio, or API work starts until the data platform reports verified coverage over a completed window.

**Rationale:**
- Turns the north star from an aspiration into a check
- One command is cheap to run and cheap to trust; it must not require standing up a monitoring stack
- Strategies built on unvalidated candles produce backtests that cannot be believed, which wastes far more time than the gate costs
- Prevents the exact failure mode observed: work that appears complete because nobody could tell it was not

**Consequences:**
- `verify_pipeline` becomes the acceptance test for Milestone D and every milestone after it
- Every milestone's exit criteria in `OBJECTIVE.md` are expressed in terms `verify_pipeline` can report
- Deferring Prometheus/Grafana is acceptable while a single command provides the same signal more cheaply

---

## ADR-011: Candle Storage Is Idempotent by Key

**Status:** Accepted

**Context:** `market_candles` is a plain `MergeTree` ordered by `(symbol, interval, timestamp)`. Any process that writes the same candle key twice inserts two rows. `storage/clickhouse_client.query_candles` returns rows ordered by timestamp with a `LIMIT`, so duplicates silently crowd out distinct candles in every consumer's input.

Today this is masked because reconciliation rarely writes anything. The moment ADR-009 lands and reconciliation runs on a schedule over an overlapping window, every re-run will duplicate rows, and `analysis/features.py` will read the same candle several times over — corrupting every indicator computed from it.

**Decision:** `market_candles` becomes `ReplacingMergeTree(replaced_at)` ordered by `(symbol, interval, timestamp, source)`, with a `replaced_at DateTime64(3)` version column written on every insert. Queries read with `FINAL`.

**Rationale:**
- Makes re-reconciling an overlapping window idempotent, which ADR-009 depends on
- Corrects open, high, low, close, and volume for a re-reconciled candle rather than appending a second version
- `FINAL` is acceptable at local data volumes and keeps query semantics honest
- Keying on `source` keeps the raw, calculated, and reconciled series of the same candle as distinct, comparable rows — which is the entire point of reconciliation

**Consequences:**
- Existing local data is invalidated by the engine change; `docker compose down -v` for a clean slate
- `query_candles` and new windowed queries must use `FINAL`
- The same treatment applies to `market_ticks` if duplicate-event handling is added (see ROADMAP Phase 6)
