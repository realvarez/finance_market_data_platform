# Agent Instructions & Guidelines

Welcome! This document provides guidelines, constraints, style standards, and architectural context for AI coding agents working on the **Market Platform** project. Please adhere to these instructions when proposing code modifications, writing scripts, or designing new features.

---

## 1. Project Overview & Objective

The **Market Platform** is an end-to-end market data and trading system. It ingests real-time and historical market data (e.g., Yahoo Finance ticks and candles), processes the data streams using Apache Kafka and Apache Spark, persists it in a dual-storage setup (MinIO and ClickHouse), and provides features/signals to a downstream rule-based paper trading bot.

Refer to the primary documents for details:
- [README.md](/README.md) — Quick Start, URLs, and general structure.
- [ARCHITECTURE.md](/context/ARCHITECTURE.md) — Detailed high-level data flow and system diagrams.
- [CURRENT_STATE.md](/context/CURRENT_STATE.md) — What works, current debt, and legacy-to-standard mapping.
- [ROADMAP.md](/context/ROADMAP.md) — Milestones and recommended implementation steps.
- [DECISIONS.md](/context/DECISIONS.md) — Architecture Decision Records (ADRs).

---

## 2. Core Constraints & Architectural Rules

Any agent modifying code in this workspace must abide by the following Architecture Decision Records (ADRs):

### ADR-001 & ADR-002: Central Event Bus & Dual-Storage Sink
*   **Kafka:** All services communicate via Kafka topics. Use internal bootstrap server `broker:29092` within Docker, and `localhost:9092` for host-level scripts/tests.
*   **Dual Storage:** Persist raw stream events as columnar Parquet files in **MinIO** (partitioned by `symbol/year/month/day`), and write calculated/reconciled time series to **ClickHouse** for analytical query performance.

### ADR-005: Thin DAGs & No Business Logic in Airflow
*   **Constraint:** Airflow DAGs must only handle scheduling, task orchestration, dependencies, and configuration parameters.
*   **Rule:** **Zero business logic inside DAG files.** All processing, network client fetching, validation, and math must live in importable Python modules (e.g., [ingestion/candles.py](/ingestion/candles.py), [analysis/data_quality.py](/analysis/data_quality.py)).
*   **DAG Directory:** [airflow/dags/](/airflow/dags/)

### ADR-003: Long-running Services as Standalone Containers
*   **Constraint:** Real-time stream listeners or WebSocket consumers (like the tick service) must not run as infinite Airflow tasks. This blocks Airflow executors and hinders scaling.
*   **Rule:** Run long-running ingestion/streaming processes as standalone services within Docker Compose (e.g. entrypoint scripts in [entrypoint/](/entrypoint/)).

### ADR-004: Schema Validation (Data Contracts)
*   **Constraint:** All components must adhere to versioned, strict JSON schemas.
*   **Rule:** Validate every outbound event payload against schemas in [schemas/v1/](/schemas/v1/) before publishing to Kafka. Use the utility validators in `ingestion.schema_validator` to raise validation errors early.

### ADR-007: Reconciliation & Feature Engineering
*   **Constraint:** Calculated candles (aggregated from ticks) may differ from official historical candles.
*   **Rule:** Downstream signal and feature engineering engines must read from the reconciled candle topic (`market.candles.reconciled`) or ClickHouse rows where `source='reconciled'`. Never compute features directly from raw/unreconciled candle streams.

---

## 3. Directory Layout & File Ownership

Do not place code files in arbitrary folders. Follow the directory layout conventions:

| Directory Path | Purpose / Responsibility |
|:---|:---|
| [ingestion/](/ingestion/) | Clients for external data APIs, message normalization, schema validation, and Kafka producers. |
| [streaming/](/streaming/) | Spark Structured Streaming pipelines (e.g., tick aggregation) and reconciliation jobs. |
| [storage/](/storage/) | Clients and writing logic for ClickHouse and MinIO. |
| [analysis/](/analysis/) | Feature engineering logic, mathematical indicators, and data quality check suites. |
| [entrypoint/](/entrypoint/) | Executable python scripts designed to run as standalone system entrypoints (e.g. tick ingestion daemon). |
| [airflow/dags/](/airflow/dags/) | Contains thin orchestration definitions. |
| [schemas/v1/](/schemas/v1/) | Strictly-versioned JSON schemas specifying data contracts. |
| [infra/](/infra/) | Database DDL initialization scripts, Kafka bootstrap scripts, and configs. |
| [tests/](/tests/) | Pytest tests for schemas, logic, and integrations. |

---

## 4. Coding Conventions & Best Practices

1.  **Configuration Management:**
    *   Do not hardcode API keys, symbol lists, or broker endpoints.
    *   Use [ingestion/config.py](/ingestion/config.py) to read environments and fall back to standard defaults.
2.  **Topic Naming Standard:**
    *   Always use unified topic names:
        *   `market.ticks` - Real-time ticks.
        *   `market.candles.raw` - Raw official API candles.
        *   `market.candles.calculated` - Spark calculated candles.
        *   `market.candles.reconciled` - Reconciled/cleaned candles.
        *   `market.signals` - Output trading signals.
        *   `market.errors` - Malformed message routing and system error logs.
3.  **Timestamp Convention:**
    *   All timestamps in payloads must use ISO 8601 formatted strings with the UTC timezone indicator (e.g. `2026-08-12T14:30:00Z`).
4.  **Error Handling & Routing:**
    *   When validation fails, log details and route the malformed payload to the `market.errors` Kafka topic rather than failing silently or printing only to standard error.
5.  **Linting & Style:**
    *   Format and check all Python changes before committing:
        ```bash
        uv run ruff check .
        uv run ruff format .
        ```
6.  **Dependency Control:**
    *   All dependencies are managed via `uv`. Use `uv add <package>` or modify `pyproject.toml` and run `uv sync` to lock dependencies. The legacy `requirements.txt` has been removed; Docker images install from `pyproject.toml`/`uv.lock`.

---

## 5. Development Runbook for Agents

### Docker Compose Stack Management
To start all background infrastructure (Kafka, ClickHouse, MinIO, Spark, Airflow):
```bash
docker compose up -d
```
Verify health:
```bash
docker compose ps
```

### Kafka Topic Operations
List existing topics:
```bash
docker compose exec broker kafka-topics --list --bootstrap-server localhost:9092
```
Manually trigger topic bootstrap:
```bash
docker compose exec broker bash /infra/kafka/init-topics.sh
```

### Running Tests
Always verify code functionality by running pytest:
```bash
uv run pytest tests/ -v
```

### Consuming Ticks for Verification
Verify that messages are reaching a topic:
```bash
docker compose exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic market.ticks \
  --from-beginning \
  --max-messages 5
```
