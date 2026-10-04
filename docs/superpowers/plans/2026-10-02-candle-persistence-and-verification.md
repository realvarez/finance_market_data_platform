# Candle Persistence & Verification — Implementation Plan

- **Date:** 2026-10-02
- **Status:** Steps 2–4 complete (PRs #2, #3, #4). Steps 5–7 pending.
- **Audience:** a fresh agent picking this up cold. Everything needed to start is in this file plus
  `context/`.

### Step status

| Step | Scope | Status |
|------|-------|--------|
| 1 | Correct the record | ✅ done |
| 2 | Idempotent candle storage | ✅ done — PR #2 |
| 3 | Candle sink service | ✅ done — PR #3 |
| 4 | Reconciliation on ClickHouse | ✅ done — PR #4 |
| 5 | DAG wiring + loud fallback | ⬜ blocked by 4 |
| 6 | `verify_pipeline` | ⬜ blocked by 5 |
| 7 | Live proof + close the record | ⬜ blocked by 6; **needs market hours** |

---

## 1. Why this work exists

The project has all the right architectural boxes — Kafka as bus, Spark for event-time aggregation,
MinIO + ClickHouse dual storage, Airflow for finite jobs. What it lacks is any instrument capable of
telling you whether any of it works. Documentation described a platform running "end-to-end" that
was, in fact, producing no stored candles at all.

The user's stated north star, from the original project brief:

> "The first objective is to create a reliable market-data platform. The trading strategy and
> automated execution should be built only after the data pipeline has demonstrated that it can
> continuously collect, process, store, validate, and replay market data correctly."

### 1.1 The primary defect: candles are never persisted

**Evidence (verify this yourself first):**

```bash
grep -rn "sink_candles\|insert_candles" --include=*.py . | grep -v .venv
```

Current output — note the single call site:

```
storage/clickhouse_client.py:81:def insert_candles(records: list[dict]) -> None:
storage/sinks.py:21:def sink_candles(records: list[dict], source: str = "raw") -> None:
streaming/reconciliation.py:105:        sink_candles(reconciled, source="reconciled")
```

`storage/tick_sink.py` does this job for `market.ticks`. **No equivalent exists for the candle
topics.** `market.candles.raw` and `market.candles.calculated` are produced but never consumed for
storage.

**Consequences:**

| Consumer | Reads | Finds |
|----------|-------|-------|
| `analysis/features.py` | reconciled, falls back to raw | neither exists → all indicators null |
| `analysis/data_quality.py` | reconciled, falls back to raw | nothing → logs `completeness: failed` into an unread log |
| `streaming/reconciliation.py` | the two Kafka topics | usually nothing → publishes nothing |

`market_candles` in ClickHouse is empty. MinIO holds tick Parquet but no candle Parquet.

### 1.2 Secondary defects, in the order they must be fixed

2. **`market_candles` is a plain `MergeTree`.** Once persistence works, re-reconciling an overlapping
   window will insert duplicate rows. `query_candles` uses `LIMIT`, so duplicates silently crowd out
   distinct candles in every feature computation. Fix *before* reconciliation goes live.
   Decision: [ADR-011](../../context/DECISIONS.md#adr-011-candle-storage-is-idempotent-by-key).

3. **Reconciliation reads Kafka offsets.** `streaming/reconciliation.py:18` opens a `KafkaConsumer`
   with no `group_id`, `auto_offset_reset="latest"`, `consumer_timeout_ms=10000`. On a 5-minute DAG it
   only sees messages produced during its own ~10s window, and it drains the raw topic for the full
   10s before starting on calculated — the windows do not overlap. Decision:
   [ADR-009](../../context/DECISIONS.md#adr-009-clickhouse-is-the-source-of-truth-for-reconciliation).

4. **Price tolerance is proportional, not absolute.** `_within_tolerance` computes
   `tolerance * max(|a|, |b|, 1)`, so with `PRICE_TOLERANCE = 0.01` a $600 stock is accepted within
   **$6.00** of the official value. A reconciliation that cannot detect a $5 error is worse than none,
   because it reports `matched` and suppresses the alert it exists to raise.

5. **The fallback is silent.** `analysis/features.py:15` falls back to raw candles without warning.
   This is what hid defects 1–3 for months.

### 1.3 One thing that is NOT broken

An early hypothesis worth discarding so nobody re-investigates it: the Spark candle builder's
watermark is fine. `spark.sql.streaming.outputMode` defaults to `append`, and
`streaming/spark_utils.py:67` sets `append` explicitly, so the watermark is meaningful. No fix needed.

There *is* a real Spark issue, lower priority: `first`/`last` resolve by **ingestion** order rather
than event-time order, so `open`/`close` can be wrong under shuffle, and there is no duplicate-event
handling. Roadmap Phase 6. Not required for the steps below.

---

## 2. Execution order and why

Each step is independently reviewable. **Do not start step N+1 until you have demonstrated step N.**

```
2 (storage idempotency) → 3 (candle sink) → 4 (reconciliation) → 5 (DAG + fallback)
                                                              → 6 (verify_pipeline) → 7 (live proof)
```

Step 2 goes first *specifically because* `market_candles` is empty right now, so `docker compose
down -v` costs nothing. Once data lands, the migration becomes painful. Cheap now, expensive later.

---

## 3. Step 2 — Idempotent candle storage (ADR-011)

### Goal

Writing the same `(symbol, interval, timestamp, source)` key twice must yield one row, so
reconciliation can re-run over overlapping windows without corrupting downstream reads.

### Files

- `infra/clickhouse/init.sql` — `market_candles` definition
- `storage/clickhouse_client.py` — `CANDLE_COLUMNS`, `insert_candles`, `query_candles`
- `tests/test_clickhouse_client.py` (new)

### Changes

**`init.sql`** — engine and sort key:

```sql
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, interval, timestamp, source);
```

Precedent exists — `market_positions` already uses `ReplacingMergeTree(updated_at)` (`init.sql:107`).

> **Decision point for the owner or agent.** ADR-011 specifies a *new* `replaced_at DateTime64(3)`
> version column. The existing `created_at` is already `DateTime64(3)` and is already populated on
> every insert path — Spark sets it to `current_timestamp()`, reconciliation sets it to
> `datetime.now(utc)`. **Resolved: reuse `created_at`** — done in PR #2. It became
> `ReplacingMergeTree(created_at)` ordered by `(symbol, interval, timestamp, source)`, and all
> candle reads use `FINAL`. `market_positions` already used this pattern. ADR-011 still says
> `replaced_at` and should be amended to match.

`FINAL` is load-bearing, not decorative. Merges are asynchronous, so a reader can observe duplicates
at any moment. Measured with merges stopped and two separate inserts: **2 rows without `FINAL`,
1 with it**. Note that a single `insert()` call containing duplicate rows is already collapsed at
insert time, so a test must use separate calls to have teeth.

The windowed query for Step 4 is **not** built yet (YAGNI until reconciliation calls it). Step 4
adds it:

```python
def query_candles_window(
    symbol: str, interval: str, source: str, start: datetime, end: datetime
) -> list[dict]:
```

Same `SELECT`, `WHERE timestamp >= {start} AND timestamp < {end}`, no `LIMIT`, `ORDER BY timestamp ASC`.
Returning ascending order matters — Step 4 pairs the two series by key, not by position.

### Verification

```bash
# 1. Engine changed
docker compose exec clickhouse clickhouse-client -q \
  "SHOW CREATE TABLE market_platform.market_candles"
# expect ReplacingMergeTree(created_at) ... ORDER BY (symbol, interval, timestamp, source)

# 2. Idempotency — run the new test
uv run pytest tests/test_clickhouse_client.py -v

# 3. Suite still green
uv run pytest tests/ -q && uv run ruff check . && uv run ruff format --check .
```

The test should insert the same candle twice and assert one row comes back. Because that needs a live
ClickHouse, structure it to skip cleanly when the host is unreachable — do not let CI depend on Docker.

**Do this before any data exists:** `docker compose down -v` once, then bring the stack back up so
`init.sql` re-runs against an empty volume.

---

## 4. Step 3 — Candle sink service (the missing component)

### Goal

Persist `market.candles.raw` and `market.candles.calculated` to MinIO and ClickHouse. Until this
lands, everything downstream reads an empty table.

### Files

- `storage/candle_sink.py` (new) — **copy `storage/tick_sink.py` and adapt**
- `entrypoint/candle_sink_service.py` (new) — copy `entrypoint/tick_sink_service.py`
- `docker-compose.yml` — new `candle-sink` service
- `docs/services.md` — add the service row
- `tests/test_candle_sink.py` (new) — mirror `tests/test_tick_sink.py`

### The template

`storage/tick_sink.py` already solves everything you need: batched consumption (500 messages / 10s),
`group_id` so offsets commit and the archive resumes after a restart, exponential-backoff broker
retry, and SIGTERM/SIGINT handling. Copy it rather than reinventing.

Differences from the tick sink:

| | `tick-sink` | `candle-sink` |
|---|---|---|
| Topics | `market.ticks` | `market.candles.raw` **and** `market.candles.calculated` |
| Group id | `tick-sink` | `candle-sink` |
| Sink call | `sink_ticks(batch)` | `sink_candles(batch)` — already takes a `source` |

Both topics in **one** consumer process. This is the owner's stated preference and matches how
`tick-sink` already handles MinIO and ClickHouse together in one process. `KafkaConsumer` accepts a
list of topics; a single `poll()` returns a dict keyed by topic, so route each partition's messages by
its topic to the right sink batch.

`sink_candles` writes MinIO at `raw/candles/symbol=…/interval=…/source=…/` and ClickHouse in one
call — it needs no changes. Note it does **not** take `interval` or `source` as arguments; both come
from the record itself.

### Compose service

Mirror the `tick-sink` block exactly — same `depends_on` (broker healthy, kafka-init completed, minio
healthy, minio-init completed, clickhouse healthy), same environment variables, same
`mem_limit: 192m` / `cpus: 0.5`.

```yaml
  candle-sink:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: candle-sink
    command: uv run --no-dev python -m entrypoint.candle_sink_service
    # ...same depends_on / environment / limits as tick-sink
```

The `Dockerfile` already copies `storage/` and `entrypoint/`, so no image change is needed.

### Populating data to verify against

The candle DAGs run on schedule. To verify **outside market hours**, trigger the backfill DAG
(`market_candles_backfill_dag`, manual trigger) which calls `fetch_candles_range` to load historical
candles into `market.candles.raw`.

Calculated candles have no backfill path — they come only from Spark aggregating live ticks. So
Step 3 is verifiable any time; full reconciliation coverage (Step 7) is **not**.

### Verification

```bash
# Both sources present
docker compose exec clickhouse clickhouse-client -q \
  "SELECT source, count() FROM market_platform.market_candles FINAL GROUP BY source"

# MinIO has candle partitions (http://localhost:9001)
# expect: raw/candles/symbol=<SYM>/interval=1m/source=yahoo_finance/...

uv run pytest tests/test_candle_sink.py -v
```

---

## 5. Step 4 — Reconciliation against ClickHouse (ADR-009)

### Goal

Compare candles as a set over an explicit time window, in ClickHouse, rather than by consuming topic
offsets. Make it durable, idempotent, and replayable.

### Files

- `streaming/reconciliation.py` — rewrite the data path
- `storage/clickhouse_client.py` — may need `count` / `latest` helpers
- `tests/test_reconciliation.py` — extend

### What to preserve

`reconcile_candle()` is a pure function, already unit-tested, and its four status outcomes are
correct. **Keep it byte-identical.** Same for `_candle_key`. The existing tests for them must keep
passing untouched — that is your regression net.

Only the I/O around them changes.

### What to change

1. **Delete** `_consume_topic` and the `from kafka import KafkaConsumer` import.
2. **New signature:**

   ```python
   def reconcile_candles(
       symbols: list[str] | None = None,
       intervals: tuple[str, ...] = ("1m", "5m"),
       lookback_minutes: int = 10,
   ) -> dict:
   ```

   The window must be **explicit and logged**, not an emergent property of process timing.
3. **Read** both series for the window from ClickHouse, build `{key: candle}` indexes, union the key
   sets, and feed each pair through the existing `reconcile_candle()`.
4. **Write** results to ClickHouse and MinIO via `sink_candles(reconciled, source="reconciled")`, and
   still **publish** to `market.candles.reconciled` — the topic stays part of the contract
   ([ADR-001](../../context/DECISIONS.md#adr-001-kafka-as-central-event-bus)), it is simply not the
   read path.
5. **Log the window and coverage ratio** on every run. Silence here is what hid the original defect.
6. **Fix the tolerance.** Split the predicate:

   ```python
   PRICE_TOLERANCE = 0.01  # absolute, in price units


   def _price_within_tolerance(a: float, b: float) -> bool:
       return abs(a - b) <= PRICE_TOLERANCE


   def _volume_within_tolerance(a: float, b: float, rel: float = 0.05) -> bool:
       if a == 0 and b == 0:
           return True
       return abs(a - b) <= rel * max(abs(a), abs(b), 1)
   ```

   Volume stays relative (5%); price becomes absolute. Update
   `tests/test_reconciliation.py` to cover the boundary in both directions — a 1-cent delta must pass,
   a 5-cent delta must fail, regardless of price level.

### Verification

The check that matters, and the reason Step 2 came first:

```bash
# Run reconciliation twice over the same window
uv run python -c "from streaming.reconciliation import reconcile_candles; print(reconcile_candles())"
uv run python -c "from streaming.reconciliation import reconcile_candles; print(reconcile_candles())"

# Row count must NOT have doubled
docker compose exec clickhouse clickhouse-client -q \
  "SELECT count() FROM market_platform.market_candles FINAL WHERE source='reconciled'"
```

Non-zero metrics on the first run (not all zeros) proves it found something. An unchanged count on the
second run proves Step 2's idempotency holds end-to-end.

Also verify historical replay, which the old Kafka-offset design could never do:

```python
reconcile_candles(lookback_minutes=60)  # a window the process was never alive for
```

---

## 6. Step 5 — DAG wiring and the loud fallback

### Goal

Put the rewritten reconciliation on the schedule, and stop the feature layer from degrading silently.

### Files

- `airflow/dags/dag_reconciliation.py` — thin, per ADR-005
- `analysis/features.py` — `_load_candles`
- `tests/test_features.py` — extend

### Changes

DAG: point the existing task at the new function signature. Nothing else. Zero business logic in the
DAG file — that rule is
[ADR-005](../../context/DECISIONS.md#adr-005-thin-airflow-dags) and is currently respected.

`features.py`: keep the fallback — it is genuinely correct on a fresh stack where reconciliation
legitimately has not run yet — but make it observable:

```python
logger.warning(
    "No reconciled candles for %s/%s; falling back to source=%s. Features are UNVALIDATED.",
    symbol,
    interval,
    config.SOURCE_YAHOO,
)
```

Return the fact alongside the candles so a caller or health check can count occurrences. Expose a
module-level counter for `verify_pipeline` to read in Step 6.

### Verification

Unpause `market_reconciliation_dag` in the Airflow UI (http://localhost:8080) and confirm a green
run. Then check `analysis/features.py` emits the warning when reconciled is empty.

---

## 7. Step 6 — `verify_pipeline` (ADR-010)

### Goal

One command that answers "is the platform working?", with a non-zero exit when it is not. This is
the artifact whose absence let every prior defect go unnoticed.

### Files

- `analysis/pipeline_health.py` (new)
- `entrypoint/verify_pipeline.py` (new)
- `airflow/dags/dag_data_quality.py` — add a task
- `tests/test_pipeline_health.py` (new)

### Checks to implement

Against ClickHouse, in one pass where possible:

| Check | Threshold |
|-------|-----------|
| Ticks arriving; age of newest tick | ≤ 2 min during market hours |
| Calculated candles in window | > 0 |
| Raw candles in window | > 0 |
| Reconciled candles in window | > 0 |
| Reconciled-to-raw coverage ratio | ≥ 0.99 |
| OHLC validity violations | 0 |
| Duplicate `(symbol, interval, timestamp)` keys | 0 |
| Ingestion latency, p50 and max | p95 ≤ 5s |
| Feature freshness | within one interval |
| **Fallback count** (from Step 5) | 0 |

Design notes:

- `analysis/` is the right home per the directory-ownership table in `.agents/AGENTS.md`.
- Market-hours awareness matters: freshness and latency checks must not fail at 4am. Take the
  session state as a parameter with a sane default rather than hardcoding it.
- `verify_pipeline` should return a structured result that both the CLI and the DAG task consume —
  business logic in the module, presentation in the entrypoint (ADR-005).
- Exit 1 on failure so Airflow marks the task red.

### Verification

```bash
docker compose up -d
uv run python -m entrypoint.verify_pipeline; echo "exit=$?"

# with the stack down, it must fail loudly:
docker compose stop clickhouse
uv run python -m entrypoint.verify_pipeline; echo "exit=$?"   # expect exit=1
docker compose start clickhouse
```

A verification command that cannot fail is worthless. Confirm the failure path explicitly.

---

## 8. Step 7 — Live proof and closing the record

Requires **market hours** (09:30–16:00 ET). Calculated candles only exist while ticks are flowing, so
there is no way to synthesize full coverage.

```bash
docker compose up -d
uv run python -m entrypoint.verify_pipeline; echo "exit=$?"
```

Target: reconciled coverage ≥ 0.99 over a completed session, zero duplicate keys, zero OHLC
violations.

Then update the documentation to record the resolutions:

- `context/CURRENT_STATE.md` — resolve Debt #1–#5, move the storage/reconciliation rows to "What
  Works", rewrite the summary
- `context/ROADMAP.md` — Phase 5 → Complete, Phase 8 → Complete, Phase 9 → Complete; un-gate Phase 14
- `context/DECISIONS.md` — amend ADR-011 if you reused `created_at` instead of adding `replaced_at`

---

## 9. Constraints the agent must not violate

From `.agents/AGENTS.md` and the ADRs — these are enforced by project convention:

1. **Zero business logic in Airflow DAG files** ([ADR-005](../../context/DECISIONS.md#adr-005-thin-airflow-dags)).
   Scheduling, retries, dependencies, params only.
2. **Long-running services are standalone containers, never Airflow tasks**
   ([ADR-003](../../context/DECISIONS.md#adr-003-standalone-tick-service-not-airflow-task)).
   The candle sink qualifies.
3. **Validate every outbound payload against `schemas/v1/` before publishing**
   ([ADR-004](../../context/DECISIONS.md#adr-004-json-schema-for-data-contracts)).
   All schemas are Draft 2020-12 with `additionalProperties: false`. `validate_candle` is already
   wired in reconciliation — do not bypass it.
4. **No hardcoded keys, symbols, or endpoints** — use `ingestion/config.py`.
5. **The six Kafka topic names are fixed.** Do not rename or repartition them.
6. **ISO-8601 UTC timestamps** in all payloads.
7. **Route validation failures to `market.errors`**, not just stderr.
8. **`uv run ruff check . && uv run ruff format --check .` and `uv run pytest tests/ -q` must pass
   before committing.** Dependencies via `uv add` only.

### Gotchas

- **`pyyaml` is not declared in `pyproject.toml`** but `tests/test_compose_*.py` import it; it
  currently resolves transitively. If a compose test starts failing on `ModuleNotFoundError`, that is
  why — do not add an unrelated dependency without asking.
- **Four test files assert on `docker-compose.yml` as a data structure**
  (`test_compose_bounds.py`, `test_compose_kafka.py`, `test_compose_spark.py`,
  `test_config_validation.py`) — e.g. `mem_limit == "768m"`. Adding a new service is fine; changing
  existing services' limits will fail these. That brittleness is itself Roadmap Phase 25 debt.
- **Line endings:** `.gitattributes` enforces LF. A CRLF bug once shipped (`14ba702`). Write files
  with LF; do not edit `.sh` or `.yml` from a CRLF source.
- **The `Dockerfile` copies only `ingestion/`, `entrypoint/`, `schemas/`, `storage/`, `utils/`.**
  `streaming/` and `analysis/` are mounted by the Spark and Airflow services respectively. Anything
  new must live in a copied directory or be added to the image.
- **`docker compose up` does not rebuild.** `tick-ingestion` and `tick-sink` used to bake their code
  into the image, so `up` reused a cached build predating `CLICKHOUSE_USER`; tick-sink then
  authenticated as `default` and every insert failed silently, because `sink_ticks` logs and
  continues. Both now mount the source like the other services, so a code edit needs only
  `docker compose restart <service>`. Rebuild only when `pyproject.toml` / `uv.lock` change.
- **ClickHouse needs real headroom.** `max_server_memory_usage` at 512MB sat below the server's own
  ~460MB baseline and it could not answer even `SELECT 1`. The override is gone and `mem_limit` is
  1280m; observed RSS is ~413MB idle. If queries fail with `MEMORY_LIMIT_EXCEEDED`, check
  `docker stats` before touching config.
- **`init.sql` is skipped when the volume already holds data** — the entrypoint logs *"Database
  directory appears to contain a database; Skipping initialization"*. A fresh clone gets new DDL;
  an existing stack does not. After editing `init.sql`, either `docker compose down -v`, or drop the
  table and re-apply by hand.

---

## 10. Open questions for the owner

1. **Symbols.** `.env` currently sets `BTC-USD,NVDA,MSTR,NU,HIMS,MU`. Market-context features
   reference SPY and QQQ, neither of which is in the list, so market context would stay empty.
   Add them?
2. **Version column.** Reuse `created_at` as the `ReplacingMergeTree` version (recommended), or add a
   new `replaced_at` column as ADR-011 currently specifies?
3. **Verification scope.** The owner chose a single `verify_pipeline` command over ClickHouse
   metrics tables, integration tests, and Prometheus/Grafana. Those were *deferred by decision*, not
   rejected — recorded in `CURRENT_STATE.md`. Reconsider at Step 7.

---

## 11. Definition of done

Per [ADR-010](../../context/DECISIONS.md#adr-010-verification-gates-the-trading-stages), the platform
is not "done" because the code was written. It is done when `verify_pipeline` exits 0 against a real
running stack over a completed market session, reporting:

- reconciled coverage ≥ 99% of official candles
- zero duplicate `(symbol, interval, timestamp)` keys
- zero OHLC validity violations
- tick freshness ≤ 2 min
- p95 ingestion latency ≤ 5s
- zero feature fallbacks

Until then, **do not start Phase 14 (signal engine).** A backtest run against unvalidated candles
produces results that cannot be believed, which costs far more time than closing the gap first.
