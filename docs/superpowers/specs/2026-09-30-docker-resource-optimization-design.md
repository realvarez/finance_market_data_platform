# Docker & Architecture Resource Optimization Design

- **Date:** 2026-09-30
- **Status:** Approved
- **Target Environment:** Local Development (WSL2 / Docker Desktop with ~7.6 GB RAM, 20 CPU threads)
- **Primary Goal:** Reduce container resource footprint from ~8.5 GB peak to ~3.2 GB (~60% reduction) without modifying data contracts or downstream trading logic.

---

## 1. Problem Statement & Baseline

The Market Platform architecture ([context/ARCHITECTURE.md](file:///home/realvarez/projects/fin-projects/market_platform/context/ARCHITECTURE.md)) runs 12 containers in [docker-compose.yml](file:///home/realvarez/projects/fin-projects/market_platform/docker-compose.yml):
* **Confluent Enterprise Server (`cp-server:7.6.0`)**: Boots with enterprise plugins (telemetry exporters, cluster links, balancers) and no JVM heap caps (`KAFKA_HEAP_OPTS`), allocating up to 1.5–2.0 GB RAM.
* **Control Center (`cp-enterprise-control-center:7.6.12`)**: Massive web and telemetry engine consuming ~1.8–2.5 GB RAM and high background CPU.
* **Schema Registry (`cp-schema-registry:8.2.2`)**: Consumes ~512 MB RAM, despite being unused by any producer/consumer (schema validation is performed client-side using Python `jsonschema`).
* **Spark 3-Container Cluster (`spark-master`, `spark-worker`, `candle-builder`)**: Runs 4 concurrent JVM processes (master daemon, worker daemon, driver JVM, executor JVM) consuming ~2.5 GB RAM to aggregate candles for 6 symbols.
* **Airflow Standalone & Postgres**: Spawns webserver, scheduler, triggerer, and DAG processor. The DAG processor scans all DAGs every 30 seconds across multiple sub-processes, causing continuous CPU spikes and memory creep.
* **ClickHouse & MinIO**: Have no cgroup memory or CPU limits defined, allowing ClickHouse's memory manager to claim up to 90% of host RAM during table queries.

On a host machine or WSL2 instance with ~7.6 GB RAM, total unconstrained demand reaches **8.0–9.5 GB RAM**, resulting in OS memory swapping, CPU throttling, and random container OOM kills.

---

## 2. Design Principles & Strategy

1. **High-Fidelity Lean Stack**: Retain all core technologies (Kafka KRaft, Spark Structured Streaming, ClickHouse, MinIO, Airflow, and Postgres) to maintain strict compliance with ADR-001 through ADR-007.
2. **YAGNI (You Aren't Gonna Need It)**: Remove enterprise daemons and unused registry services that provide zero value in local dev.
3. **Single Standalone Spark Engine**: Collapse multi-container Spark master/worker into a single container running `--master local[2]`.
4. **Predictable Cgroups**: Apply explicit `mem_limit`, `mem_reservation`, and `cpus` limits to every service in `docker-compose.yml`.
5. **Profile-Driven Composition**: Use Docker Compose profiles (`profiles: ["ui"]`, `profiles: ["spark-cluster"]`) so heavyweight tooling runs strictly on-demand.

---

## 3. Detailed Component Optimizations

### 3.1 Event Bus (Kafka KRaft)
* **Image**: Switch from `confluentinc/cp-server:7.6.0` to standard `confluentinc/cp-kafka:7.6.0` (or `apache/kafka:3.8.0`).
* **JVM Options**: Set `KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m"`.
* **Cgroup Limits**: `mem_limit: 768m`, `mem_reservation: 384m`, `cpus: 1.0`.
* **Topic Bootstrap (`kafka-init`)**: Reuses the broker image to avoid downloading a separate 2 GB image, assigned `mem_limit: 128m`, and terminates upon exit.

### 3.2 Kafka UI & Schema Registry
* **Schema Registry**: Removed from default startup. Placed under `profiles: ["debug"]` if needed for future Avro testing.
* **Control Center Replacement**: Replaced with **AKHQ** (`tchiotludo/akhq:latest`, ~80–120 MB RAM) mapped to port `9021`.
* **UI Profile**: Assigned `profiles: ["ui"]`. Starting default stack with `docker compose up -d` keeps the UI dormant; `docker compose --profile ui up -d` boots AKHQ when visual topic inspection is needed.

### 3.3 Streaming Layer (Spark Candle Builder)
* **Consolidation**: Eliminate `spark-master` and `spark-worker` from default startup. Move them to `profiles: ["spark-cluster"]`.
* **Execution Mode**: `candle-builder` runs:
  ```bash
  /opt/spark/bin/spark-submit \
    --master local[2] \
    --driver-memory 768m \
    --conf spark.sql.shuffle.partitions=2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.780 \
    /opt/spark/apps/stream_candle_builder.py
  ```
* **Cgroup Limits**: `mem_limit: 1024m`, `cpus: 1.5`.
* **Shuffle Partitions**: Reduced from Spark's default of 200 down to 2, drastically lowering thread context switching and state-store overhead.
* **Application Logic**: Zero modifications required to [streaming/stream_candle_builder.py](file:///home/realvarez/projects/fin-projects/market_platform/streaming/stream_candle_builder.py) or [streaming/spark_utils.py](file:///home/realvarez/projects/fin-projects/market_platform/streaming/spark_utils.py).

### 3.4 Storage Layer (ClickHouse & MinIO)
* **ClickHouse**:
  * Set `mem_limit: 768m`, `mem_reservation: 384m`, `cpus: 1.0`.
  * Add configuration override in `infra/clickhouse/config.d/memory.xml`:
    * `<max_server_memory_usage>536870912</max_server_memory_usage>` (512 MB cap).
    * `<mark_cache_size>67108864</mark_cache_size>` (64 MB cache).
* **MinIO**:
  * Set `mem_limit: 256m`, `cpus: 0.5`.
  * Add `MINIO_API_REQUESTS_MAX: 100` to prevent unconstrained buffers.
* **`minio-init`**: Set `mem_limit: 128m` and `restart: "no"`.

### 3.5 Ingestion Layer
* **`tick-ingestion`**: Set `mem_limit: 192m`, `cpus: 0.5`.
* **`tick-sink` (Stage 1 Service)**: Pre-configured with `mem_limit: 192m`, `cpus: 0.5`.

### 3.6 Orchestration Layer (Airflow & Postgres)
* **Airflow Standalone**:
  * Set `mem_limit: 1024m`, `mem_reservation: 512m`, `cpus: 1.5`.
  * Add environment configuration:
    * `AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL: 60` (reduces DAG parse frequency from 30s to 60s).
    * `AIRFLOW__SCHEDULER__PARSING_PROCESSES: 1` (single parser worker).
    * `AIRFLOW__CORE__PARALLELISM: 4` (caps global concurrent task instances).
    * `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 2`.
    * `AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1`.
* **Postgres Metadata DB**:
  * Set `mem_limit: 192m`, `mem_reservation: 96m`, `cpus: 0.5`.
  * Command: `postgres -c shared_buffers=64MB -c max_connections=30`.

---

## 4. Resource Allocation Budget

| Service | Current RAM (Est.) | Optimized RAM Limit | Optimized CPU Limit | Compose Profile |
|:---|:---|:---|:---|:---|
| `broker` | ~1,750 MB | 768 MB | 1.0 core | default |
| `kafka-init` | one-shot | 128 MB (exits) | 0.5 core | default |
| `candle-builder` | ~2,500 MB (master + worker + driver) | 1,024 MB | 1.5 cores | default |
| `clickhouse` | ~1,500 MB | 768 MB | 1.0 core | default |
| `minio` | ~300 MB | 256 MB | 0.5 core | default |
| `minio-init` | one-shot | 128 MB (exits) | 0.5 core | default |
| `tick-ingestion` | ~100 MB | 192 MB | 0.5 core | default |
| `postgres` | ~200 MB | 192 MB | 0.5 core | default |
| `airflow` | ~1,200 MB | 1,024 MB | 1.5 cores | default |
| **Core Stack Total** | **~7,550 MB** | **~3,200 MB** *(Hard Cap: 3.8 GB)* | **~5.5 pooled** | **default** |
| `akhq` *(optional)* | ~2,000 MB *(Control Center)* | 128 MB | 0.5 core | `ui` |
| `spark-master` *(optional)* | ~450 MB | 384 MB | 0.5 core | `spark-cluster` |
| `spark-worker` *(optional)* | ~1,300 MB | 768 MB | 1.0 core | `spark-cluster` |

---

## 5. Files to Create and Modify

1. [docker-compose.yml](file:///home/realvarez/projects/fin-projects/market_platform/docker-compose.yml):
   - Update broker to `cp-kafka:7.6.0` with `KAFKA_HEAP_OPTS`.
   - Remove `schema-registry` from default.
   - Replace `control-center` with `akhq` under profile `ui`.
   - Reconfigure `candle-builder` to run `--master local[2]`.
   - Add `profiles: ["spark-cluster"]` to `spark-master` and `spark-worker`.
   - Add Airflow scheduler configuration variables.
   - Add Postgres connection/buffer command flags.
   - Add `mem_limit` and `cpus` to all active services.
2. `infra/clickhouse/config.d/memory.xml` *(new file)*:
   - ClickHouse memory cap configuration.
3. [docker-compose.yml](file:///home/realvarez/projects/fin-projects/market_platform/docker-compose.yml) volumes:
   - Mount `./infra/clickhouse/config.d/memory.xml:/etc/clickhouse-server/config.d/memory.xml`.
4. [docs/services.md](file:///home/realvarez/projects/fin-projects/market_platform/docs/services.md):
   - Update documentation to reflect updated resource profiles, ports, and default memory budgets.

---

## 6. Verification and Testing

1. **Syntax Validation**: Run `docker compose config` to verify YAML validity.
2. **Container Launch**: Boot stack via `docker compose up -d`.
3. **Memory Inspection**: Run `docker stats --no-stream` and assert that no container exceeds its allocation and total active memory remains under ~3.5 GB.
4. **Data Continuity Verification**:
   * Topic verification: `docker compose exec broker kafka-topics --list --bootstrap-server localhost:9092`.
   * Tick ingestion: Assert incoming ticks on `market.ticks`.
   * Spark calculation: Assert 1m and 5m candle generation on `market.candles.calculated`.
   * Airflow health: Assert `curl -f http://localhost:8080/health` returns healthy.
5. **Regression Test Suite**: Run `uv run pytest tests/ -v`.
