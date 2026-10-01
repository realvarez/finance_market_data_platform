# Docker & Architecture Resource Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Optimize Docker Compose resource allocations, service topologies, and runtime JVM/scheduler configurations to reduce total development RAM usage from ~8.5 GB to ~3.2 GB (~60% reduction) without breaking data contracts or architectural standards.

**Architecture:** 
- Switch Kafka to standard KRaft `cp-kafka` with bounded JVM heap (`-Xms256m -Xmx512m`) and drop unused Schema Registry.
- Replace Confluent Control Center with lightweight AKHQ under an optional `ui` profile.
- Consolidate multi-container Spark master/worker into a single standalone streaming container running `--master local[2]`.
- Enforce explicit `mem_limit` and `cpus` limits across ClickHouse, MinIO, Postgres, Airflow, and Ingestion services.

**Tech Stack:** Docker Compose, Apache Kafka KRaft (Confluent cp-kafka), Apache Spark 4.0, ClickHouse, MinIO, Apache Airflow 3.2, Python 3.12 (uv).

**Spec:** [docs/superpowers/specs/2026-09-30-docker-resource-optimization-design.md](file:///home/realvarez/projects/fin-projects/market_platform/docs/superpowers/specs/2026-09-30-docker-resource-optimization-design.md)

## Global Constraints

- Retain full compliance with ADR-001 (central event bus), ADR-002 (dual storage sink), and ADR-005 (thin DAGs).
- Zero breaking changes to Kafka topic names (`market.ticks`, `market.candles.raw`, `market.candles.calculated`, `market.candles.reconciled`, `market.signals`, `market.errors`).
- Spark streaming candle aggregation logic in [streaming/stream_candle_builder.py](file:///home/realvarez/projects/fin-projects/market_platform/streaming/stream_candle_builder.py) must remain functionally identical.
- All cgroup limits must specify both `mem_limit` and CPU limits.

## Review Focus

1. **Kafka Listener Connectivity:** Switching to `cp-kafka:7.6.0` must keep internal listener `broker:29092` and host listener `localhost:9092` fully operational for both containerized services and host tests.
2. **Spark Local Streaming Checkpointing:** `candle-builder` in `local[2]` mode must successfully authenticate to MinIO (`s3a://market-data/checkpoints/`) and write to `market.candles.calculated`.
3. **ClickHouse Memory Enforcement:** ClickHouse memory XML override must not reject table initialization from [infra/clickhouse/init.sql](file:///home/realvarez/projects/fin-projects/market_platform/infra/clickhouse/init.sql).
4. **Airflow Parser Churn:** Airflow standalone scheduler tuning must not prevent DAGs from triggering on schedule or fetching candles.
5. **Profile Isolation:** Running `docker compose up -d` must not spin up AKHQ, Spark Master, or Spark Worker unless their respective `--profile` flag is provided.

---

### Task 1: ClickHouse Memory Configuration Override

**Files:**
- Create: `infra/clickhouse/config.d/memory.xml`
- Test: `tests/test_config_validation.py` (new test verifying XML validity and ClickHouse configuration files)

**Interfaces:**
- Consumes: ClickHouse 24+ server configuration schema.
- Produces: `infra/clickhouse/config.d/memory.xml` mounted into `/etc/clickhouse-server/config.d/memory.xml`.

- [ ] **Step 1: Write test for ClickHouse XML configuration validity**

Create `tests/test_config_validation.py`:
```python
import xml.etree.ElementTree as ET
from pathlib import Path


def test_clickhouse_memory_xml():
    xml_path = (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "clickhouse"
        / "config.d"
        / "memory.xml"
    )
    assert xml_path.exists(), f"File {xml_path} does not exist"

    tree = ET.parse(xml_path)
    root = tree.getroot()
    assert root.tag == "clickhouse"

    mem_usage = root.find("max_server_memory_usage")
    assert mem_usage is not None
    assert int(mem_usage.text) == 536870912  # 512 MB

    mark_cache = root.find("mark_cache_size")
    assert mark_cache is not None
    assert int(mark_cache.text) == 67108864  # 64 MB
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_config_validation.py -v`
Expected: FAIL with "File ... does not exist"

- [ ] **Step 3: Implement ClickHouse memory XML configuration**

Create `infra/clickhouse/config.d/memory.xml`:
```xml
<clickhouse>
    <!-- Cap server-wide memory allocation to 512MB for local development -->
    <max_server_memory_usage>536870912</max_server_memory_usage>
    <!-- Cap mark cache size to 64MB -->
    <mark_cache_size>67108864</mark_cache_size>
</clickhouse>
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_config_validation.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add infra/clickhouse/config.d/memory.xml tests/test_config_validation.py
git commit -m "perf(clickhouse): add 512MB memory cap configuration override"
```

---

### Task 2: Kafka Broker & UI Layer Compose Optimization

**Files:**
- Modify: `docker-compose.yml:3-108`
- Modify: `infra/kafka/init-topics.sh:29-35`
- Test: `tests/test_compose_kafka.py`

**Interfaces:**
- Consumes: [infra/kafka/init-topics.sh](file:///home/realvarez/projects/fin-projects/market_platform/infra/kafka/init-topics.sh).
- Produces: Optimized `broker`, `kafka-init`, and `akhq` service configurations in [docker-compose.yml](file:///home/realvarez/projects/fin-projects/market_platform/docker-compose.yml).

- [ ] **Step 1: Write test for Kafka Compose and Topic bootstrap configuration**

Create `tests/test_compose_kafka.py`:
```python
from pathlib import Path
import yaml


def test_compose_kafka_and_ui_config():
    compose_path = (
        Path(__file__).resolve().parent.parent / "docker-compose.yml"
    )
    with open(compose_path) as f:
        config = yaml.safe_load(f)

    services = config["services"]

    # Broker assertions
    broker = services["broker"]
    assert broker["image"] == "confluentinc/cp-kafka:7.6.0"
    assert 'KAFKA_HEAP_OPTS' in broker["environment"]
    assert broker["environment"]["KAFKA_HEAP_OPTS"] == "-Xms256m -Xmx512m"
    assert broker["mem_limit"] == "768m"

    # Schema Registry assertions (should be removed from default services)
    assert "schema-registry" not in services or "debug" in services.get(
        "schema-registry", {}
    ).get("profiles", [])

    # Kafka Init assertions
    kafka_init = services["kafka-init"]
    assert kafka_init["image"] == "confluentinc/cp-kafka:7.6.0"
    assert kafka_init["mem_limit"] == "128m"

    # UI assertions (AKHQ under profile 'ui')
    assert "control-center" not in services
    assert "akhq" in services
    assert "ui" in services["akhq"].get("profiles", [])
    assert services["akhq"]["mem_limit"] == "128m"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_compose_kafka.py -v`
Expected: FAIL with assertion errors on broker image/heap.

- [ ] **Step 3: Modify `docker-compose.yml` and `infra/kafka/init-topics.sh`**

In [infra/kafka/init-topics.sh](file:///home/realvarez/projects/fin-projects/market_platform/infra/kafka/init-topics.sh), remove the `_confluent-telemetry-metrics` workaround lines (29–34) since `cp-kafka` does not run the commercial telemetry exporter:
```bash
#!/bin/bash
set -e

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

create_topic() {
    local topic=$1
    local partitions=$2
    local retention_ms=$3

    kafka-topics --create \
        --if-not-exists \
        --bootstrap-server "$BOOTSTRAP" \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config retention.ms="$retention_ms"
}

echo "Creating Kafka topics on $BOOTSTRAP..."

create_topic "market.ticks"               3 604800000    # 7 days
create_topic "market.candles.raw"         3 2592000000   # 30 days
create_topic "market.candles.calculated"  3 2592000000   # 30 days
create_topic "market.candles.reconciled"  3 7776000000   # 90 days
create_topic "market.signals"             1 2592000000   # 30 days
create_topic "market.errors"              1 604800000    # 7 days

echo "Topics created:"
kafka-topics --list --bootstrap-server "$BOOTSTRAP"
```

In [docker-compose.yml](file:///home/realvarez/projects/fin-projects/market_platform/docker-compose.yml), update `broker`, `kafka-init`, remove `schema-registry` and replace `control-center` with `akhq`:
```yaml
  broker:
    image: confluentinc/cp-kafka:7.6.0
    hostname: broker
    container_name: broker
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker:29093'
      CLUSTER_ID: 'eMR-IFA7RgSQb3UDnGHyUw'
      KAFKA_LISTENERS: 'PLAINTEXT://broker:29092,CONTROLLER://broker:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_LOG_DIRS: '/var/lib/kafka/data'
      KAFKA_METADATA_LOG_DIR: '/var/lib/kafka/data'
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
    mem_limit: 768m
    mem_reservation: 384m
    cpus: 1.0
    volumes:
      - kafka_data:/var/lib/kafka/data
    networks:
      - market_platform_network
    healthcheck:
      test: [ "CMD", "bash", "-c", 'nc -z localhost 9092' ]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka-init:
    image: confluentinc/cp-kafka:7.6.0
    container_name: kafka-init
    depends_on:
      broker:
        condition: service_healthy
    volumes:
      - ./infra/kafka/init-topics.sh:/init-topics.sh
    environment:
      KAFKA_BOOTSTRAP_SERVERS: broker:29092
    entrypoint: [ "bash", "/init-topics.sh" ]
    mem_limit: 128m
    restart: "no"
    networks:
      - market_platform_network

  akhq:
    image: tchiotludo/akhq:latest
    container_name: akhq
    profiles:
      - ui
    depends_on:
      broker:
        condition: service_healthy
    ports:
      - "9021:8080"
    environment:
      AKHQ_CONFIGURATION: |
        akhq:
          connections:
            market-platform:
              properties:
                bootstrap.servers: "broker:29092"
    mem_limit: 128m
    cpus: 0.5
    networks:
      - market_platform_network
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_compose_kafka.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml infra/kafka/init-topics.sh tests/test_compose_kafka.py
git commit -m "perf(kafka): optimize broker with cp-kafka and replace control-center with akhq"
```

---

### Task 3: Spark Streaming Consolidation into Standalone Container

**Files:**
- Modify: `docker-compose.yml:164-250`
- Modify: `streaming/spark_utils.py:30-41`
- Test: `tests/test_compose_spark.py`

**Interfaces:**
- Consumes: [streaming/stream_candle_builder.py](file:///home/realvarez/projects/fin-projects/market_platform/streaming/stream_candle_builder.py).
- Produces: Consolidated single-container `candle-builder` running `--master local[2]`.

- [ ] **Step 1: Write test for Spark Compose configuration**

Create `tests/test_compose_spark.py`:
```python
from pathlib import Path
import yaml


def test_compose_spark_consolidation():
    compose_path = (
        Path(__file__).resolve().parent.parent / "docker-compose.yml"
    )
    with open(compose_path) as f:
        config = yaml.safe_load(f)

    services = config["services"]

    # candle-builder is standalone and has correct master and memory
    candle_builder = services["candle-builder"]
    assert candle_builder["mem_limit"] == "1024m"
    assert "--master local[2]" in candle_builder["command"]
    assert "--driver-memory 768m" in candle_builder["command"]
    assert "spark.sql.shuffle.partitions=2" in candle_builder["command"]

    # depends_on must not depend on spark-worker
    depends_on = candle_builder["depends_on"]
    assert "spark-worker" not in depends_on
    assert "spark-master" not in depends_on
    assert "broker" in depends_on

    # spark-master and spark-worker should be gated behind 'spark-cluster' profile
    if "spark-master" in services:
        assert "spark-cluster" in services["spark-master"].get("profiles", [])
    if "spark-worker" in services:
        assert "spark-cluster" in services["spark-worker"].get("profiles", [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_compose_spark.py -v`
Expected: FAIL with assertion errors on candle-builder dependencies or command.

- [ ] **Step 3: Modify `docker-compose.yml` and `streaming/spark_utils.py`**

In [streaming/spark_utils.py](file:///home/realvarez/projects/fin-projects/market_platform/streaming/spark_utils.py):
```python
        spark = (
            SparkSession.builder.appName(name)
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
```

In [docker-compose.yml](file:///home/realvarez/projects/fin-projects/market_platform/docker-compose.yml):
```yaml
  spark-master:
    image: spark:4.0.2-java21-python3
    container_name: spark-master
    profiles:
      - spark-cluster
    command: >
      /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host spark-master --webui-port 8080
    environment:
      PYTHONPATH: /app
      KAFKA_BOOTSTRAP_SERVERS: broker:29092
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    mem_limit: 384m
    cpus: 0.5
    ports:
      - "8082:8080"
      - "7077:7077"
    volumes:
      - .:/app
      - ./streaming:/opt/spark/apps
    networks:
      - market_platform_network

  spark-worker:
    image: spark:4.0.2-java21-python3
    container_name: spark-worker
    profiles:
      - spark-cluster
    command: >
      /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker -c 2 -m 1g spark://spark-master:7077
    depends_on:
      spark-master:
        condition: service_healthy
    environment:
      PYTHONPATH: /app
      KAFKA_BOOTSTRAP_SERVERS: broker:29092
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    mem_limit: 768m
    cpus: 1.0
    volumes:
      - .:/app
      - ./streaming:/opt/spark/apps
    networks:
      - market_platform_network

  candle-builder:
    image: spark:4.0.2-java21-python3
    container_name: candle-builder
    command: >
      /opt/spark/bin/spark-submit
      --master local[2]
      --driver-memory 768m
      --conf spark.sql.shuffle.partitions=2
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.780
      /opt/spark/apps/stream_candle_builder.py
    depends_on:
      broker:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
      minio:
        condition: service_started
      minio-init:
        condition: service_completed_successfully
    environment:
      PYTHONPATH: /app
      KAFKA_BOOTSTRAP_SERVERS: broker:29092
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    mem_limit: 1024m
    cpus: 1.5
    user: root
    volumes:
      - .:/app
      - ./streaming:/opt/spark/apps
      - spark_ivy_cache:/root/.ivy2
    restart: unless-stopped
    networks:
      - market_platform_network
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_compose_spark.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml streaming/spark_utils.py tests/test_compose_spark.py
git commit -m "perf(spark): consolidate candle-builder to local[2] mode with 1024m limit"
```

---

### Task 4: Storage, Ingestion, and Airflow Resource Bounds

**Files:**
- Modify: `docker-compose.yml:109-163,251-341`
- Test: `tests/test_compose_bounds.py`

**Interfaces:**
- Consumes: `infra/clickhouse/config.d/memory.xml`.
- Produces: Bounded resource cgroups and tuned scheduler parameters across `minio`, `clickhouse`, `tick-ingestion`, `postgres`, and `airflow`.

- [ ] **Step 1: Write test for storage and airflow limits**

Create `tests/test_compose_bounds.py`:
```python
from pathlib import Path
import yaml


def test_compose_storage_and_airflow_bounds():
    compose_path = (
        Path(__file__).resolve().parent.parent / "docker-compose.yml"
    )
    with open(compose_path) as f:
        config = yaml.safe_load(f)

    services = config["services"]

    # MinIO
    minio = services["minio"]
    assert minio["mem_limit"] == "256m"
    assert minio["environment"]["MINIO_API_REQUESTS_MAX"] == "100"

    # ClickHouse
    ch = services["clickhouse"]
    assert ch["mem_limit"] == "768m"
    assert any("memory.xml" in v for v in ch["volumes"])

    # Tick Ingestion
    tick = services["tick-ingestion"]
    assert tick["mem_limit"] == "192m"

    # Postgres
    pg = services["postgres"]
    assert pg["mem_limit"] == "192m"
    assert "shared_buffers=64MB" in pg["command"]

    # Airflow
    af = services["airflow"]
    assert af["mem_limit"] == "1024m"
    env = af["environment"]
    assert env["AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL"] == "60"
    assert env["AIRFLOW__SCHEDULER__PARSING_PROCESSES"] == "1"
    assert env["AIRFLOW__CORE__PARALLELISM"] == "4"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_compose_bounds.py -v`
Expected: FAIL with KeyError or AssertionError on missing limits.

- [ ] **Step 3: Modify `docker-compose.yml` with bounds and tuning**

Update `minio`, `clickhouse`, `tick-ingestion`, `postgres`, and `airflow` blocks in [docker-compose.yml](file:///home/realvarez/projects/fin-projects/market_platform/docker-compose.yml):
```yaml
  minio:
    image: minio/minio:latest
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
      MINIO_API_REQUESTS_MAX: "100"
    mem_limit: 256m
    cpus: 0.5
    volumes:
      - minio_data:/data
    command: server /data --console-address ":9001"
    networks:
      - market_platform_network
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:9000/minio/health/live" ]
      interval: 30s
      timeout: 10s
      retries: 3

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse
    environment:
      CLICKHOUSE_USER: clickhouse
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
      CLICKHOUSE_PASSWORD: clickhouse 
    ports:
      - "8123:8123"
      - "9009:9000"
    mem_limit: 768m
    mem_reservation: 384m
    cpus: 1.0
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./infra/clickhouse/init.sql:/docker-entrypoint-initdb.d/init.sql
      - ./infra/clickhouse/config.d/memory.xml:/etc/clickhouse-server/config.d/memory.xml
    networks:
      - market_platform_network
    healthcheck:
      test: [ "CMD", "wget", "--spider", "-q", "http://localhost:8123/ping" ]
      interval: 30s
      timeout: 10s
      retries: 3

  tick-ingestion:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: tick-ingestion
    depends_on:
      broker:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
    environment:
      KAFKA_BOOTSTRAP_SERVERS: broker:29092
      SYMBOLS: "BTC-USD,NVDA,MSTR,NU,HIMS,MU"
    mem_limit: 192m
    cpus: 0.5
    restart: unless-stopped
    networks:
      - market_platform_network

  postgres:
    container_name: postgres_db
    image: postgres:16.0
    command: postgres -c shared_buffers=64MB -c max_connections=30
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    mem_limit: 192m
    mem_reservation: 96m
    cpus: 0.5
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - market_platform_network

  airflow:
    container_name: airflow
    build:
      context: .
      dockerfile: airflow/Dockerfile
    command: standalone
    depends_on:
      postgres:
        condition: service_started
      broker:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: 'true' 
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'false'
      AIRFLOW__LOGGING__LOGGING_LEVEL: INFO
      AIRFLOW__WEBSERVER__SECRET_KEY: 'developer-secret-key'
      AIRFLOW__API__SECRET_KEY: 'developer-secret-key'
      AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL: "60"
      AIRFLOW__SCHEDULER__PARSING_PROCESSES: "1"
      AIRFLOW__CORE__PARALLELISM: "4"
      AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: "2"
      AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: "1"
      KAFKA_BOOTSTRAP_SERVERS: broker:29092
      PYTHONPATH: /opt/airflow
    mem_limit: 1024m
    mem_reservation: 512m
    cpus: 1.5
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./ingestion:/opt/airflow/ingestion
      - ./streaming:/opt/airflow/streaming
      - ./storage:/opt/airflow/storage
      - ./analysis:/opt/airflow/analysis
      - ./schemas:/opt/airflow/schemas
      - ./utils:/opt/airflow/utils
    ports:
      - "8080:8080"
    networks:
      - market_platform_network
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_compose_bounds.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml tests/test_compose_bounds.py
git commit -m "perf(compose): set memory limits and scheduler tuning across storage and airflow"
```

---

### Task 5: Documentation & End-to-End Test Suite

**Files:**
- Modify: `docs/services.md`
- Test: Full unit test suite (`uv run pytest tests/ -v`)
- Lint: `uv run ruff check .` and `uv run ruff format --check .`

**Interfaces:**
- Consumes: Updated [docker-compose.yml](file:///home/realvarez/projects/fin-projects/market_platform/docker-compose.yml).
- Produces: Updated service documentation reflecting the ~3.2 GB profile topology and all unit tests passing.

- [ ] **Step 1: Update `docs/services.md`**

Update the services reference table, resource budget table, and Web UIs section to document AKHQ (`port 9021`), the standalone `candle-builder`, and the new profiles (`ui`, `spark-cluster`).

- [ ] **Step 2: Run Ruff linting and formatting**

Run: `uv run ruff check . && uv run ruff format --check .`
Expected: Clean check with zero errors.

- [ ] **Step 3: Run complete pytest test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests pass (existing 48 tests + 3 new compose/config tests).

- [ ] **Step 4: Commit**

```bash
git add docs/services.md
git commit -m "docs(services): update services reference and resource allocations"
```
