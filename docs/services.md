# Services Reference

## Docker Compose Services

| Service | Container | Host Port | Internal Port | Memory Limit | CPU Limit | Profile | Purpose |
|:---|:---|:---|:---|:---|:---|:---|:---|
| broker | broker | 9092, 9101 | 29092, 9101 | 768 MB | 1.0 core | default | Kafka broker (KRaft, `-Xms256m -Xmx512m`) |
| kafka-init | kafka-init | — | — | 128 MB | 0.5 core | default | One-shot topic bootstrap |
| akhq | akhq | 9021 | 8080 | 384 MB | 0.5 core | `ui` | Lightweight Kafka web UI (`-Xms128m -Xmx256m`) |
| airflow | airflow | 8080 | 8080 | 1024 MB | 1.5 cores | default | Airflow standalone (tuned 60s parser) |
| postgres | postgres_db | — | 5432 | 192 MB | 0.5 core | default | Airflow metadata DB (`shared_buffers=64MB`) |
| minio | minio | 9000, 9001 | 9000, 9001 | 256 MB | 0.5 core | default | S3-compatible object storage |
| minio-init | minio-init | — | — | 128 MB | 0.5 core | default | One-shot bucket initialization |
| clickhouse | clickhouse | 8123, 9009 | 8123, 9000 | 768 MB | 1.0 core | default | Analytical database (512MB max memory) |
| candle-builder | candle-builder | — | — | 1024 MB | 1.5 cores | default | Spark streaming standalone (`--master local[2]`) |
| tick-ingestion | tick-ingestion | — | — | 192 MB | 0.5 core | default | Yahoo WebSocket tick service |
| tick-sink | tick-sink | — | — | 192 MB | 0.5 core | default | *(Stage 1)* market.ticks → MinIO / ClickHouse |
| spark-master | spark-master | 8082, 7077 | 8080, 7077 | 384 MB | 0.5 core | `spark-cluster` | Optional distributed Spark master |
| spark-worker | spark-worker | — | 8081 | 768 MB | 1.0 core | `spark-cluster` | Optional distributed Spark worker |

## Web UIs

| Service | URL | Profile | Credentials |
|:---|:---|:---|:---|
| Airflow | http://localhost:8080 | default | — (dev mode: all-admins, no login) |
| AKHQ (Kafka UI) | http://localhost:9021 | `ui` | — |
| MinIO Console | http://localhost:9001 | default | minioadmin / minioadmin |
| Spark Master | http://localhost:8082 | `spark-cluster` | — |

## API Endpoints

| Service | URL | Protocol |
|:---|:---|:---|
| ClickHouse HTTP | http://localhost:8123 | HTTP |
| MinIO S3 API | http://localhost:9000 | S3-compatible |

## Network

All services communicate on the `market_platform_network` Docker bridge network.

Internal hostnames match service names (e.g. `broker`, `minio`, `clickhouse`).

## Volumes

| Volume | Service | Purpose |
|:---|:---|:---|
| `kafka_data` | broker | Kafka log segments |
| `minio_data` | minio | Object storage data |
| `clickhouse_data` | clickhouse | ClickHouse data |
| `postgres_data` | postgres | Airflow metadata |
| `spark_ivy_cache` | candle-builder | Maven/Ivy package cache |

## Health Checks

```bash
# Kafka
docker compose exec broker nc -z localhost 9092

# Airflow
curl -f http://localhost:8080/health

# ClickHouse
curl 'http://localhost:8123/?query=SELECT%201'

# MinIO
curl -f http://localhost:9000/minio/health/live
```

## Resource Usage & Profiles

### Default Core Stack (~3.2 GB RAM)
Starts only the essential streaming data pipeline:
```bash
docker compose up -d
```

### With Topic Inspector UI (~3.3 GB RAM)
Adds the lightweight AKHQ management dashboard:
```bash
docker compose --profile ui up -d
```

### With Distributed Spark Cluster (~4.3 GB RAM)
Starts multi-container Spark master/worker daemons:
```bash
docker compose --profile spark-cluster up -d
```

## Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f tick-ingestion
docker compose logs -f airflow
docker compose logs -f candle-builder
```

## Restart Individual Services

```bash
docker compose restart tick-ingestion
docker compose restart airflow
docker compose up -d --build tick-ingestion
```
