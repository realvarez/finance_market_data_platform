# Services Reference

## Docker Compose Services

| Service | Container | Host Port | Internal Port | Purpose |
|---------|-----------|-----------|---------------|---------|
| broker | broker | 9092, 9101 | 29092, 9101 | Kafka broker (KRaft) |
| schema-registry | schema-registry | 8081 | 8081 | Confluent Schema Registry |
| control-center | control-center | 9021 | 9021 | Kafka Control Center UI |
| airflow | airflow | 8080 | 8080 | Airflow standalone |
| postgres | postgres_db | — | 5432 | Airflow metadata DB |
| minio | minio | 9000, 9001 | 9000, 9001 | S3-compatible object storage |
| clickhouse | clickhouse | 8123, 9009 | 8123, 9000 | Analytical database |
| spark-master | spark-master | 8082, 7077 | 8080, 7077 | Spark master |
| spark-worker | spark-worker | — | 8081 | Spark worker |
| tick-ingestion | tick-ingestion | — | — | Yahoo WebSocket tick service |
| kafka-init | kafka-init | — | — | One-shot topic bootstrap |

## Web UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | — (dev mode: all-admins, no login) |
| Kafka Control Center | http://localhost:9021 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master | http://localhost:8082 | — |

## API Endpoints

| Service | URL | Protocol |
|---------|-----|----------|
| Schema Registry | http://localhost:8081/subjects | REST |
| ClickHouse HTTP | http://localhost:8123 | HTTP |
| MinIO S3 API | http://localhost:9000 | S3-compatible |

## Network

All services communicate on the `market_platform_network` Docker bridge network.

Internal hostnames match service names (e.g. `broker`, `minio`, `clickhouse`).

## Volumes

| Volume | Service | Purpose |
|--------|---------|---------|
| `kafka_data` | broker | Kafka log segments |
| `minio_data` | minio | Object storage data |
| `clickhouse_data` | clickhouse | ClickHouse data |
| `postgres_data` | postgres | Airflow metadata |

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

## Resource Usage (Development Defaults)

| Service | Memory | CPU |
|---------|--------|-----|
| broker | ~512 MB | 1 core |
| airflow | ~1 GB | 1 core |
| spark-master | ~512 MB | 1 core |
| spark-worker | ~1 GB | 2 cores |
| clickhouse | ~512 MB | 1 core |
| minio | ~256 MB | 0.5 core |

Total recommended: 8 GB RAM, 4 CPU cores.

## Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f tick-ingestion
docker compose logs -f airflow
docker compose logs -f spark-master
```

## Restart Individual Services

```bash
docker compose restart tick-ingestion
docker compose restart airflow
docker compose up -d --build tick-ingestion
```
