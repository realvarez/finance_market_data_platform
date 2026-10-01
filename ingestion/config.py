import os
from pathlib import Path

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
SYMBOLS = os.getenv("SYMBOLS", "GOOGL,NVDA,AMZN,TSLA").split(",")
DEFAULT_SYMBOLS = [s.strip().upper() for s in SYMBOLS if s.strip()]

# Standard Topic Names (ADR-001 & ADR-002)
TOPIC_TICKS = "market.ticks"
TOPIC_CANDLES_RAW = "market.candles.raw"
TOPIC_CANDLES_CALCULATED = "market.candles.calculated"
TOPIC_CANDLES_RECONCILED = "market.candles.reconciled"
TOPIC_SIGNALS = "market.signals"
TOPIC_ERRORS = "market.errors"

# MinIO / S3 Settings
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "market-data")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_CHECKPOINT_BUCKET = os.getenv("MINIO_CHECKPOINT_BUCKET", MINIO_BUCKET)

# ClickHouse Settings
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "market_platform")

# Project Paths & Schemas
SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas" / "v1"

# Standard Source Identifiers
SOURCE_YAHOO = "yahoo_finance"
SOURCE_SPARK = "spark_streaming"
SOURCE_RECONCILED = "reconciled"
