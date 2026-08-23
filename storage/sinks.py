import logging

from storage import clickhouse_client, minio_client

logger = logging.getLogger(__name__)


def sink_ticks(records: list[dict]) -> None:
    try:
        minio_client.write_parquet(records, "ticks")
    except Exception:
        logger.exception("Failed to sink ticks to MinIO")
    try:
        clickhouse_client.insert_ticks(records)
    except Exception:
        logger.exception("Failed to sink ticks to ClickHouse")


def sink_candles(records: list[dict], source: str = "raw") -> None:
    prefix = f"raw/candles" if source != "reconciled" else "reconciled/candles"
    for record in records:
        extra = {
            "prefix": prefix,
            "interval": record.get("interval", "1m"),
            "source": record.get("source", source),
        }
        try:
            minio_client.write_parquet([record], "candles", **extra)
        except Exception:
            logger.exception("Failed to sink candle to MinIO")
    try:
        clickhouse_client.insert_candles(records)
    except Exception:
        logger.exception("Failed to sink candles to ClickHouse")


def sink_features(records: list[dict]) -> None:
    try:
        clickhouse_client.insert_features(records)
    except Exception:
        logger.exception("Failed to sink features to ClickHouse")
