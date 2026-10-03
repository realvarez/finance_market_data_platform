import logging

from ingestion import config
from storage import clickhouse_client, minio_client

logger = logging.getLogger(__name__)

# MinIO prefix per source. Calculated candles must not sit under raw/, where a reader would
# assume they are Yahoo data — they are Spark's reconstruction from ticks.
_PREFIX_BY_SOURCE = {
    config.SOURCE_RECONCILED: "reconciled/candles",
    config.SOURCE_SPARK: "calculated/candles",
}


def sink_ticks(records: list[dict]) -> None:
    if not records:
        return
    try:
        minio_client.write_parquet(records, "ticks")
    except Exception:
        logger.exception("Failed to sink ticks to MinIO")
    try:
        clickhouse_client.insert_ticks(records)
    except Exception:
        logger.exception("Failed to sink ticks to ClickHouse")


def sink_candles(records: list[dict], source: str = "raw") -> None:
    if not records:
        return
    prefix = _PREFIX_BY_SOURCE.get(source, "raw/candles")
    for record in records:
        try:
            minio_client.write_parquet(
                [record],
                "candles",
                prefix=prefix,
                interval=record.get("interval", "1m"),
                source=record.get("source", source),
            )
        except Exception:
            logger.exception("Failed to sink candle to MinIO")
    try:
        clickhouse_client.insert_candles(records)
    except Exception:
        logger.exception("Failed to sink candles to ClickHouse")


def sink_features(records: list[dict]) -> None:
    if not records:
        return
    try:
        clickhouse_client.insert_features(records)
    except Exception:
        logger.exception("Failed to sink features to ClickHouse")


def sink_signals(records: list[dict]) -> None:
    if not records:
        return
    try:
        clickhouse_client.insert_signals(records)
    except Exception:
        logger.exception("Failed to sink signals to ClickHouse")
