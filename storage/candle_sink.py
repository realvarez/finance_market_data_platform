"""Persist candle topics to MinIO + ClickHouse.

Neither candle topic had a storage consumer, so ClickHouse `market_candles` stayed empty and
reconciliation, data quality, and feature engineering all read nothing (debt #1 in
CURRENT_STATE.md).

Two things make this more than a copy of the tick sink:

* Records are routed by the topic they arrived on. `sink_candles` picks its MinIO prefix from the
  source, so a raw and a calculated batch must not be written together.
* Records are validated here. The Spark candle builder publishes to `market.candles.calculated`
  without ever calling `validate_candle`, so this is the last checkpoint before storage. Failures
  go to `market.errors` rather than being written (ADR-004).
"""

import logging

from kafka import KafkaProducer

from ingestion import config
from ingestion.kafka_utils import create_sync_producer, send_error_sync
from ingestion.schema_validator import validate_candle
from storage.kafka_sink import (
    BATCH_FLUSH_SECONDS,
    BATCH_MAX_MESSAGES,
    create_consumer_with_retry,
    run_consumer_sink,
)
from storage.sinks import sink_candles

logger = logging.getLogger(__name__)

CONSUMER_GROUP_ID = "candle-sink"

TOPIC_SOURCE = {
    config.TOPIC_CANDLES_RAW: config.SOURCE_YAHOO,
    config.TOPIC_CANDLES_CALCULATED: config.SOURCE_SPARK,
}

_create_consumer_with_retry = create_consumer_with_retry


def sink_candle_batch(
    topic: str,
    records: list[dict],
    error_producer: KafkaProducer | None = None,
) -> tuple[int, int]:
    """Write one batch, routing malformed candles to market.errors. Returns (written, rejected)."""
    source = TOPIC_SOURCE.get(topic, config.SOURCE_YAHOO)
    valid: list[dict] = []
    rejected = 0

    for record in records:
        try:
            valid.append(validate_candle(record))
        except Exception as exc:
            rejected += 1
            logger.warning("Rejected candle from %s: %s", topic, exc)
            if error_producer is not None:
                send_error_sync(error_producer, record, str(exc))

    if valid:
        sink_candles(valid, source=source)
    return len(valid), rejected


def run_candle_sink(
    batch_max_messages: int = BATCH_MAX_MESSAGES,
    flush_seconds: float = BATCH_FLUSH_SECONDS,
) -> None:
    """Consume both candle topics continuously and persist them (long-running service, ADR-003)."""
    error_producer = create_sync_producer()
    try:
        run_consumer_sink(
            topics=list(TOPIC_SOURCE),
            group_id=CONSUMER_GROUP_ID,
            on_batch=lambda topic, records: sink_candle_batch(topic, records, error_producer),
            batch_max_messages=batch_max_messages,
            flush_seconds=flush_seconds,
        )
    finally:
        error_producer.close()
