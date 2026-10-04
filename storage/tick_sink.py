import logging

from ingestion import config
from storage.kafka_sink import (
    BATCH_FLUSH_SECONDS,
    BATCH_MAX_MESSAGES,
    create_consumer_with_retry,
    run_consumer_sink,
)
from storage.sinks import sink_ticks

logger = logging.getLogger(__name__)

CONSUMER_GROUP_ID = "tick-sink"

# Re-exported under its original private name so the retry behaviour has one tested implementation
# shared with the candle sink, while existing tests keep addressing it here.
_create_consumer_with_retry = create_consumer_with_retry


def run_tick_sink(
    batch_max_messages: int = BATCH_MAX_MESSAGES,
    flush_seconds: float = BATCH_FLUSH_SECONDS,
) -> None:
    """Consume market.ticks continuously and persist ticks to MinIO + ClickHouse.

    Long-running service (ADR-003): must run as a standalone container, never inside an Airflow
    task. Offsets are committed via the consumer group so the archive resumes where it left off
    after a restart.
    """
    run_consumer_sink(
        topics=[config.TOPIC_TICKS],
        group_id=CONSUMER_GROUP_ID,
        on_batch=lambda _topic, records: sink_ticks(records),
        batch_max_messages=batch_max_messages,
        flush_seconds=flush_seconds,
    )
