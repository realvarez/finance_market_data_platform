import json
import logging
import signal
import threading
import time

from kafka import KafkaConsumer

from ingestion import config
from storage.sinks import sink_ticks

logger = logging.getLogger(__name__)

BATCH_MAX_MESSAGES = 500
BATCH_FLUSH_SECONDS = 10.0

CONSUMER_GROUP_ID = "tick-sink"


def _create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        config.TOPIC_TICKS,
        bootstrap_servers=[config.KAFKA_BOOTSTRAP_SERVERS],
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


def _create_consumer_with_retry(
    consumer_factory=None,
    initial_delay: float = 2.0,
    max_delay: float = 30.0,
    sleep=time.sleep,
) -> KafkaConsumer:
    """Retry consumer creation with exponential backoff until the broker is reachable."""
    factory = consumer_factory or _create_consumer
    delay = initial_delay
    while True:
        try:
            return factory()
        except Exception:
            logger.warning("Kafka not reachable yet; retrying in %.0fs", delay)
            sleep(delay)
            delay = min(delay * 2, max_delay)


def run_tick_sink(
    batch_max_messages: int = BATCH_MAX_MESSAGES,
    flush_seconds: float = BATCH_FLUSH_SECONDS,
) -> None:
    """Consume market.ticks continuously and persist ticks to MinIO + ClickHouse.

    Long-running service (ADR-003): must run as a standalone container, never
    inside an Airflow task. Offsets are committed via the consumer group so the
    archive resumes where it left off after a restart.
    """
    consumer = _create_consumer_with_retry()
    stop = threading.Event()

    def handle_signal(signum, _frame):
        logger.info("Received signal %s, shutting down tick sink", signum)
        stop.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    batch: list[dict] = []
    last_flush = time.monotonic()
    total = 0

    logger.info("Tick sink consuming %s -> MinIO/ClickHouse", config.TOPIC_TICKS)
    try:
        while not stop.is_set():
            polled = consumer.poll(timeout_ms=1000, max_records=batch_max_messages)
            for messages in polled.values():
                batch.extend(m.value for m in messages)

            flush_due = time.monotonic() - last_flush >= flush_seconds
            if batch and (len(batch) >= batch_max_messages or flush_due):
                sink_ticks(batch)
                total += len(batch)
                logger.debug("Flushed %d ticks (%d total)", len(batch), total)
                batch = []
                last_flush = time.monotonic()
        if batch:
            sink_ticks(batch)
            total += len(batch)
    finally:
        consumer.close()
        logger.info("Tick sink stopped after persisting %d ticks", total)
