"""Shared consumer loop for long-running Kafka -> storage sink services (ADR-003).

Sinks must run as standalone containers, never as Airflow tasks, so each one needs the same
mechanics: wait for the broker, handle shutdown signals, batch writes, and flush on exit. Only
the topics and the write function differ, so those are the only things each sink supplies.
"""

import json
import logging
import signal
import threading
import time
from collections.abc import Callable

from kafka import KafkaConsumer

from ingestion import config

logger = logging.getLogger(__name__)

BATCH_MAX_MESSAGES = 500
BATCH_FLUSH_SECONDS = 10.0


def create_consumer(topics: list[str], group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        *topics,
        bootstrap_servers=[config.KAFKA_BOOTSTRAP_SERVERS],
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


def create_consumer_with_retry(
    consumer_factory: Callable[[], KafkaConsumer],
    initial_delay: float = 2.0,
    max_delay: float = 30.0,
    sleep=time.sleep,
) -> KafkaConsumer:
    """Retry consumer creation with exponential backoff until the broker is reachable."""
    delay = initial_delay
    while True:
        try:
            return consumer_factory()
        except Exception:
            logger.warning("Kafka not reachable yet; retrying in %.0fs", delay)
            sleep(delay)
            delay = min(delay * 2, max_delay)


def run_consumer_sink(
    topics: list[str],
    group_id: str,
    on_batch: Callable[[str, list[dict]], None],
    batch_max_messages: int = BATCH_MAX_MESSAGES,
    flush_seconds: float = BATCH_FLUSH_SECONDS,
) -> None:
    """Consume `topics` continuously, handing each flushed batch to `on_batch(topic, records)`.

    Offsets are committed via the consumer group, so the archive resumes where it left off after a
    restart. Batches are bucketed per topic because sinks choose their destination from the topic
    the record arrived on.
    """
    consumer = create_consumer_with_retry(
        consumer_factory=lambda: create_consumer(topics, group_id)
    )
    stop = threading.Event()

    def handle_signal(signum, _frame):
        logger.info("Received signal %s, shutting down %s", signum, group_id)
        stop.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    batches: dict[str, list[dict]] = {}
    last_flush = time.monotonic()
    total = 0

    logger.info("Sink %s consuming %s", group_id, ", ".join(topics))
    try:
        while not stop.is_set():
            polled = consumer.poll(timeout_ms=1000, max_records=batch_max_messages)
            for messages in polled.values():
                for message in messages:
                    batches.setdefault(message.topic, []).append(message.value)

            flush_due = time.monotonic() - last_flush >= flush_seconds
            pending = sum(len(b) for b in batches.values())
            if pending and (pending >= batch_max_messages or flush_due):
                for topic, records in batches.items():
                    if records:
                        on_batch(topic, records)
                        total += len(records)
                        logger.debug("Flushed %d records from %s", len(records), topic)
                batches = {}
                last_flush = time.monotonic()

        for topic, records in batches.items():
            if records:
                on_batch(topic, records)
                total += len(records)
    finally:
        consumer.close()
        logger.info("Sink %s stopped after persisting %d records", group_id, total)
