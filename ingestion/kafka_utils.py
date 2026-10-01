import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from kafka import KafkaProducer

from ingestion import config

logger = logging.getLogger(__name__)


async def create_async_producer() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(bootstrap_servers=[config.KAFKA_BOOTSTRAP_SERVERS])
    await producer.start()
    return producer


def create_sync_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[config.KAFKA_BOOTSTRAP_SERVERS],
        max_block_ms=5000,
    )


async def send_async(producer: AIOKafkaProducer, topic: str, key: str, data: dict) -> None:
    try:
        await producer.send(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(data).encode("utf-8"),
        )
    except Exception:
        logger.exception("Failed to send message to %s", topic)
        await send_error_async(producer, data, str(topic))


def _build_error_payload(original: dict, reason: str) -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "original": original,
    }


async def send_error_async(producer: AIOKafkaProducer, original: dict, reason: str) -> None:
    error_msg = _build_error_payload(original, reason)
    try:
        await producer.send(
            topic=config.TOPIC_ERRORS,
            value=json.dumps(error_msg).encode("utf-8"),
        )
    except Exception:
        logger.exception("Failed to send error message")


def send_sync(producer: KafkaProducer, topic: str, key: str, data: dict) -> None:
    try:
        producer.send(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(data).encode("utf-8"),
        )
    except Exception:
        logger.exception("Failed to send message to %s", topic)
        send_error_sync(producer, data, str(topic))


def send_error_sync(producer: KafkaProducer, original: dict, reason: str) -> None:
    error_msg = _build_error_payload(original, reason)
    try:
        producer.send(
            topic=config.TOPIC_ERRORS,
            value=json.dumps(error_msg).encode("utf-8"),
        )
    except Exception:
        logger.exception("Failed to send error message")
