import json
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer

from ingestion import config
from ingestion.kafka_utils import create_sync_producer, send_sync
from ingestion.schema_validator import validate_candle
from storage.sinks import sink_candles

logger = logging.getLogger(__name__)

PRICE_TOLERANCE = 0.01  # 1 cent
VOLUME_TOLERANCE = 0.05  # 5%


def _consume_topic(topic: str, timeout_ms: int = 10000) -> list[dict]:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[config.KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset="latest",
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    messages = []
    for msg in consumer:
        messages.append(msg.value)
    consumer.close()
    return messages


def _candle_key(candle: dict) -> str:
    return f"{candle['symbol']}:{candle['interval']}:{candle['timestamp']}"


def _within_tolerance(a: float, b: float, tolerance: float) -> bool:
    if a == 0 and b == 0:
        return True
    return abs(a - b) <= tolerance * max(abs(a), abs(b), 1)


def reconcile_candle(raw: dict | None, calculated: dict | None) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    if raw and calculated:
        ohlcv_match = all(
            [
                _within_tolerance(raw["open"], calculated["open"], PRICE_TOLERANCE),
                _within_tolerance(raw["high"], calculated["high"], PRICE_TOLERANCE),
                _within_tolerance(raw["low"], calculated["low"], PRICE_TOLERANCE),
                _within_tolerance(raw["close"], calculated["close"], PRICE_TOLERANCE),
                _within_tolerance(raw["volume"], calculated["volume"], VOLUME_TOLERANCE),
            ]
        )
        base = raw if ohlcv_match else raw
        status = "matched" if ohlcv_match else "corrected"
        return {
            **base,
            "source": config.SOURCE_RECONCILED,
            "created_at": now,
            "reconciliation_status": status,
        }

    if raw:
        return {
            **raw,
            "source": config.SOURCE_RECONCILED,
            "created_at": now,
            "reconciliation_status": "raw_only",
        }

    if calculated:
        return {
            **calculated,
            "source": config.SOURCE_RECONCILED,
            "created_at": now,
            "reconciliation_status": "calculated_only",
        }

    raise ValueError("Both raw and calculated candles are None")


def reconcile_candles(symbols: list[str] | None = None) -> dict:
    """Compare raw vs calculated candles and publish reconciled results."""
    symbols = symbols or config.DEFAULT_SYMBOLS
    raw_messages = _consume_topic(config.TOPIC_CANDLES_RAW)
    calc_messages = _consume_topic(config.TOPIC_CANDLES_CALCULATED)

    if symbols:
        raw_messages = [m for m in raw_messages if m.get("symbol") in symbols]
        calc_messages = [m for m in calc_messages if m.get("symbol") in symbols]

    raw_index = {_candle_key(c): c for c in raw_messages}
    calc_index = {_candle_key(c): c for c in calc_messages}

    all_keys = set(raw_index.keys()) | set(calc_index.keys())
    producer = create_sync_producer()
    reconciled = []
    metrics = {"matched": 0, "corrected": 0, "raw_only": 0, "calculated_only": 0, "missing": 0}

    for key in all_keys:
        raw = raw_index.get(key)
        calc = calc_index.get(key)

        try:
            result = reconcile_candle(raw, calc)
            validated = validate_candle(result)
            send_sync(producer, config.TOPIC_CANDLES_RECONCILED, validated["symbol"], validated)
            reconciled.append(validated)
            status = validated.get("reconciliation_status", "unknown")
            if status in metrics:
                metrics[status] += 1
        except Exception:
            logger.exception("Failed to reconcile candle %s", key)
            metrics["missing"] += 1

    producer.flush()
    producer.close()

    if reconciled:
        sink_candles(reconciled, source="reconciled")

    logger.info("Reconciliation complete: %s", metrics)
    return metrics
