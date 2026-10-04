"""Compare calculated candles against Yahoo's official candles (ADR-007, ADR-009).

Both series are read from ClickHouse over an explicit window rather than consumed from Kafka. The
previous implementation read topic offsets with a 10-second window on a 5-minute DAG, so it usually
reconciled nothing — and because `analysis/features.py` silently falls back to raw candles, nothing
downstream ever noticed. Kafka is transport, not a store; see ADR-009.
"""

import logging
from datetime import datetime, timedelta, timezone

from ingestion import config
from ingestion.kafka_utils import create_sync_producer, send_sync
from ingestion.schema_validator import validate_candle
from storage.clickhouse_client import query_candles_window
from storage.sinks import sink_candles

logger = logging.getLogger(__name__)

# Price is compared absolutely: a proportional tolerance would accept a $6.00 error on a $600
# stock, which is a discrepancy this process exists to catch.
PRICE_TOLERANCE = 0.01
VOLUME_TOLERANCE = 0.05  # relative — volume differs legitimately with venue rounding
# Binary floats cannot represent 100.01 exactly: abs(100.0 - 100.01) is 0.010000000000005116,
# which would reject a difference of exactly one cent.
_EPSILON = 1e-9


def _price_within_tolerance(a: float, b: float, tolerance: float = PRICE_TOLERANCE) -> bool:
    return abs(a - b) <= tolerance + _EPSILON


def _volume_within_tolerance(a: float, b: float, tolerance: float = VOLUME_TOLERANCE) -> bool:
    if a == 0 and b == 0:
        return True
    return abs(a - b) <= tolerance * max(abs(a), abs(b), 1)


def _candle_key(candle: dict) -> str:
    return f"{candle['symbol']}:{candle['interval']}:{candle['timestamp']}"


def _to_iso(value) -> str:
    """ClickHouse returns DateTime64 as a naive datetime in the server's timezone. Payloads carry
    ISO-8601 UTC strings (AGENTS.md), and the candle schema types these fields as strings."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def _to_payload(candle: dict) -> dict:
    payload = dict(candle)
    for field in ("timestamp", "created_at"):
        if field in payload:
            payload[field] = _to_iso(payload[field])
    return payload


def reconcile_candle(raw: dict | None, calculated: dict | None) -> dict:
    if not raw and not calculated:
        raise ValueError("Both raw and calculated candles are None")

    if raw and calculated:
        ohlc_match = all(
            _price_within_tolerance(raw[k], calculated[k]) for k in ("open", "high", "low", "close")
        ) and _volume_within_tolerance(raw["volume"], calculated["volume"])
        base, status = raw, ("matched" if ohlc_match else "corrected")
    elif raw:
        base, status = raw, "raw_only"
    else:
        base, status = calculated, "calculated_only"

    return {
        **base,
        "source": config.SOURCE_RECONCILED,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reconciliation_status": status,
    }


def reconcile_candles(
    symbols: list[str] | None = None,
    intervals: tuple[str, ...] = ("1m", "5m"),
    lookback_minutes: int = 10,
) -> dict:
    """Compare both candle series over a window and publish the reconciled result.

    Re-running over an overlapping window is safe: `market_candles` is a ReplacingMergeTree keyed by
    (symbol, interval, timestamp, source) and reads use FINAL (ADR-011).
    """
    symbols = symbols or config.DEFAULT_SYMBOLS
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=lookback_minutes)

    metrics = {"matched": 0, "corrected": 0, "raw_only": 0, "calculated_only": 0, "missing": 0}
    raw_total = 0
    reconciled: list[dict] = []
    producer = create_sync_producer()

    try:
        for symbol in symbols:
            for interval in intervals:
                raw = {
                    _candle_key(c): c
                    for c in query_candles_window(symbol, interval, config.SOURCE_YAHOO, start, end)
                }
                calculated = {
                    _candle_key(c): c
                    for c in query_candles_window(symbol, interval, config.SOURCE_SPARK, start, end)
                }
                raw_total += len(raw)

                for key in sorted(set(raw) | set(calculated)):
                    try:
                        result = _to_payload(reconcile_candle(raw.get(key), calculated.get(key)))
                        validated = validate_candle(result)
                        send_sync(
                            producer,
                            config.TOPIC_CANDLES_RECONCILED,
                            validated["symbol"],
                            validated,
                        )
                        reconciled.append(validated)
                        status = validated.get("reconciliation_status", "unknown")
                        metrics[status] = metrics.get(status, 0) + 1
                    except Exception:
                        logger.exception("Failed to reconcile candle %s", key)
                        metrics["missing"] += 1

        producer.flush()
    finally:
        producer.close()

    if reconciled:
        sink_candles(reconciled, source=config.SOURCE_RECONCILED)

    coverage = (raw_total - metrics["raw_only"]) / raw_total if raw_total else None
    logger.info(
        "Reconciled window %s..%s across %d symbols x %s: %s (raw=%d, coverage=%s)",
        start.isoformat(),
        end.isoformat(),
        len(symbols),
        ",".join(intervals),
        metrics,
        raw_total,
        "n/a" if coverage is None else f"{coverage:.3f}",
    )
    return metrics
