import logging
from datetime import datetime, timezone

from ingestion import config
from storage.clickhouse_client import query_candles

logger = logging.getLogger(__name__)


def check_ohlc_validity(candle: dict) -> list[str]:
    errors = []
    o, h, low, c = candle["open"], candle["high"], candle["low"], candle["close"]
    if low > o:
        errors.append(f"low ({low}) > open ({o})")
    if low > c:
        errors.append(f"low ({low}) > close ({c})")
    if h < o:
        errors.append(f"high ({h}) < open ({o})")
    if h < c:
        errors.append(f"high ({h}) < close ({c})")
    if candle.get("volume", 0) < 0:
        errors.append("volume is negative")
    return errors


def check_completeness(candles: list[dict], expected_count: int) -> dict:
    actual = len(candles)
    return {
        "check": "completeness",
        "expected": expected_count,
        "actual": actual,
        "passed": actual >= expected_count,
    }


def check_uniqueness(candles: list[dict]) -> dict:
    keys = [f"{c['symbol']}:{c['interval']}:{c['timestamp']}" for c in candles]
    duplicates = len(keys) - len(set(keys))
    return {
        "check": "uniqueness",
        "duplicates": duplicates,
        "passed": duplicates == 0,
    }


def check_freshness(candles: list[dict], max_age_minutes: int = 10) -> dict:
    if not candles:
        return {"check": "freshness", "passed": False, "reason": "no candles"}
    latest = max(candles, key=lambda c: c["timestamp"])
    latest_ts = datetime.fromisoformat(str(latest["timestamp"]))
    age_minutes = (datetime.now(timezone.utc) - latest_ts).total_seconds() / 60
    return {
        "check": "freshness",
        "latest_timestamp": str(latest["timestamp"]),
        "age_minutes": round(age_minutes, 2),
        "passed": age_minutes <= max_age_minutes,
    }


def check_ohlc_batch(candles: list[dict]) -> dict:
    invalid = [
        {"event_id": c.get("event_id"), "errors": errors}
        for c in candles
        if (errors := check_ohlc_validity(c))
    ]
    return {
        "check": "ohlc_validity",
        "invalid_count": len(invalid),
        "passed": not invalid,
        "details": invalid[:10],
    }


def run_checks(symbols: list[str] | None = None, interval: str = "1m") -> dict:
    symbols = symbols or config.DEFAULT_SYMBOLS
    all_results = {"timestamp": datetime.now(timezone.utc).isoformat(), "symbols": {}}

    for symbol in symbols:
        candles = query_candles(
            symbol, interval, config.SOURCE_RECONCILED, limit=100
        ) or query_candles(symbol, interval, config.SOURCE_YAHOO, limit=100)

        symbol_results = {
            "completeness": check_completeness(candles, expected_count=1),
            "uniqueness": check_uniqueness(candles),
            "freshness": check_freshness(candles),
            "ohlc_validity": check_ohlc_batch(candles),
        }
        symbol_results["all_passed"] = all(r["passed"] for r in symbol_results.values())
        all_results["symbols"][symbol] = symbol_results
        logger.info("Data quality for %s: all_passed=%s", symbol, symbol_results["all_passed"])

    return all_results
