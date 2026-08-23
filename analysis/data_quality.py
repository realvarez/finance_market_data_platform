import logging
from datetime import datetime, timezone

from storage.clickhouse_client import query_candles

logger = logging.getLogger(__name__)


def check_ohlc_validity(candle: dict) -> list[str]:
    errors = []
    o, h, l, c = candle["open"], candle["high"], candle["low"], candle["close"]
    if l > o:
        errors.append(f"low ({l}) > open ({o})")
    if l > c:
        errors.append(f"low ({l}) > close ({c})")
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
    latest_ts = datetime.fromisoformat(str(latest["timestamp"]).replace("Z", "+00:00"))
    age_minutes = (datetime.now(timezone.utc) - latest_ts).total_seconds() / 60
    return {
        "check": "freshness",
        "latest_timestamp": str(latest["timestamp"]),
        "age_minutes": round(age_minutes, 2),
        "passed": age_minutes <= max_age_minutes,
    }


def check_ohlc_batch(candles: list[dict]) -> dict:
    invalid = []
    for candle in candles:
        errors = check_ohlc_validity(candle)
        if errors:
            invalid.append({"event_id": candle.get("event_id"), "errors": errors})
    return {
        "check": "ohlc_validity",
        "invalid_count": len(invalid),
        "passed": len(invalid) == 0,
        "details": invalid[:10],
    }


def run_checks(symbols: list[str] | None = None, interval: str = "1m") -> dict:
    from ingestion import config

    symbols = symbols or config.DEFAULT_SYMBOLS
    all_results = {"timestamp": datetime.now(timezone.utc).isoformat(), "symbols": {}}

    for symbol in symbols:
        candles = query_candles(symbol, interval, "reconciled", limit=100)
        if not candles:
            candles = query_candles(symbol, interval, "yahoo_finance", limit=100)

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
