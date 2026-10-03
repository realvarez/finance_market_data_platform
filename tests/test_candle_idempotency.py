"""Reconciling the same candle key twice must leave one row (ADR-011)."""

import pytest

from storage.clickhouse_client import get_client, insert_candles, query_candles

CANDLE = {
    "event_id": "TESTIDEMPOTENCY",
    "symbol": "ZZTEST",
    "interval": "1m",
    "timestamp": "2020-01-01 00:00:00.000",
    "open": 1.0,
    "high": 1.0,
    "low": 1.0,
    "close": 1.0,
    "volume": 1.0,
    "source": "idempotency_test",
    "created_at": "2020-01-01 00:00:00.000",
}


def test_duplicate_candle_key_stores_one_row():
    try:
        get_client()
    except Exception as exc:
        pytest.skip(f"ClickHouse unavailable: {exc}")

    # Separate calls, so each insert lands in its own part: ReplacingMergeTree only collapses
    # them at merge time, and query_candles relies on FINAL to collapse them before that.
    insert_candles([CANDLE])
    insert_candles([CANDLE])

    rows = query_candles("ZZTEST", "1m", "idempotency_test", limit=10)
    assert len(rows) == 1, f"expected 1 row after duplicate insert, got {len(rows)}"
