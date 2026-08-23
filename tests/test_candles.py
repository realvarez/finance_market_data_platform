"""Tests for historical candle range fetching (backfill)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from ingestion import candles as candles_mod


def test_fetch_candles_range_chunks(monkeypatch):
    calls = []

    def fake_fetch_symbol(producer, symbol, interval, start, end):
        calls.append((symbol, interval, start, end))
        return len(calls)  # distinct count per call

    monkeypatch.setattr(candles_mod, "_fetch_symbol_candles", fake_fetch_symbol)
    monkeypatch.setattr(candles_mod, "create_sync_producer", MagicMock())

    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    end = datetime(2026, 8, 20, tzinfo=timezone.utc)  # 19 days -> 7 + 7 + 5 day chunks

    total = candles_mod.fetch_candles_range(["NVDA"], interval="1m", start=start, end=end)

    assert total == 1 + 2 + 3
    assert len(calls) == 3
    assert calls[0][2] == start
    assert calls[-1][3] == end
    for _, _, win_start, win_end in calls:
        assert win_start < win_end <= end
        assert (win_end - win_start) <= timedelta(days=7)


def test_fetch_candles_range_requires_start():
    try:
        candles_mod.fetch_candles_range(["NVDA"], interval="1m", start=None)
        raised = False
    except ValueError:
        raised = True
    assert raised


def test_multiple_symbols_per_chunk(monkeypatch):
    symbols_seen = []
    monkeypatch.setattr(
        candles_mod,
        "_fetch_symbol_candles",
        lambda producer, symbol, interval, start, end: symbols_seen.append(symbol) or 0,
    )
    monkeypatch.setattr(candles_mod, "create_sync_producer", MagicMock())

    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    end = datetime(2026, 8, 3, tzinfo=timezone.utc)
    candles_mod.fetch_candles_range(["AAPL", "SPY"], interval="5m", start=start, end=end)

    assert symbols_seen == ["AAPL", "SPY"]
