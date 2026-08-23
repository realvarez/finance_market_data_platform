"""Unit tests for candle reconciliation logic (pure functions only)."""

import pytest

from streaming.reconciliation import _candle_key, _within_tolerance, reconcile_candle


def make_raw(**overrides) -> dict:
    candle = {
        "event_id": "raw-1",
        "symbol": "NVDA",
        "interval": "1m",
        "timestamp": "2026-08-22T15:30:00Z",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 5000,
        "source": "yahoo_finance",
    }
    candle.update(overrides)
    return candle


def make_calc(**overrides) -> dict:
    candle = {
        "event_id": "calc-1",
        "symbol": "NVDA",
        "interval": "1m",
        "timestamp": "2026-08-22T15:30:00Z",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 5000,
        "source": "spark_streaming",
    }
    candle.update(overrides)
    return candle


class TestCandleKey:
    def test_key_combines_symbol_interval_timestamp(self):
        assert _candle_key(make_raw()) == "NVDA:1m:2026-08-22T15:30:00Z"


class TestWithinTolerance:
    def test_identical_values(self):
        assert _within_tolerance(100.0, 100.0, 0.01)

    def test_small_difference_within_tolerance(self):
        assert _within_tolerance(100.0, 100.9, 0.01)

    def test_large_difference_beyond_tolerance(self):
        assert not _within_tolerance(100.0, 105.0, 0.01)

    def test_both_zero(self):
        assert _within_tolerance(0.0, 0.0, 0.05)

    def test_zero_vs_nonzero_uses_absolute_floor(self):
        # tolerance * max(0, x, 1) = 0.05 -> within
        assert _within_tolerance(0.0, 0.04, 0.05)
        assert not _within_tolerance(0.0, 0.10, 0.05)


class TestReconcileCandle:
    def test_matching_candles_marked_matched(self):
        result = reconcile_candle(make_raw(), make_calc())
        assert result["source"] == "reconciled"
        assert result["reconciliation_status"] == "matched"
        assert result["event_id"] == "raw-1"  # raw wins as base

    def test_mismatched_close_marked_corrected(self):
        result = reconcile_candle(make_raw(), make_calc(close=110.0))
        assert result["reconciliation_status"] == "corrected"
        assert result["source"] == "reconciled"

    def test_volume_mismatch_also_corrects(self):
        result = reconcile_candle(make_raw(volume=5000), make_calc(volume=9000))
        assert result["reconciliation_status"] == "corrected"

    def test_raw_only(self):
        result = reconcile_candle(make_raw(), None)
        assert result["reconciliation_status"] == "raw_only"
        assert result["source"] == "reconciled"

    def test_calculated_only(self):
        result = reconcile_candle(None, make_calc())
        assert result["reconciliation_status"] == "calculated_only"
        assert result["source"] == "reconciled"

    def test_both_none_raises(self):
        with pytest.raises(ValueError, match="Both raw and calculated"):
            reconcile_candle(None, None)
