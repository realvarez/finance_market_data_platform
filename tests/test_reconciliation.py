"""Unit tests for candle reconciliation logic (pure functions only)."""

from datetime import datetime, timedelta, timezone

import pytest

from streaming.reconciliation import (
    _candle_key,
    _price_within_tolerance,
    _to_iso,
    _to_payload,
    _volume_within_tolerance,
    reconcile_candle,
)


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


class TestPriceTolerance:
    """Price tolerance is absolute. A proportional rule accepted a $6.00 error on a $600 stock."""

    def test_identical_values(self):
        assert _price_within_tolerance(100.0, 100.0)

    def test_one_cent_within_tolerance(self):
        assert _price_within_tolerance(100.0, 100.01)

    def test_five_cents_outside_tolerance(self):
        assert not _price_within_tolerance(100.0, 100.05)

    def test_expensive_asset_is_not_given_a_wider_leash(self):
        """Regression: 0.01 * 600 = $6.00 used to pass."""
        assert not _price_within_tolerance(600.0, 605.0)
        assert _price_within_tolerance(600.0, 600.005)

    def test_cheap_asset_uses_the_same_absolute_leash(self):
        assert not _price_within_tolerance(3.0, 3.02)


class TestVolumeTolerance:
    """Volume stays relative — legitimate rounding differs by more than a cent."""

    def test_identical(self):
        assert _volume_within_tolerance(5000, 5000)

    def test_four_percent_within(self):
        assert _volume_within_tolerance(1000, 1040)

    def test_forty_percent_outside(self):
        assert not _volume_within_tolerance(1000, 1400)

    def test_both_zero(self):
        assert _volume_within_tolerance(0, 0)


class TestPayloadNormalisation:
    def test_naive_clickhouse_datetime_becomes_iso_utc(self):
        assert _to_iso(datetime(2026, 8, 22, 15, 30)) == "2026-08-22T15:30:00Z"

    def test_aware_datetime_is_converted_to_utc(self):
        aware = datetime(2026, 8, 22, 17, 30, tzinfo=timezone(timedelta(hours=2)))
        assert _to_iso(aware) == "2026-08-22T15:30:00Z"

    def test_string_passes_through_unchanged(self):
        assert _to_iso("2026-08-22T15:30:00Z") == "2026-08-22T15:30:00Z"

    def test_payload_converts_both_time_fields(self):
        payload = _to_payload(
            {
                "timestamp": datetime(2026, 8, 22, 15, 30),
                "created_at": datetime(2026, 8, 22, 15, 31),
                "close": 1.0,
            }
        )
        assert payload["timestamp"] == "2026-08-22T15:30:00Z"
        assert payload["created_at"] == "2026-08-22T15:31:00Z"
        assert payload["close"] == 1.0


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
