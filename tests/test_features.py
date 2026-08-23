"""Unit tests for feature engineering math (EMA, RSI, ATR, VWAP, returns)."""

import pytest

from analysis.features import (
    _compute_atr,
    _compute_ema,
    _compute_relative_volume,
    _compute_rsi,
    _compute_vwap,
    compute_features,
)


class TestComputeEma:
    def test_matches_hand_computed_series(self):
        # EMA(3) over [1..10] with multiplier 0.5 seeded from prices[0]
        prices = [float(i) for i in range(1, 11)]
        assert _compute_ema(prices, 3) == pytest.approx(9.001953125)

    def test_insufficient_data_returns_none(self):
        assert _compute_ema([1.0, 2.0], 9) is None

    def test_constant_series_converges_to_constant(self):
        assert _compute_ema([42.0] * 21, 21) == pytest.approx(42.0)


class TestComputeRsi:
    # Classic StockCharts ChartSchool example (14-period Wilder RSI).
    WILDER_PRICES = [
        44.34,
        44.09,
        44.15,
        43.61,
        44.33,
        44.83,
        45.10,
        45.42,
        45.84,
        46.08,
        45.89,
        46.03,
        45.61,
        46.28,
        46.28,
    ]

    def test_matches_wilder_reference_value(self):
        # avg_gain = 3.34/14, avg_loss = 1.40/14 -> RSI ~= 70.4641
        rsi = _compute_rsi(self.WILDER_PRICES, 14)
        assert rsi == pytest.approx(70.4641, abs=1e-3)

    def test_all_gains_is_overbought(self):
        prices = [float(i) for i in range(1, 31)]
        assert _compute_rsi(prices, 14) == 100.0

    def test_all_losses_is_oversold(self):
        prices = [float(i) for i in range(30, 0, -1)]
        assert _compute_rsi(prices, 14) == 0.0

    def test_flat_series_treated_as_no_loss(self):
        # Preserves existing edge-case convention: zero losses -> 100.
        assert _compute_rsi([100.0] * 20, 14) == 100.0

    def test_insufficient_data_returns_none(self):
        assert _compute_rsi([1.0] * 14, 14) is None

    def test_uses_full_history_not_last_window(self):
        # Wilder smoothing must differ from a simple average of the last N deltas;
        # an early spike's influence decays but still shifts the result.
        long = self.WILDER_PRICES + [46.50]  # one extra gain after the seed window
        plain_avg = 70.4641  # simple-average value of the seed window alone
        assert _compute_rsi(long, 14) != pytest.approx(plain_avg, abs=1e-6)


class TestComputeAtr:
    def test_constant_range(self):
        # Every candle spans exactly 2 points -> every TR is 2.
        candles = make_candles(20)
        assert _compute_atr(candles, 14) == pytest.approx(2.0)

    def test_wilder_smoothing_over_seed_window(self):
        candles = [
            {"high": 100.5, "low": 99.5, "close": 100},  # seed candle, TR unused
            {"high": 103.0, "low": 99.0, "close": 102},  # TR = max(4, 3, 1) = 4
            {"high": 102.5, "low": 100.0, "close": 101},  # TR = max(2.5, 0.5, 2) = 2.5
        ]
        # period=1: seed = 4, then Wilder step (4*0 + 2.5)/1 = 2.5
        assert _compute_atr(candles, 1) == pytest.approx(2.5)

    def test_insufficient_data_returns_none(self):
        assert _compute_atr(make_candles(10), 14) is None


class TestRelativeVolume:
    def test_spike_against_flat_baseline(self):
        volumes = [100.0] * 20 + [300.0]
        assert _compute_relative_volume(volumes) == pytest.approx(3.0)

    def test_latest_excluded_from_baseline(self):
        volumes = [100.0] * 21
        assert _compute_relative_volume(volumes) == pytest.approx(1.0)

    def test_zero_baseline_returns_none(self):
        assert _compute_relative_volume([0.0] * 5 + [10.0]) is None


class TestComputeVwap:
    def test_weighted_typical_price(self):
        candles = [
            {"high": 10.0, "low": 8.0, "close": 9.0, "volume": 100},  # typical 9
            {"high": 12.0, "low": 10.0, "close": 11.0, "volume": 300},  # typical 11
        ]
        # (9*100 + 11*300) / 400 = 10.5
        assert _compute_vwap(candles) == pytest.approx(10.5)

    def test_no_volume_returns_none(self):
        candles = [{"high": 10.0, "low": 8.0, "close": 9.0, "volume": 0}]
        assert _compute_vwap(candles) is None


def make_candles(n: int = 30, volume: float = 1000.0) -> list[dict]:
    """Deterministic uptrend candles used as fixtures."""
    candles = []
    for i in range(n):
        base = 100.0 + i
        candles.append(
            {
                "event_id": f"evt-{i}",
                "symbol": "TEST",
                "interval": "1m",
                "timestamp": f"2026-01-01T00:{i:02d}:00Z",
                "open": base,
                "high": base + 1.0,
                "low": base - 1.0,
                "close": base + 0.5,
                "volume": volume,
                "source": "reconciled",
                "created_at": f"2026-01-01T00:{i:02d}:05Z",
            }
        )
    return candles


class TestComputeFeatures:
    def test_feature_names_and_values(self, monkeypatch):
        candles = make_candles()
        monkeypatch.setattr("analysis.features.query_candles", lambda *a, **k: candles)

        features = compute_features("TEST", market_context={})
        by_name = {f["feature_name"]: f["feature_value"] for f in features}

        expected_names = {
            "return_1",
            "return_5",
            "log_return_1",
            "candle_body",
            "upper_wick",
            "lower_wick",
            "ema_9",
            "ema_21",
            "rsi_14",
            "atr_14",
            "rolling_std_10",
            "relative_volume",
            "vwap",
        }
        assert expected_names <= set(by_name.keys())

        closes = [c["close"] for c in candles]
        assert by_name["return_1"] == pytest.approx((closes[-1] - closes[-2]) / closes[-2])
        assert by_name["ema_9"] == pytest.approx(_compute_ema(closes, 9))
        # Strictly increasing closes -> RSI pinned at 100; constant 2-point range -> ATR 2.
        assert by_name["rsi_14"] == 100.0
        assert by_name["atr_14"] == pytest.approx(_compute_atr(candles, 14))
        latest = candles[-1]
        assert by_name["candle_body"] == pytest.approx(latest["close"] - latest["open"])
        assert by_name["upper_wick"] == pytest.approx(latest["high"] - latest["close"])
        # Flat volume baseline -> relative volume 1.0.
        assert by_name["relative_volume"] == pytest.approx(1.0)
        assert by_name["vwap"] > 0

    def test_market_context_attached(self, monkeypatch):
        candles = make_candles()
        monkeypatch.setattr("analysis.features.query_candles", lambda *a, **k: candles)

        features = compute_features("TEST", market_context={"context_spy_return_1": 0.01})
        by_name = {f["feature_name"]: f["feature_value"] for f in features}
        assert by_name["context_spy_return_1"] == pytest.approx(0.01)

    def test_context_auto_computed_when_not_provided(self, monkeypatch):
        # Without explicit market_context, SPY/QQQ are queried and attached.
        symbols_queried = []

        def fake_query(symbol, interval, source, limit=None):
            symbols_queried.append(symbol)
            return make_candles()

        monkeypatch.setattr("analysis.features.query_candles", fake_query)
        features = compute_features("TEST")
        names = {f["feature_name"] for f in features}
        assert {"SPY", "QQQ"} <= set(symbols_queried)
        assert "context_spy_return_1" in names
        assert "context_qqq_return_1" in names

    def test_falls_back_to_yahoo_source_when_reconciled_missing(self, monkeypatch):
        calls = []

        def fake_query(symbol, interval, source, limit=None):
            calls.append((symbol, source))
            return make_candles() if source == "yahoo_finance" and symbol == "TEST" else []

        monkeypatch.setattr("analysis.features.query_candles", fake_query)
        features = compute_features("TEST")
        assert ("TEST", "reconciled") in calls
        assert ("TEST", "yahoo_finance") in calls
        assert len(features) > 0

    def test_not_enough_candles_returns_empty(self, monkeypatch):
        monkeypatch.setattr("analysis.features.query_candles", lambda *a, **k: [])
        assert compute_features("TEST") == []


class TestMarketContext:
    def test_context_computed_from_reference_symbols(self, monkeypatch):
        from analysis.features import compute_market_context

        spy = make_candles()
        qqq = make_candles(volume=2000.0)

        def fake_query(symbol, interval, source, limit=None):
            return {"SPY": spy, "QQQ": qqq}.get(symbol, [])

        monkeypatch.setattr("analysis.features.query_candles", fake_query)
        context = compute_market_context()
        assert set(context.keys()) == {"context_spy_return_1", "context_qqq_return_1"}
        closes = [c["close"] for c in spy]
        assert context["context_spy_return_1"] == pytest.approx(
            (closes[-1] - closes[-2]) / closes[-2]
        )

    def test_missing_reference_symbol_skipped(self, monkeypatch):
        from analysis.features import compute_market_context

        monkeypatch.setattr("analysis.features.query_candles", lambda *a, **k: [])
        assert compute_market_context() == {}
