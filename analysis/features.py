import logging
from datetime import datetime, timezone

import numpy as np

from storage.clickhouse_client import query_candles
from storage.sinks import sink_features

logger = logging.getLogger(__name__)

CONTEXT_SYMBOLS = ("SPY", "QQQ")


def _load_candles(symbol: str, interval: str, limit: int) -> list[dict]:
    """Candles preferring reconciled source, falling back to raw Yahoo candles."""
    candles = query_candles(symbol, interval, "reconciled", limit=limit)
    if not candles:
        candles = query_candles(symbol, interval, "yahoo_finance", limit=limit)
    return candles


def _compute_ema(prices: list[float], period: int) -> float | None:
    if len(prices) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = (price - ema) * multiplier + ema
    return ema


def _compute_rsi(prices: list[float], period: int = 14) -> float | None:
    """RSI with Wilder smoothing (the standard definition).

    Seeds the average gain/loss with a simple mean over the first `period`
    deltas, then applies Wilder's recursive smoothing over the rest of the
    history so the indicator converges like reference implementations.
    """
    if len(prices) < period + 1:
        return None
    deltas = np.diff(np.asarray(prices, dtype=float))
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _compute_atr(candles: list[dict], period: int = 14) -> float | None:
    """Average True Range with Wilder smoothing."""
    if len(candles) < period + 1:
        return None
    trs = []
    for prev, cur in zip(candles[:-1], candles[1:]):
        tr = max(
            float(cur["high"]) - float(cur["low"]),
            abs(float(cur["high"]) - float(prev["close"])),
            abs(float(cur["low"]) - float(prev["close"])),
        )
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def _compute_relative_volume(volumes: list[float], window: int = 20) -> float | None:
    """Latest volume divided by the rolling mean of the preceding `window` candles."""
    if len(volumes) < 2:
        return None
    baseline = volumes[-(window + 1) : -1]
    avg = float(np.mean(baseline)) if baseline else 0.0
    if avg <= 0:
        return None
    return volumes[-1] / avg


def _compute_vwap(candles: list[dict]) -> float | None:
    """VWAP over the loaded candle window using typical price ((H+L+C)/3)."""
    total_volume = sum(float(c["volume"]) for c in candles)
    if total_volume <= 0:
        return None
    weighted = sum(
        (float(c["high"]) + float(c["low"]) + float(c["close"])) / 3 * float(c["volume"])
        for c in candles
    )
    return weighted / total_volume


def compute_market_context(interval: str = "1m", limit: int = 50) -> dict[str, float]:
    """Market-context returns from reference symbols (SPY/QQQ latest 1-bar return)."""
    context: dict[str, float] = {}
    for symbol in CONTEXT_SYMBOLS:
        try:
            candles = _load_candles(symbol, interval, limit)
        except Exception:
            logger.exception("Failed to load market context candles for %s", symbol)
            continue
        if len(candles) < 2:
            continue
        candles.sort(key=lambda c: c["timestamp"])
        closes = [float(c["close"]) for c in candles]
        if closes[-2] > 0:
            context[f"context_{symbol.lower()}_return_1"] = (closes[-1] - closes[-2]) / closes[-2]
    return context


def compute_features(
    symbol: str,
    interval: str = "1m",
    limit: int = 50,
    market_context: dict[str, float] | None = None,
) -> list[dict]:
    candles = _load_candles(symbol, interval, limit)
    if len(candles) < 2:
        logger.warning("Not enough candles for %s to compute features", symbol)
        return []

    candles.sort(key=lambda c: c["timestamp"])
    closes = [float(c["close"]) for c in candles]
    volumes = [float(c["volume"]) for c in candles]
    now = datetime.now(timezone.utc).isoformat()
    latest_ts = candles[-1]["timestamp"]
    features = []

    def add(name: str, value: float | None):
        if value is not None:
            features.append(
                {
                    "symbol": symbol,
                    "timestamp": str(latest_ts),
                    "interval": interval,
                    "feature_name": name,
                    "feature_value": float(value),
                    "computed_at": now,
                }
            )

    add("return_1", (closes[-1] - closes[-2]) / closes[-2] if closes[-2] else 0)
    if len(closes) >= 6:
        add("return_5", (closes[-1] - closes[-6]) / closes[-6])
    add("log_return_1", np.log(closes[-1] / closes[-2]) if closes[-2] > 0 else 0)

    latest = candles[-1]
    body = abs(latest["close"] - latest["open"])
    add("candle_body", body)
    add("upper_wick", latest["high"] - max(latest["open"], latest["close"]))
    add("lower_wick", min(latest["open"], latest["close"]) - latest["low"])

    add("ema_9", _compute_ema(closes, 9))
    add("ema_21", _compute_ema(closes, 21))
    add("rsi_14", _compute_rsi(closes, 14))
    add("atr_14", _compute_atr(candles, 14))

    if len(closes) >= 2:
        returns = np.diff(closes) / np.array(closes[:-1])
        std = np.std(returns[-10:]) if len(returns) >= 10 else np.std(returns)
        add("rolling_std_10", float(std))

    add("relative_volume", _compute_relative_volume(volumes))
    add("vwap", _compute_vwap(candles))

    context = (
        market_context if market_context is not None else compute_market_context(interval, limit)
    )
    for name, value in context.items():
        add(name, value)

    return features


def latest_feature_vector(symbol: str, interval: str = "1m") -> dict[str, float]:
    """Pivot the most recent feature rows for a symbol into {name: value}."""
    from storage.clickhouse_client import query_features

    rows = query_features(symbol, interval)
    if not rows:
        return {}
    latest_ts = max(r["timestamp"] for r in rows)
    return {
        r["feature_name"]: float(r["feature_value"]) for r in rows if r["timestamp"] == latest_ts
    }


def generate_features(symbols: list[str] | None = None, interval: str = "1m") -> int:
    from ingestion import config

    symbols = symbols or config.DEFAULT_SYMBOLS
    market_context = compute_market_context(interval)
    total = 0
    for symbol in symbols:
        features = compute_features(symbol, interval, market_context=market_context)
        if features:
            sink_features(features)
            total += len(features)
            logger.info("Generated %d features for %s", len(features), symbol)
    return total
