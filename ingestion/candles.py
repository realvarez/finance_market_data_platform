import logging
from datetime import datetime, timedelta, timezone

import yfinance as yf

from ingestion import config
from ingestion.kafka_utils import create_sync_producer, send_sync
from ingestion.schema_validator import validate_candle

logger = logging.getLogger(__name__)

INTERVAL_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "1d": 1440}

# Yahoo caps how much history a single intraday request may span.
RANGE_CHUNK_LIMITS = {
    "1m": timedelta(days=7),
    "5m": timedelta(days=30),
    "15m": timedelta(days=30),
    "30m": timedelta(days=30),
    "1h": timedelta(days=300),
    "1d": timedelta(days=3650),
}


def fetch_latest_candles(
    symbol: str,
    interval: str = "1m",
    overlap_minutes: int = 5,
    symbols: list[str] | None = None,
) -> int:
    """Fetch latest completed candles with overlap and publish to Kafka."""
    target_symbols = symbols or [symbol]
    producer = create_sync_producer()
    total = 0

    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=overlap_minutes + INTERVAL_MINUTES.get(interval, 1))
    end = now

    for sym in target_symbols:
        count = _fetch_symbol_candles(producer, sym, interval, start, end)
        total += count
        logger.info("Published %d candles for %s (%s)", count, sym, interval)

    producer.flush()
    producer.close()
    return total


def fetch_candles_range(
    symbols: list[str],
    interval: str = "1m",
    start: datetime | None = None,
    end: datetime | None = None,
) -> int:
    """Fetch historical candles over a date range, chunked to respect Yahoo limits.

    Used by the backfill DAG to seed the data lake for backtesting.
    """
    if end is None:
        end = datetime.now(timezone.utc)
    if start is None:
        raise ValueError("start is required for a range fetch")

    chunk_limit = RANGE_CHUNK_LIMITS.get(interval, timedelta(days=7))
    producer = create_sync_producer()
    total = 0

    current = start
    while current < end:
        window_end = min(current + chunk_limit, end)
        for sym in symbols:
            count = _fetch_symbol_candles(producer, sym, interval, current, window_end)
            total += count
            logger.info(
                "Backfilled %d candles for %s (%s) %s..%s",
                count,
                sym,
                interval,
                current.isoformat(),
                window_end.isoformat(),
            )
        current = window_end

    producer.flush()
    producer.close()
    return total


def _fetch_symbol_candles(
    producer,
    symbol: str,
    interval: str,
    start: datetime,
    end: datetime,
) -> int:

    logger.info(
        "Fetching candles for %s, start=%s, end=%s, interval=%s", symbol, start, end, interval
    )

    ticker = yf.Ticker(symbol)

    history = ticker.history(
        start=start,
        end=end,
        interval=interval,
    )

    if history.empty:
        logger.warning("No candle data for %s (%s)", symbol, interval)
        return 0

    count = 0
    created_at = datetime.now(timezone.utc).isoformat()

    for row in history.itertuples():
        ts = row.Index
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)

        candle = {
            "event_id": f"{symbol}{ts.strftime('%Y%m%d%H%M%S')}",
            "symbol": symbol,
            "interval": interval,
            "timestamp": ts.isoformat(),
            "open": float(row.Open),
            "high": float(row.High),
            "low": float(row.Low),
            "close": float(row.Close),
            "volume": float(row.Volume),
            "source": config.SOURCE_YAHOO,
            "created_at": created_at,
        }

        try:
            validated = validate_candle(candle)
            send_sync(producer, config.TOPIC_CANDLES_RAW, symbol, validated)
            count += 1
        except Exception:
            logger.exception("Failed to validate/send candle for %s", symbol)

    return count
