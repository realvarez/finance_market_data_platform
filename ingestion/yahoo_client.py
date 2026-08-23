import logging
from datetime import datetime, timezone
from typing import Callable

import yfinance as yf

from ingestion import config

logger = logging.getLogger(__name__)


def normalize_tick(message: dict) -> dict:
    symbol = message.get("id") or message.get("symbol", "")
    timestamp_posix = message.get("time", "0")
    timestamp = datetime.fromtimestamp(float(timestamp_posix) / 1000, tz=timezone.utc)
    now = datetime.now(timezone.utc)

    return {
        "event_id": f"{symbol}{timestamp.strftime('%Y%m%d%H%M%S')}",
        "symbol": symbol,
        "timestamp": timestamp.isoformat(),
        "ingestion_timestamp": now.isoformat(),
        "price": message.get("price"),
        "volume": message.get("volume"),
        "bid": message.get("bid"),
        "ask": message.get("ask"),
        "source": config.SOURCE_YAHOO,
    }


async def run_websocket(symbols: list[str], handler: Callable) -> None:
    async with yf.AsyncWebSocket() as ws:
        await ws.subscribe(symbols)
        logger.info("Subscribed to symbols: %s", symbols)
        await ws.listen(handler)
