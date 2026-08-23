import asyncio
import functools
import logging
import signal

from ingestion import config
from ingestion.kafka_utils import create_async_producer, send_async, send_error_async
from ingestion.schema_validator import validate_tick
from ingestion.yahoo_client import normalize_tick, run_websocket

logger = logging.getLogger(__name__)


async def handle_tick(producer, message: dict) -> None:
    try:
        tick = normalize_tick(message)
        if tick["price"] is None:
            return
        validated = validate_tick(tick)
        await send_async(producer, config.TOPIC_TICKS, validated["symbol"], validated)
    except Exception as e:
        logger.exception("Error processing tick")
        await send_error_async(producer, message, str(e))


async def run_tick_ingestion(symbols: list[str] | None = None) -> None:
    symbols = symbols or config.DEFAULT_SYMBOLS
    producer = await create_async_producer()
    logger.info("Starting tick ingestion for %s", symbols)

    shutdown = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        shutdown.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    while not shutdown.is_set():
        try:
            await run_websocket(
                symbols,
                functools.partial(handle_tick, producer),
            )
        except Exception:
            if shutdown.is_set():
                break
            logger.exception("WebSocket connection lost, reconnecting in 5s...")
            await asyncio.sleep(5)

    await producer.stop()
    logger.info("Tick ingestion stopped")
