import asyncio
import logging
import sys

from ingestion import config
from ingestion.ticks import run_tick_ingestion

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)

if __name__ == "__main__":
    asyncio.run(run_tick_ingestion(config.DEFAULT_SYMBOLS))
