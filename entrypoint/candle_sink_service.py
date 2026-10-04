import logging
import sys

from storage.candle_sink import run_candle_sink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)

if __name__ == "__main__":
    run_candle_sink()
