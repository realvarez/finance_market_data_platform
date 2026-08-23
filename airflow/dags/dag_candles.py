import datetime

import pendulum
from airflow.sdk import Param, dag, task

from ingestion import config
from ingestion.candles import fetch_latest_candles


@task()
def fetch_candles(interval: str, **context):
    params = context.get("params", {})
    symbols_str = params.get("symbols", ",".join(config.DEFAULT_SYMBOLS))
    overlap = int(params.get("overlap_minutes", 5))
    symbol_list = [s.strip().upper() for s in symbols_str.split(",") if s.strip()]
    return fetch_latest_candles(
        symbol=symbol_list[0],
        interval=interval,
        overlap_minutes=overlap,
        symbols=symbol_list,
    )


@dag(
    schedule=datetime.timedelta(minutes=1),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Ingestion", "Candles"],
    params={
        "symbols": Param(default=",".join(config.DEFAULT_SYMBOLS), type="string"),
        "overlap_minutes": Param(default=5, type="integer"),
    },
)
def market_candles_1m_dag():
    fetch_candles(interval="1m")


market_candles_1m_dag()


@dag(
    schedule=datetime.timedelta(minutes=5),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Ingestion", "Candles"],
    params={
        "symbols": Param(default=",".join(config.DEFAULT_SYMBOLS), type="string"),
        "overlap_minutes": Param(default=5, type="integer"),
    },
)
def market_candles_5m_dag():
    fetch_candles(interval="5m")


market_candles_5m_dag()
