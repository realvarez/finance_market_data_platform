import datetime

import pendulum
from airflow.sdk import Param, dag, task
from dag_common import resolve_symbols  # noqa: E402 (sibling module in dags folder)

from ingestion import config
from ingestion.candles import fetch_latest_candles


@task()
def fetch_candles(interval: str, **context):
    params = context.get("params", {})
    symbols = resolve_symbols(params.get("symbols"))
    overlap = int(params.get("overlap_minutes", 5))
    return fetch_latest_candles(
        symbol=symbols[0],
        interval=interval,
        overlap_minutes=overlap,
        symbols=symbols,
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
