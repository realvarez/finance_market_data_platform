import pendulum
from airflow.sdk import Param, dag, task
from dag_common import resolve_symbols  # noqa: E402 (sibling module in dags folder)

from ingestion.candles import fetch_candles_range

BACKFILL_PARAMS = {
    "symbols": Param(
        default="",
        type=["null", "string"],
        title="Comma-separated; empty = Variable/config default",
    ),
    "interval": Param(default="1m", enum=["1m", "5m", "15m", "30m", "1h", "1d"]),
    "start_date": Param(default="", type="string", title="ISO 8601 UTC, e.g. 2026-08-01T00:00:00Z"),
    "end_date": Param(default="", type=["null", "string"], title="ISO 8601 UTC; empty = now"),
}


@task()
def backfill_candles(**context):
    params = context.get("params", {})
    symbols = resolve_symbols(params.get("symbols"))
    interval = params.get("interval", "1m")

    start_raw = params.get("start_date")
    if not start_raw:
        raise ValueError("start_date param is required (ISO 8601, e.g. 2026-08-01T00:00:00Z)")
    start = pendulum.parse(start_raw).in_timezone("UTC")

    end_raw = params.get("end_date")
    end = pendulum.parse(end_raw).in_timezone("UTC") if end_raw else pendulum.now("UTC")
    if end <= start:
        raise ValueError("end_date must be after start_date")

    return fetch_candles_range(symbols=symbols, interval=interval, start=start, end=end)


@dag(
    schedule=None,  # manual trigger only
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Ingestion", "Candles", "Backfill"],
    params=BACKFILL_PARAMS,
)
def market_candles_backfill_dag():
    backfill_candles()


market_candles_backfill_dag()
