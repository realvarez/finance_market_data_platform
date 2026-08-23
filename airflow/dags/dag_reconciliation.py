import pendulum
from airflow.sdk import dag, task, Param

from ingestion import config
from streaming.reconciliation import reconcile_candles


@task()
def run_reconciliation(**context):
    params = context.get("params", {})
    symbols_str = params.get("symbols", ",".join(config.DEFAULT_SYMBOLS))
    symbol_list = [s.strip().upper() for s in symbols_str.split(",") if s.strip()]
    return reconcile_candles(symbols=symbol_list)


@dag(
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Reconciliation"],
    params={
        "symbols": Param(default=",".join(config.DEFAULT_SYMBOLS), type="string"),
    },
)
def market_reconciliation_dag():
    run_reconciliation()


market_reconciliation_dag()
