import pendulum
from airflow.sdk import Param, dag, task
from dag_common import resolve_symbols  # noqa: E402

from ingestion import config
from streaming.reconciliation import reconcile_candles


@task()
def run_reconciliation(**context):
    symbols = resolve_symbols(context.get("params", {}).get("symbols"))
    return reconcile_candles(symbols=symbols)


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
