import pendulum
from airflow.sdk import Param, dag, task
from dag_common import resolve_symbols  # noqa: E402 (sibling module in dags folder)

from analysis.data_quality import run_checks
from analysis.features import generate_features
from ingestion import config


@task()
def data_quality_task(**context):
    symbols = resolve_symbols(context.get("params", {}).get("symbols"))
    return run_checks(symbols=symbols)


@task()
def feature_generation_task(**context):
    symbols = resolve_symbols(context.get("params", {}).get("symbols"))
    return generate_features(symbols=symbols)


@dag(
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Analysis", "DataQuality"],
    params={
        "symbols": Param(default=",".join(config.DEFAULT_SYMBOLS), type="string"),
    },
)
def market_data_quality_dag():
    quality = data_quality_task()
    features = feature_generation_task()
    quality >> features


market_data_quality_dag()
