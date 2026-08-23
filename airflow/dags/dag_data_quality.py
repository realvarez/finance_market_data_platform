import pendulum
from airflow.sdk import dag, task, Param

from analysis.data_quality import run_checks
from analysis.features import generate_features
from ingestion import config


@task()
def data_quality_task(**context):
    params = context.get("params", {})
    symbols_str = params.get("symbols", ",".join(config.DEFAULT_SYMBOLS))
    symbol_list = [s.strip().upper() for s in symbols_str.split(",") if s.strip()]
    return run_checks(symbols=symbol_list)


@task()
def feature_generation_task(**context):
    params = context.get("params", {})
    symbols_str = params.get("symbols", ",".join(config.DEFAULT_SYMBOLS))
    symbol_list = [s.strip().upper() for s in symbols_str.split(",") if s.strip()]
    return generate_features(symbols=symbol_list)


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
