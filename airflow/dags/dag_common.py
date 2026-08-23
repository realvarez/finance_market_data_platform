"""Shared helpers for DAGs (kept out of business modules so they stay Airflow-free)."""

from airflow.sdk import Variable

from ingestion import config

SYMBOLS_VARIABLE = "MARKET_SYMBOLS"


def resolve_symbols(params_value: str | None = None) -> list[str]:
    """Symbol list resolution order: DAG params > Airflow Variable > env/config default."""
    raw = params_value
    if not raw:
        try:
            raw = Variable.get(SYMBOLS_VARIABLE)
        except Exception:
            raw = None
    if not raw:
        raw = ",".join(config.DEFAULT_SYMBOLS)
    return [s.strip().upper() for s in raw.split(",") if s.strip()]
