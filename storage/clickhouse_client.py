import logging

import clickhouse_connect

from ingestion import config

logger = logging.getLogger(__name__)


def get_client():
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        database=config.CLICKHOUSE_DATABASE,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
    )


TICK_COLUMNS = [
    "event_id",
    "symbol",
    "timestamp",
    "ingestion_timestamp",
    "price",
    "volume",
    "bid",
    "ask",
    "source",
]

CANDLE_COLUMNS = [
    "event_id",
    "symbol",
    "interval",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "created_at",
    "reconciliation_status",
]

FEATURE_COLUMNS = [
    "symbol",
    "timestamp",
    "interval",
    "feature_name",
    "feature_value",
    "computed_at",
]

SIGNAL_COLUMNS = [
    "event_id",
    "symbol",
    "timestamp",
    "strategy",
    "signal",
    "confidence",
    "price",
    "created_at",
]


def _insert_records(table: str, records: list[dict], columns: list[str]) -> None:
    if not records:
        return
    client = get_client()
    rows = [[r.get(c) for c in columns] for r in records]
    client.insert(table, rows, column_names=columns)
    logger.info("Inserted %d rows into %s", len(rows), table)


def insert_ticks(records: list[dict]) -> None:
    _insert_records("market_ticks", records, TICK_COLUMNS)


def insert_candles(records: list[dict]) -> None:
    _insert_records("market_candles", records, CANDLE_COLUMNS)


def insert_features(records: list[dict]) -> None:
    _insert_records("market_features", records, FEATURE_COLUMNS)


def insert_signals(records: list[dict]) -> None:
    _insert_records("market_signals", records, SIGNAL_COLUMNS)


def query_candles(symbol: str, interval: str, source: str, limit: int = 100) -> list[dict]:
    client = get_client()
    result = client.query(
        """
        SELECT event_id, symbol, interval, timestamp, open, high, low, close, volume, source
        FROM market_candles
        WHERE symbol = {symbol:String} AND interval = {interval:String} AND source = {source:String}
        ORDER BY timestamp DESC
        LIMIT {limit:UInt32}
        """,
        parameters={"symbol": symbol, "interval": interval, "source": source, "limit": limit},
    )
    columns = result.column_names
    return [dict(zip(columns, row)) for row in result.result_rows]


def query_features(symbol: str, interval: str, limit: int = 100) -> list[dict]:
    """Latest feature rows for a symbol, newest first."""
    client = get_client()
    result = client.query(
        """
        SELECT symbol, timestamp, interval, feature_name, feature_value, computed_at
        FROM market_features
        WHERE symbol = {symbol:String} AND interval = {interval:String}
        ORDER BY timestamp DESC, computed_at DESC
        LIMIT {limit:UInt32}
        """,
        parameters={"symbol": symbol, "interval": interval, "limit": limit},
    )
    columns = result.column_names
    return [dict(zip(columns, row)) for row in result.result_rows]
