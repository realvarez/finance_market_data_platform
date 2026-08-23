import logging

import clickhouse_connect

from ingestion import config

logger = logging.getLogger(__name__)


def get_client():
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        database=config.CLICKHOUSE_DATABASE,
    )


def insert_ticks(records: list[dict]) -> None:
    if not records:
        return
    client = get_client()
    columns = [
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
    rows = [[r.get(c) for c in columns] for r in records]
    client.insert("market_ticks", rows, column_names=columns)
    logger.info("Inserted %d ticks into ClickHouse", len(rows))


def insert_candles(records: list[dict]) -> None:
    if not records:
        return
    client = get_client()
    columns = [
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
    rows = [[r.get(c) for c in columns] for r in records]
    client.insert("market_candles", rows, column_names=columns)
    logger.info("Inserted %d candles into ClickHouse", len(rows))


def insert_features(records: list[dict]) -> None:
    if not records:
        return
    client = get_client()
    columns = ["symbol", "timestamp", "interval", "feature_name", "feature_value", "computed_at"]
    rows = [[r.get(c) for c in columns] for r in records]
    client.insert("market_features", rows, column_names=columns)
    logger.info("Inserted %d features into ClickHouse", len(rows))


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
