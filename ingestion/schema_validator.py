import json
from pathlib import Path

import jsonschema

from ingestion.config import SCHEMAS_DIR


def _load_schema(name: str) -> dict:
    path = SCHEMAS_DIR / name
    with open(path) as f:
        return json.load(f)


_SCHEMAS = {
    "tick": _load_schema("market_tick.schema.json"),
    "candle": _load_schema("market_candle.schema.json"),
    "signal": _load_schema("market_signal.schema.json"),
    "order": _load_schema("order.schema.json"),
    "trade": _load_schema("trade.schema.json"),
    "position": _load_schema("position.schema.json"),
}


def validate(data: dict, schema_type: str) -> dict:
    schema = _SCHEMAS[schema_type]
    jsonschema.validate(instance=data, schema=schema)
    return data


def validate_tick(data: dict) -> dict:
    return validate(data, "tick")


def validate_candle(data: dict) -> dict:
    return validate(data, "candle")


def validate_signal(data: dict) -> dict:
    return validate(data, "signal")
