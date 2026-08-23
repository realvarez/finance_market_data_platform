"""Contract tests: every schemas/v1 payload validates, malformed ones are rejected."""

import jsonschema
import pytest

from ingestion.schema_validator import (
    validate_candle,
    validate_order,
    validate_position,
    validate_signal,
    validate_tick,
    validate_trade,
)


def make_valid_payloads() -> dict[str, dict]:
    return {
        "tick": {
            "event_id": "MU20260625004813",
            "symbol": "MU",
            "timestamp": "2026-06-25T00:48:13Z",
            "ingestion_timestamp": "2026-06-25T00:48:13.100Z",
            "price": 1227.01,
            "volume": None,
            "bid": None,
            "ask": None,
            "source": "yahoo_finance",
        },
        "candle": {
            "event_id": "NVDA20260625132800",
            "symbol": "NVDA",
            "interval": "1m",
            "timestamp": "2026-06-25T13:28:00Z",
            "open": 195.57,
            "high": 195.58,
            "low": 195.34,
            "close": 195.37,
            "volume": 162257,
            "source": "yahoo_finance",
            "created_at": "2026-06-25T13:28:10Z",
        },
        "signal": {
            "event_id": "sig-001",
            "symbol": "NVDA",
            "timestamp": "2026-08-22T15:30:00Z",
            "strategy": "ema_crossover",
            "signal": "BUY",
            "confidence": 0.8,
            "price": 195.5,
        },
        "order": {
            "order_id": "ord-001",
            "symbol": "NVDA",
            "timestamp": "2026-08-22T15:30:00Z",
            "side": "BUY",
            "quantity": 10,
            "order_type": "MARKET",
            "status": "PENDING",
            "strategy": "ema_crossover",
        },
        "trade": {
            "trade_id": "trd-001",
            "order_id": "ord-001",
            "symbol": "NVDA",
            "timestamp": "2026-08-22T15:30:00Z",
            "side": "BUY",
            "quantity": 10,
            "price": 195.5,
            "commission": 0.0,
        },
        "position": {
            "symbol": "NVDA",
            "quantity": 10,
            "average_entry_price": 195.5,
            "current_price": 197.0,
            "unrealized_pnl": 15.0,
            "updated_at": "2026-08-22T15:35:00Z",
        },
    }


@pytest.mark.parametrize(
    "validator_name",
    ["tick", "candle", "signal", "order", "trade", "position"],
)
def test_valid_payloads_pass(validator_name):
    validators = {
        "tick": validate_tick,
        "candle": validate_candle,
        "signal": validate_signal,
        "order": validate_order,
        "trade": validate_trade,
        "position": validate_position,
    }
    payload = make_valid_payloads()[validator_name]
    assert validators[validator_name](payload) == payload


@pytest.mark.parametrize(
    ("validator", "mutation"),
    [
        # Missing required fields
        (validate_tick, lambda p: p.pop("price")),
        (validate_candle, lambda p: p.pop("close")),
        (validate_signal, lambda p: p.pop("strategy")),
        (validate_order, lambda p: p.pop("quantity")),
        (validate_trade, lambda p: p.pop("order_id")),
        (validate_position, lambda p: p.pop("average_entry_price")),
        # Enum violations
        (validate_signal, lambda p: p.update(signal="MAYBE")),
        (validate_candle, lambda p: p.update(interval="2m")),
        (validate_order, lambda p: p.update(status="MAYBE_LATER")),
        (validate_trade, lambda p: p.update(side="HOLD")),
        # Range violations
        (validate_signal, lambda p: p.update(confidence=1.5)),
        (validate_signal, lambda p: p.update(confidence=-0.1)),
        (validate_order, lambda p: p.update(quantity=-10)),
        (validate_tick, lambda p: p.update(price=-1)),
        # Domain violations
        (validate_candle, lambda p: p.update(source="binance")),
        (validate_signal, lambda p: p.update(symbol="not valid!")),
        # additionalProperties: false
        (validate_tick, lambda p: p.update(exchange="NASDAQ")),
        (validate_signal, lambda p: p.update(action="buy")),
    ],
)
def test_invalid_payloads_rejected(validator, mutation):
    payload_key = {
        id(validate_tick): "tick",
        id(validate_candle): "candle",
        id(validate_signal): "signal",
        id(validate_order): "order",
        id(validate_trade): "trade",
        id(validate_position): "position",
    }[id(validator)]
    payload = mutation(dict(make_valid_payloads()[payload_key]))
    with pytest.raises(jsonschema.ValidationError):
        validator(payload)
