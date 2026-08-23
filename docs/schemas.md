# Data Schemas

All schemas are JSON Schema Draft 2020-12 files in `schemas/v1/`.

## MarketTick

Real-time market price update from WebSocket.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `event_id` | string | yes | Unique ID: `{symbol}{YYYYMMDDHHMMSS}` |
| `symbol` | string | yes | Ticker symbol (e.g. AAPL) |
| `timestamp` | string (date-time) | yes | When the market event occurred |
| `ingestion_timestamp` | string (date-time) | yes | When the platform received the event |
| `price` | number | yes | Last traded price |
| `volume` | number \| null | no | Trade volume if available |
| `bid` | number \| null | no | Best bid price |
| `ask` | number \| null | no | Best ask price |
| `source` | string | yes | Data source identifier |

**File:** `schemas/v1/market_tick.schema.json`

## MarketCandle

OHLCV candle for a given symbol and interval.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `event_id` | string | yes | Unique ID: `{symbol}{YYYYMMDDHHMMSS}` |
| `symbol` | string | yes | Ticker symbol |
| `interval` | string | yes | Candle interval: `1m`, `5m`, `15m`, `1h`, `1d` |
| `timestamp` | string (date-time) | yes | Candle open time |
| `open` | number | yes | Opening price |
| `high` | number | yes | Highest price |
| `low` | number | yes | Lowest price |
| `close` | number | yes | Closing price |
| `volume` | number | yes | Total volume |
| `source` | string | yes | `yahoo_finance`, `spark_streaming`, or `reconciled` |
| `created_at` | string (date-time) | yes | When the record was created |

**File:** `schemas/v1/market_candle.schema.json`

## MarketSignal

Trading signal from a strategy.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `event_id` | string | yes | Unique signal ID |
| `symbol` | string | yes | Ticker symbol |
| `timestamp` | string (date-time) | yes | Signal generation time |
| `strategy` | string | yes | Strategy name (e.g. `ema_crossover`) |
| `signal` | string | yes | `BUY`, `SELL`, or `HOLD` |
| `confidence` | number | yes | 0.0 to 1.0 |
| `price` | number | yes | Price at signal time |
| `features` | object | no | Relevant feature values |
| `expected_holding_period` | string | no | ISO 8601 duration |

**File:** `schemas/v1/market_signal.schema.json`

## Order

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `order_id` | string | yes | Unique order ID |
| `symbol` | string | yes | Ticker symbol |
| `timestamp` | string (date-time) | yes | Order submission time |
| `side` | string | yes | `BUY` or `SELL` |
| `quantity` | number | yes | Number of shares |
| `order_type` | string | yes | `MARKET`, `LIMIT`, `STOP` |
| `limit_price` | number | no | Limit price if applicable |
| `status` | string | yes | `PENDING`, `FILLED`, `CANCELLED`, `REJECTED` |
| `strategy` | string | no | Originating strategy |

**File:** `schemas/v1/order.schema.json`

## Trade

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `trade_id` | string | yes | Unique trade ID |
| `order_id` | string | yes | Parent order ID |
| `symbol` | string | yes | Ticker symbol |
| `timestamp` | string (date-time) | yes | Execution time |
| `side` | string | yes | `BUY` or `SELL` |
| `quantity` | number | yes | Shares executed |
| `price` | number | yes | Execution price |
| `commission` | number | no | Commission paid |

**File:** `schemas/v1/trade.schema.json`

## Position

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `symbol` | string | yes | Ticker symbol |
| `quantity` | number | yes | Current shares held |
| `average_entry_price` | number | yes | Weighted average entry |
| `current_price` | number | yes | Latest market price |
| `unrealized_pnl` | number | yes | Unrealized profit/loss |
| `updated_at` | string (date-time) | yes | Last update time |

**File:** `schemas/v1/position.schema.json`

## Validation

Python modules validate against schemas before publishing:

```python
from ingestion.schema_validator import validate_tick, validate_candle

tick = validate_tick(raw_message)  # raises ValidationError on failure
```

## Versioning

Schemas live in `schemas/v1/`. Breaking changes require a new version directory (`v2/`). Producers should include schema version in message metadata when using Schema Registry.

## Sample Payloads

Example messages (not schemas) are in `schemas/samples/`:

- `schemas/samples/market_tick.json`
- `schemas/samples/market_candle.json`
