CREATE DATABASE IF NOT EXISTS market_platform;

CREATE TABLE IF NOT EXISTS market_platform.market_ticks
(
    event_id            String,
    symbol              String,
    timestamp           DateTime64(3, 'UTC'),
    ingestion_timestamp DateTime64(3, 'UTC'),
    price               Float64,
    volume              Nullable(Float64),
    bid                 Nullable(Float64),
    ask                 Nullable(Float64),
    source              String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE TABLE IF NOT EXISTS market_platform.market_candles
(
    event_id              String,
    symbol                String,
    interval              String,
    timestamp             DateTime64(3, 'UTC'),
    open                  Float64,
    high                  Float64,
    low                   Float64,
    close                 Float64,
    volume                Float64,
    source                String,
    created_at            DateTime64(3, 'UTC'),
    reconciliation_status Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, interval, timestamp);

CREATE TABLE IF NOT EXISTS market_platform.market_features
(
    symbol      String,
    timestamp   DateTime64(3, 'UTC'),
    interval    String,
    feature_name String,
    feature_value Float64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, interval, feature_name, timestamp);

CREATE TABLE IF NOT EXISTS market_platform.market_signals
(
    event_id    String,
    symbol      String,
    timestamp   DateTime64(3, 'UTC'),
    strategy    String,
    signal      LowCardinality(String),
    confidence  Float64,
    price       Float64,
    features    String DEFAULT '{}',
    created_at  DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, strategy, timestamp);

CREATE TABLE IF NOT EXISTS market_platform.market_orders
(
    order_id    String,
    symbol      String,
    timestamp   DateTime64(3, 'UTC'),
    side        LowCardinality(String),
    quantity    Float64,
    order_type  LowCardinality(String),
    limit_price Nullable(Float64),
    status      LowCardinality(String),
    strategy    String DEFAULT ''
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE TABLE IF NOT EXISTS market_platform.market_trades
(
    trade_id    String,
    order_id    String,
    symbol      String,
    timestamp   DateTime64(3, 'UTC'),
    side        LowCardinality(String),
    quantity    Float64,
    price       Float64,
    commission  Nullable(Float64)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp);

CREATE TABLE IF NOT EXISTS market_platform.market_positions
(
    symbol               String,
    quantity             Float64,
    average_entry_price  Float64,
    current_price        Float64,
    unrealized_pnl       Float64,
    updated_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY symbol;
