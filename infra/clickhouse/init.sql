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
