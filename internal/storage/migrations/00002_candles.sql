-- +goose Up
CREATE TABLE IF NOT EXISTS candles (
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    timeframe LowCardinality(String) COMMENT '1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M',
    open_time DateTime('UTC') CODEC (DoubleDelta, ZSTD(3)),
    open Float64 CODEC (Gorilla, ZSTD(3)),
    high Float64 CODEC (Gorilla, ZSTD(3)),
    low Float64 CODEC (Gorilla, ZSTD(3)),
    close Float64 CODEC (Gorilla, ZSTD(3)),
    base_volume Float64 CODEC (Gorilla, ZSTD(3)),
    quote_volume Float64 CODEC (Gorilla, ZSTD(3)),
    trade_count UInt64 CODEC (T64, ZSTD(3)),
    taker_buy_base_volume Float64 CODEC (Gorilla, ZSTD(3)),
    taker_buy_quote_volume Float64 CODEC (Gorilla, ZSTD(3))
)
    ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(open_time)
    ORDER BY (exchange, symbol, timeframe, open_time)
    SETTINGS index_granularity = 8192;

-- +goose Down
DROP TABLE IF EXISTS candles;
