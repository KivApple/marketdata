-- +goose Up
CREATE TABLE IF NOT EXISTS exchange_symbols (
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    base_asset LowCardinality(String),
    quote_asset LowCardinality(String),
    status LowCardinality(String) COMMENT 'TRADING / HALT / BREAK / DELISTED',
    tick_size Float64 DEFAULT 0,
    step_size Float64 DEFAULT 0,
    min_notional Float64 DEFAULT 0,
    updated_at DateTime('UTC') DEFAULT now()
)
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (exchange, symbol);

-- +goose Down
DROP TABLE IF EXISTS exchange_symbols;
