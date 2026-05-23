-- +goose Up
CREATE TABLE IF NOT EXISTS backfill_status (
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    timeframe LowCardinality(String) COMMENT '1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M',
    backfilled_by DateTime('UTC'),
    updated_at DateTime('UTC') DEFAULT now()
)
    ENGINE ReplacingMergeTree(updated_at)
    ORDER BY (exchange, symbol, timeframe);

-- +goose Down
DROP TABLE IF EXISTS backfill_status;
