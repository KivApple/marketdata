-- +goose Up
ALTER TABLE exchange_symbols
    MODIFY COLUMN tick_size    Decimal128(8) DEFAULT 0,
    MODIFY COLUMN step_size    Decimal128(8) DEFAULT 0,
    MODIFY COLUMN min_notional Decimal128(8) DEFAULT 0;

-- +goose Down
ALTER TABLE exchange_symbols
    MODIFY COLUMN tick_size    Float64 DEFAULT 0,
    MODIFY COLUMN step_size    Float64 DEFAULT 0,
    MODIFY COLUMN min_notional Float64 DEFAULT 0;
