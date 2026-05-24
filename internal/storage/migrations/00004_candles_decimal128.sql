-- +goose Up
ALTER TABLE candles
    MODIFY COLUMN open                   Decimal128(8) CODEC(ZSTD(3)),
    MODIFY COLUMN high                   Decimal128(8) CODEC(ZSTD(3)),
    MODIFY COLUMN low                    Decimal128(8) CODEC(ZSTD(3)),
    MODIFY COLUMN close                  Decimal128(8) CODEC(ZSTD(3)),
    MODIFY COLUMN base_volume            Decimal128(8) CODEC(ZSTD(3)),
    MODIFY COLUMN quote_volume           Decimal128(8) CODEC(ZSTD(3)),
    MODIFY COLUMN taker_buy_base_volume  Decimal128(8) CODEC(ZSTD(3)),
    MODIFY COLUMN taker_buy_quote_volume Decimal128(8) CODEC(ZSTD(3));

-- +goose Down
ALTER TABLE candles
    MODIFY COLUMN open                   Float64 CODEC(Gorilla, ZSTD(3)),
    MODIFY COLUMN high                   Float64 CODEC(Gorilla, ZSTD(3)),
    MODIFY COLUMN low                    Float64 CODEC(Gorilla, ZSTD(3)),
    MODIFY COLUMN close                  Float64 CODEC(Gorilla, ZSTD(3)),
    MODIFY COLUMN base_volume            Float64 CODEC(Gorilla, ZSTD(3)),
    MODIFY COLUMN quote_volume           Float64 CODEC(Gorilla, ZSTD(3)),
    MODIFY COLUMN taker_buy_base_volume  Float64 CODEC(Gorilla, ZSTD(3)),
    MODIFY COLUMN taker_buy_quote_volume Float64 CODEC(Gorilla, ZSTD(3));
