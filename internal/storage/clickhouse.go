package storage

import (
	"context"
	"embed"
	"fmt"
	"marketdata/internal/domain"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pressly/goose/v3"
)

type ClickHouseConfig struct {
	DSN string `env:"DSN,required"`
}

type ClickHouse struct {
	conn driver.Conn
}

//go:embed migrations/*.sql
var migrationsFS embed.FS

func OpenClickHouse(ctx context.Context, cfg ClickHouseConfig) (*ClickHouse, error) {
	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open conn: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping conn: %w", err)
	}
	return &ClickHouse{conn: conn}, nil
}

func MigrateClickHouse(ctx context.Context, cfg ClickHouseConfig) error {
	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	db := clickhouse.OpenDB(opts)
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	goose.SetBaseFS(migrationsFS)
	if err := goose.SetDialect("clickhouse"); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, "migrations"); err != nil {
		return fmt.Errorf("up: %w", err)
	}
	return nil
}

func (ch *ClickHouse) Close() error {
	return ch.conn.Close()
}

func (ch *ClickHouse) SaveSymbols(ctx context.Context, symbols []domain.ExchangeSymbol) error {
	if len(symbols) == 0 {
		return nil
	}
	batch, err := ch.conn.PrepareBatch(
		ctx,
		`INSERT INTO exchange_symbols
		(exchange, symbol, base_asset, quote_asset, status, tick_size, step_size, min_notional)`,
	)
	if err != nil {
		return fmt.Errorf("prepare exchange symbols batch: %w", err)
	}
	defer func() { _ = batch.Close() }()
	for _, symbol := range symbols {
		err := batch.Append(
			string(symbol.Exchange),
			string(symbol.Symbol),
			symbol.BaseAsset,
			symbol.QuoteAsset,
			string(symbol.Status),
			symbol.TickSize,
			symbol.StepSize,
			symbol.MinNotional,
		)
		if err != nil {
			return fmt.Errorf("append exchange symbol: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send exchange symbols batch: %w", err)
	}
	return nil
}

func (ch *ClickHouse) SaveCandles(ctx context.Context, candles []domain.Candle) error {
	if len(candles) == 0 {
		return nil
	}
	batch, err := ch.conn.PrepareBatch(
		ctx,
		`INSERT INTO candles 
    	(exchange, symbol, timeframe, open_time, open, high, low, close, base_volume, quote_volume, 
    	 trade_count, taker_buy_base_volume, taker_buy_quote_volume)`,
	)
	if err != nil {
		return fmt.Errorf("prepare candles batch: %w", err)
	}
	defer func() { _ = batch.Close() }()
	for _, candle := range candles {
		err := batch.Append(
			string(candle.Exchange),
			string(candle.Symbol),
			string(candle.Interval),
			candle.OpenTime,
			candle.Open,
			candle.High,
			candle.Low,
			candle.Close,
			candle.BaseVolume,
			candle.QuoteVolume,
			candle.TradeCount,
			candle.TakerBuyBaseVolume,
			candle.TakerBuyQuoteVolume,
		)
		if err != nil {
			return fmt.Errorf("append candle: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send candles batch: %w", err)
	}
	return nil
}
