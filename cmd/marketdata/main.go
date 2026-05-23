package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"marketdata/internal/config"
	"marketdata/internal/exchange"
	"marketdata/internal/exchange/binance"
	"marketdata/internal/ingester"
	"marketdata/internal/storage"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const httpClientTimeout = 30 * time.Second

func run() error {
	slog.Info("market data")

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt, syscall.SIGTERM,
	)
	defer cancel()

	httpClient := &http.Client{
		Timeout: httpClientTimeout,
	}
	if err := storage.MigrateClickHouse(ctx, cfg.ClickHouse); err != nil {
		return fmt.Errorf("migrate ClickHouse: %w", err)
	}
	clickhouse, err := storage.OpenClickHouse(ctx, cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("open ClickHouse: %w", err)
	}
	defer func() {
		_ = clickhouse.Close()
	}()

	binanceAdapter := binance.NewAdapter(httpClient)
	candleIngester := ingester.CandleIngester{
		Adapters: []exchange.Adapter{
			binanceAdapter,
		},
		ExchangeSymbolsStorage: clickhouse,
		CandleWriter:           clickhouse,
		BackfillStatusStorage:  clickhouse,
		CandleInterval:         cfg.CandleInterval,
	}
	err = candleIngester.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("candle ingester: %w", err)
	}

	slog.Info("shutdown")
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("error", "err", err)
		os.Exit(1)
	}
}
