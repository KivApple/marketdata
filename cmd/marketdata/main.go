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
	"marketdata/internal/metrics"
	"marketdata/internal/storage"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

const (
	httpClientTimeout   = 30 * time.Second
	httpShutdownTimeout = 5 * time.Second
)

func run() error {
	slog.Info("market data")

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	metrics.RecordBuildInfo(metrics.ReadBuildInfo())

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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /api/metrics", promhttp.Handler())
	srv := &http.Server{Addr: cfg.ListenAddr, Handler: mux}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := candleIngester.Run(gctx)
		if err != nil {
			return fmt.Errorf("candle ingester: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		slog.Info("listening", "addr", cfg.ListenAddr)
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})
	err = g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
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
