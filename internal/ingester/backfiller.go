package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"marketdata/internal/domain"
	"marketdata/internal/exchange"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	parallelBackfillLimit = 10
	backfillChunkSize     = 8192
)

type adapterBackfiller struct {
	backfillStatusStorage BackfillStatusStorage
	candleWriter          CandleWriter
	candleInterval        domain.Interval
	adapter               exchange.Adapter
	ongoing               symbolSet
	ongoingMu             sync.Mutex
}

func (b *adapterBackfiller) startBackfill(symbols []domain.ExchangeSymbol) {
	b.ongoingMu.Lock()
	defer b.ongoingMu.Unlock()
	if b.ongoing == nil {
		b.ongoing = make(symbolSet, len(symbols))
	}
	for _, symbol := range symbols {
		b.ongoing[symbol.Symbol] = struct{}{}
	}
}

func (b *adapterBackfiller) ongoingSymbols() []domain.Symbol {
	b.ongoingMu.Lock()
	defer b.ongoingMu.Unlock()
	out := make([]domain.Symbol, 0, len(b.ongoing))
	for symbol := range b.ongoing {
		out = append(out, symbol)
	}
	return out
}

func (b *adapterBackfiller) runSymbolOnce(ctx context.Context, symbol domain.Symbol) error {
	backfilledBy, err := b.backfillStatusStorage.GetBackfilledBy(ctx, b.adapter.Name(), symbol, b.candleInterval)
	if err != nil {
		return fmt.Errorf("backfilled by retrieval for exchange %q symbol %q: %w", b.adapter.Name(), symbol, err)
	}
	req := exchange.CandlesRequest{
		Symbol:   symbol,
		Interval: b.candleInterval,
		From:     backfilledBy.Add(1 * time.Millisecond),
		To:       time.Now(),
	}
	batch := make([]domain.Candle, 0, backfillChunkSize)
	var lastSaved time.Time
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := b.candleWriter.SaveCandles(ctx, batch); err != nil {
			return fmt.Errorf(
				"save candles for exchange %q symbol %q timeframe %q: %w",
				b.adapter.Name(),
				symbol,
				b.candleInterval,
				err,
			)
		}
		last := batch[len(batch)-1].OpenTime
		if err := b.backfillStatusStorage.SetBackfilledBy(ctx, b.adapter.Name(), symbol, b.candleInterval, last); err != nil {
			return fmt.Errorf(
				"set backfilled by for exchange %q symbol %q timeframe %q: %w",
				b.adapter.Name(),
				symbol,
				b.candleInterval,
				err,
			)
		}
		lastSaved = last
		batch = batch[:0]
		return nil
	}
	for candle, err := range b.adapter.Candles(ctx, req) {
		if err != nil {
			return fmt.Errorf(
				"get candles for exchange %q symbol %q timeframe %q: %w",
				b.adapter.Name(),
				symbol,
				b.candleInterval,
				err,
			)
		}
		batch = append(batch, candle)
		if len(batch) >= backfillChunkSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if !lastSaved.IsZero() {
		slog.Info(
			"backfilled symbol",
			"exchange", b.adapter.Name(),
			"symbol", symbol,
			"timeframe", b.candleInterval,
			"to", lastSaved,
		)
	}
	return nil
}

func (b *adapterBackfiller) runOnce(ctx context.Context) {
	var g errgroup.Group
	g.SetLimit(parallelBackfillLimit)
	for _, symbol := range b.ongoingSymbols() {
		g.Go(func() error {
			if err := b.runSymbolOnce(ctx, symbol); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error(
					"backfill symbol",
					"exchange", b.adapter.Name(),
					"symbol", symbol,
					"err", err,
				)
			}
			return nil
		})
	}
	_ = g.Wait()
}

func (b *adapterBackfiller) run(ctx context.Context) error {
	for {
		b.runOnce(ctx)
		select {
		case <-time.After(10 * time.Minute):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
