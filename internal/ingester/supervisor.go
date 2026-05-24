package ingester

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"marketdata/internal/domain"
	"marketdata/internal/exchange"
	"marketdata/internal/metrics"
	"time"
)

type adapterSupervisor struct {
	ingester   *CandleIngester
	out        chan<- domain.Candle
	adapter    exchange.Adapter
	backfiller *adapterBackfiller

	cancel context.CancelFunc
	done   chan struct{}
	active symbolSet
}

func (s *adapterSupervisor) stop() {
	if s.cancel == nil {
		return
	}
	s.cancel()
	<-s.done
	s.cancel, s.done = nil, nil
}

func (s *adapterSupervisor) start(ctx context.Context, next symbolSet) {
	s.stop()
	s.active = next
	metrics.ExchangeTradingSymbols.WithLabelValues(string(s.adapter.Name())).Set(float64(len(next)))
	if len(next) == 0 {
		slog.Warn("no active symbols, skipping stream", "exchange", s.adapter.Name())
		return
	}
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.cancel, s.done = cancel, done
	go func() {
		defer close(done)
		if err := s.ingester.streamCandlesFromAdapter(sctx, s.adapter, next, s.out); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("stream candles", "exchange", s.adapter.Name(), "err", err)
		}
	}()
}

func (s *adapterSupervisor) refresh(ctx context.Context) {
	symbols, err := s.ingester.refreshAdapterSymbols(ctx, s.adapter)
	if err != nil {
		slog.Error("refresh symbols", "exchange", s.adapter.Name(), "err", err)
		return
	}
	next := activeSymbolSet(symbols)
	if s.cancel == nil || !maps.Equal(s.active, next) {
		s.start(ctx, next)
	}
	if s.backfiller != nil {
		s.backfiller.startBackfill(symbols)
	}
}

func (s *adapterSupervisor) run(ctx context.Context) error {
	defer s.stop()

	if symbols, err := s.ingester.fetchAdapterSymbols(ctx, s.adapter); err != nil {
		slog.Error("initial symbols", "exchange", s.adapter.Name(), "err", err)
	} else {
		s.start(ctx, activeSymbolSet(symbols))
		if s.backfiller != nil {
			s.backfiller.startBackfill(symbols)
		}
	}

	ticker := time.NewTicker(symbolsRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			s.cancel()
			s.cancel, s.done = nil, nil
		case <-ticker.C:
			s.refresh(ctx)
		}
	}
}

type symbolSet = map[domain.Symbol]struct{}

func activeSymbolSet(symbols []domain.ExchangeSymbol) symbolSet {
	set := make(symbolSet, len(symbols))
	for _, s := range symbols {
		if s.Status == domain.SymbolStatusTrading {
			set[s.Symbol] = struct{}{}
		}
	}
	return set
}
