package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"marketdata/internal/domain"
	"marketdata/internal/exchange"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	candleChanBufferSize   = 4096
	candleBufferSize       = 8192
	candleBufferCount      = 16
	flushInterval          = 1 * time.Second
	flushTimeout           = 10 * time.Second
	symbolsRefreshInterval = 1 * time.Hour
)

type ExchangeSymbolsStorage interface {
	SaveSymbols(ctx context.Context, symbols []domain.ExchangeSymbol) error

	ExchangeSymbols(ctx context.Context, exchange domain.Exchange) ([]domain.ExchangeSymbol, error)
}

type CandleWriter interface {
	SaveCandles(ctx context.Context, candles []domain.Candle) error
}

type CandleIngester struct {
	Adapters               []exchange.Adapter
	ExchangeSymbolsStorage ExchangeSymbolsStorage
	CandleWriter           CandleWriter
	CandleInterval         domain.Interval
}

func (i *CandleIngester) streamCandlesFromAdapter(
	ctx context.Context,
	adapter exchange.Adapter,
	active symbolSet,
	out chan<- domain.Candle,
) error {
	symbols := make([]domain.Symbol, 0, len(active))
	for s := range active {
		symbols = append(symbols, s)
	}
	req := exchange.StreamRequest{
		Symbols:  symbols,
		Interval: i.CandleInterval,
	}
	slog.Info("streaming candles", "exchange", adapter.Name(), "symbols", len(symbols))
	if err := adapter.StreamCandles(ctx, req, out); err != nil {
		return fmt.Errorf("stream candles for %q: %w", adapter.Name(), err)
	}
	return nil
}

func (i *CandleIngester) fetchAdapterSymbols(
	ctx context.Context,
	adapter exchange.Adapter,
) ([]domain.ExchangeSymbol, error) {
	symbols, err := adapter.Symbols(ctx)
	if err == nil {
		slog.Info("fetched symbols", "exchange", adapter.Name(), "count", len(symbols))
		if err := i.ExchangeSymbolsStorage.SaveSymbols(ctx, symbols); err != nil {
			slog.Error("save symbols", "exchange", adapter.Name(), "err", err)
		}
		return symbols, nil
	}
	slog.Error("fetch symbols", "exchange", adapter.Name(), "err", err)
	cached, cerr := i.ExchangeSymbolsStorage.ExchangeSymbols(ctx, adapter.Name())
	if cerr != nil || len(cached) == 0 {
		return nil, fmt.Errorf("get symbols for %q: %w", adapter.Name(), err)
	}
	slog.Warn("using cached symbols", "exchange", adapter.Name())
	return cached, nil
}

func (i *CandleIngester) refreshAdapterSymbols(
	ctx context.Context,
	adapter exchange.Adapter,
) ([]domain.ExchangeSymbol, error) {
	cached, err := i.ExchangeSymbolsStorage.ExchangeSymbols(ctx, adapter.Name())
	if err != nil {
		slog.Error("load cached symbols", "exchange", adapter.Name(), "err", err)
	}
	symbols, err := adapter.AllSymbols(ctx, cached)
	if err != nil {
		if len(cached) == 0 {
			return nil, fmt.Errorf("get all symbols for %q: %w", adapter.Name(), err)
		}
		slog.Warn("using cached symbols", "exchange", adapter.Name(), "err", err)
		return cached, nil
	}
	slog.Info("fetched all symbols", "exchange", adapter.Name(), "count", len(symbols))
	if err := i.ExchangeSymbolsStorage.SaveSymbols(ctx, symbols); err != nil {
		slog.Error("save symbols", "exchange", adapter.Name(), "err", err)
	}
	return symbols, nil
}

func (i *CandleIngester) bufferCandles(
	ctx context.Context,
	in <-chan domain.Candle,
	out chan<- []domain.Candle,
) error {
	buffer := make([]domain.Candle, 0, candleBufferSize)
	flushTimer := time.NewTimer(flushInterval)
	defer flushTimer.Stop()
	defer close(out)
	for {
		select {
		case candle := <-in:
			if candle.Closed {
				buffer = append(buffer, candle)
				if len(buffer) >= candleBufferSize {
					select {
					case out <- buffer:
						buffer = make([]domain.Candle, 0, candleBufferSize)
					case <-ctx.Done():
						return ctx.Err()
					}
				} else {
					flushTimer.Reset(flushInterval)
				}
			}
		case <-flushTimer.C:
			if len(buffer) > 0 {
				select {
				case out <- buffer:
					buffer = make([]domain.Candle, 0, candleBufferSize)
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		case <-ctx.Done():
			if len(buffer) > 0 {
				select {
				case out <- buffer:
				default:
				}
			}
			return ctx.Err()
		}
	}
}

func (i *CandleIngester) consumeCandlesOnce(in <-chan []domain.Candle) (bool, error) {
	candles, ok := <-in
	if !ok {
		slog.Info("candles channel closed")
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	slog.Info("saving candles", "count", len(candles))
	if err := i.CandleWriter.SaveCandles(ctx, candles); err != nil {
		return true, fmt.Errorf("save candles: %w", err)
	}
	return true, ctx.Err()
}

func (i *CandleIngester) consumeCandles(in <-chan []domain.Candle) error {
	for {
		if cont, err := i.consumeCandlesOnce(in); !cont || err != nil {
			return err
		}
	}
}

func (i *CandleIngester) Run(ctx context.Context) error {
	candles := make(chan domain.Candle, candleChanBufferSize)
	bufferedCandles := make(chan []domain.Candle, candleBufferCount)
	defer close(candles)
	g, gctx := errgroup.WithContext(ctx)
	for _, adapter := range i.Adapters {
		sup := &adapterSupervisor{ingester: i, adapter: adapter, out: candles}
		g.Go(func() error {
			return sup.run(gctx)
		})
	}
	g.Go(func() error {
		return i.bufferCandles(gctx, candles, bufferedCandles)
	})
	g.Go(func() error {
		return i.consumeCandles(bufferedCandles)
	})
	return g.Wait()
}

type adapterSupervisor struct {
	ingester *CandleIngester
	adapter  exchange.Adapter
	out      chan<- domain.Candle

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
}

func (s *adapterSupervisor) run(ctx context.Context) error {
	defer s.stop()

	if symbols, err := s.ingester.fetchAdapterSymbols(ctx, s.adapter); err != nil {
		slog.Error("initial symbols", "exchange", s.adapter.Name(), "err", err)
	} else {
		s.start(ctx, activeSymbolSet(symbols))
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
