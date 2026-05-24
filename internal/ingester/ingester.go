package ingester

import (
	"context"
	"fmt"
	"log/slog"
	"marketdata/internal/domain"
	"marketdata/internal/exchange"
	"marketdata/internal/metrics"
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

type BackfillStatusStorage interface {
	GetBackfilledBy(
		ctx context.Context,
		exchange domain.Exchange,
		symbol domain.Symbol,
		interval domain.Interval,
	) (time.Time, error)

	SetBackfilledBy(
		ctx context.Context,
		exchange domain.Exchange,
		symbol domain.Symbol,
		interval domain.Interval,
		backfilledBy time.Time,
	) error
}

type CandleIngester struct {
	Adapters               []exchange.Adapter
	ExchangeSymbolsStorage ExchangeSymbolsStorage
	CandleWriter           CandleWriter
	BackfillStatusStorage  BackfillStatusStorage
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
	metrics.ExchangeSymbols.WithLabelValues(string(adapter.Name())).Set(float64(len(symbols)))
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
			metrics.CandlesIngestedTotal.WithLabelValues(string(candle.Exchange)).Inc()
			if candle.Closed {
				metrics.ClosedCandlesIngestedTotal.WithLabelValues(string(candle.Exchange)).Inc()
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
	slog.Debug("saving candles", "count", len(candles))
	if err := i.CandleWriter.SaveCandles(ctx, candles); err != nil {
		return true, fmt.Errorf("save candles: %w", err)
	}
	return true, nil
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
		supervisor := &adapterSupervisor{
			ingester: i,
			out:      candles,
			adapter:  adapter,
		}
		if i.BackfillStatusStorage != nil {
			backfiller := &adapterBackfiller{
				backfillStatusStorage: i.BackfillStatusStorage,
				candleWriter:          i.CandleWriter,
				candleInterval:        i.CandleInterval,
				adapter:               adapter,
			}
			supervisor.backfiller = backfiller
		}
		g.Go(func() error {
			return supervisor.run(gctx)
		})
		if supervisor.backfiller != nil {
			g.Go(func() error {
				return supervisor.backfiller.run(gctx)
			})
		}
	}
	g.Go(func() error {
		return i.bufferCandles(gctx, candles, bufferedCandles)
	})
	g.Go(func() error {
		return i.consumeCandles(bufferedCandles)
	})
	return g.Wait()
}
