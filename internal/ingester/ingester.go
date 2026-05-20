package ingester

import (
	"context"
	"fmt"
	"log/slog"
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

func (ingester *CandleIngester) runAdapter(
	ctx context.Context,
	adapter exchange.Adapter,
	out chan<- domain.Candle,
) error {
	symbols, err := adapter.Symbols(ctx)
	if err != nil {
		return fmt.Errorf("get symbols for %q: %w", adapter.Name(), err)
	}
	slog.Info("fetched symbols", "exchange", adapter.Name(), "count", len(symbols))
	err = ingester.ExchangeSymbolsStorage.SaveSymbols(ctx, symbols)
	if err != nil {
		return fmt.Errorf("save exchange symbols for %q: %w", adapter.Name(), err)
	}
	activeSymbols := make([]domain.Symbol, 0, len(symbols))
	for _, symbol := range symbols {
		if symbol.Status == domain.SymbolStatusTrading {
			activeSymbols = append(activeSymbols, symbol.Symbol)
		}
	}
	req := exchange.StreamRequest{
		Symbols:  activeSymbols,
		Interval: ingester.CandleInterval,
	}
	slog.Info("streaming candles", "exchange", adapter.Name())
	err = adapter.StreamCandles(ctx, req, out)
	if err != nil {
		return fmt.Errorf("stream candles for %q: %w", adapter.Name(), err)
	}
	return nil
}

func (ingester *CandleIngester) bufferCandles(
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

func (ingester *CandleIngester) consumeCandlesOnce(in <-chan []domain.Candle) (bool, error) {
	candles, ok := <-in
	if !ok {
		slog.Info("candles channel closed")
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	slog.Info("saving candles", "count", len(candles))
	if err := ingester.CandleWriter.SaveCandles(ctx, candles); err != nil {
		return true, fmt.Errorf("save candles: %w", err)
	}
	return true, ctx.Err()
}

func (ingester *CandleIngester) consumeCandles(in <-chan []domain.Candle) error {
	for {
		if cont, err := ingester.consumeCandlesOnce(in); !cont || err != nil {
			return err
		}
	}
}

func (ingester *CandleIngester) runAdapterSymbolsRefreshOnce(
	ctx context.Context,
	adapter exchange.Adapter,
) error {
	cached, err := ingester.ExchangeSymbolsStorage.ExchangeSymbols(ctx, adapter.Name())
	if err != nil {
		return fmt.Errorf("load symbols for %q: %w", adapter.Name(), err)
	}
	symbols, err := adapter.AllSymbols(ctx, cached)
	if err != nil {
		return fmt.Errorf("get all symbols for %q: %w", adapter.Name(), err)
	}
	slog.Info("fetched all symbols", "exchange", adapter.Name(), "count", len(symbols))
	err = ingester.ExchangeSymbolsStorage.SaveSymbols(ctx, symbols)
	if err != nil {
		return fmt.Errorf("save symbols for %q: %w", adapter.Name(), err)
	}
	return nil
}

func (ingester *CandleIngester) runAdapterSymbolsRefresh(
	ctx context.Context,
	adapter exchange.Adapter,
) error {
	ticker := time.NewTicker(symbolsRefreshInterval)
	defer ticker.Stop()
	for {
		err := ingester.runAdapterSymbolsRefreshOnce(ctx, adapter)
		if err != nil {
			slog.Error("refresh adapter symbols", "err", err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (ingester *CandleIngester) Run(ctx context.Context) error {
	bufferedCandles := make(chan []domain.Candle, candleBufferCount)
	candles := make(chan domain.Candle, candleChanBufferSize)
	defer close(candles)
	g, gctx := errgroup.WithContext(ctx)
	for _, adapter := range ingester.Adapters {
		g.Go(func() error {
			return ingester.runAdapter(gctx, adapter, candles)
		})
		g.Go(func() error {
			return ingester.runAdapterSymbolsRefresh(gctx, adapter)
		})
	}
	g.Go(func() error {
		return ingester.bufferCandles(gctx, candles, bufferedCandles)
	})
	g.Go(func() error {
		return ingester.consumeCandles(bufferedCandles)
	})
	return g.Wait()
}
