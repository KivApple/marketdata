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
	candleChanBufferSize = 4096
	candleBufferSize     = 8192
	candleBufferCount    = 16
	flushInterval        = 1 * time.Second
	flushTimeout         = 10 * time.Second
)

type ExchangeSymbolWriter interface {
	SaveSymbols(ctx context.Context, symbols []domain.ExchangeSymbol) error
}

type CandleWriter interface {
	SaveCandles(ctx context.Context, candles []domain.Candle) error
}

type CandleIngester struct {
	Adapters             []exchange.Adapter
	ExchangeSymbolWriter ExchangeSymbolWriter
	CandleWriter         CandleWriter
	CandleInterval       domain.Interval
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
	err = ingester.ExchangeSymbolWriter.SaveSymbols(ctx, symbols)
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

func (ingester *CandleIngester) Run(ctx context.Context) error {
	bufferedCandles := make(chan []domain.Candle, candleBufferCount)
	candles := make(chan domain.Candle, candleChanBufferSize)
	defer close(candles)
	g, gctx := errgroup.WithContext(ctx)
	for _, adapter := range ingester.Adapters {
		g.Go(func() error {
			return ingester.runAdapter(gctx, adapter, candles)
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
