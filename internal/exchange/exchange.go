package exchange

import (
	"context"
	"iter"
	"marketdata/internal/domain"
	"time"
)

type CandlesRequest struct {
	Symbol   domain.Symbol
	Interval domain.Interval
	From     time.Time
	To       time.Time
}

type StreamRequest struct {
	Symbols  []domain.Symbol
	Interval domain.Interval
}

type Adapter interface {
	// Name returns exchange name. Very fast, can be called in hot path.
	Name() domain.Exchange

	// Symbols returns at least all trading symbols (some/all delisted symbols might be missing).
	Symbols(ctx context.Context) ([]domain.ExchangeSymbol, error)

	// AllSymbols returns all symbols (including delisted ones). Might be slower than Symbols.
	AllSymbols(ctx context.Context, cached []domain.ExchangeSymbol) ([]domain.ExchangeSymbol, error)

	// Candles yields all candles for given symbol, interval and time range in chronological order.
	// Might be very slow. All yielded candles guaranteed to be closed.
	// On error, yields (zero, err) once and stops.
	Candles(ctx context.Context, req CandlesRequest) iter.Seq2[domain.Candle, error]

	// StreamCandles streams new candles in realtime. Might return unclosed candles.
	// Runs until context canceled or fatal error happen. Handles non-fatal error retries
	// under the hood (e.g. temporary network failures).
	StreamCandles(ctx context.Context, req StreamRequest, out chan<- domain.Candle) error
}
