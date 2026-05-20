package exchange

import (
	"context"
	"marketdata/internal/domain"
)

type StreamRequest struct {
	Symbols  []domain.Symbol
	Interval domain.Interval
}

type Adapter interface {
	Name() domain.Exchange

	Symbols(ctx context.Context) ([]domain.ExchangeSymbol, error)

	AllSymbols(ctx context.Context, cached []domain.ExchangeSymbol) ([]domain.ExchangeSymbol, error)

	StreamCandles(ctx context.Context, req StreamRequest, out chan<- domain.Candle) error
}
