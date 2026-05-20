package binance

import (
	"context"
	"marketdata/internal/domain"
	"marketdata/internal/exchange"
	"net/http"
)

const exchangeName = "binance"

type Adapter struct {
	apiClient *apiClient
}

var _ exchange.Adapter = (*Adapter)(nil)

func NewAdapter(httpClient *http.Client) *Adapter {
	return &Adapter{
		apiClient: newAPIClient(httpClient),
	}
}

func (a *Adapter) Name() domain.Exchange {
	return exchangeName
}

func (a *Adapter) Symbols(ctx context.Context) ([]domain.ExchangeSymbol, error) {
	return a.apiClient.getExchangeInfo(ctx)
}

func (a *Adapter) StreamCandles(ctx context.Context, req exchange.StreamRequest, out chan<- domain.Candle) error {
	return streamSymbolsSharded(ctx, req, out)
}

func isValidInterval(interval domain.Interval) bool {
	switch interval {
	case "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M":
		return true
	default:
		return false
	}
}
