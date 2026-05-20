package binance

import (
	"context"
	"fmt"
	"log/slog"
	"marketdata/internal/domain"
	"marketdata/internal/exchange"
	"net/http"
)

const exchangeName = "binance"
const maxAssetNameLen = 10

type Adapter struct {
	apiClient *apiClient
	s3Client  *s3Client
}

var _ exchange.Adapter = (*Adapter)(nil)

func NewAdapter(httpClient *http.Client) *Adapter {
	return &Adapter{
		apiClient: newAPIClient(httpClient),
		s3Client:  newS3Client(httpClient),
	}
}

func (a *Adapter) Name() domain.Exchange {
	return exchangeName
}

func (a *Adapter) Symbols(ctx context.Context) ([]domain.ExchangeSymbol, error) {
	return a.apiClient.getExchangeInfo(ctx)
}

func splitSymbol(symbol domain.Symbol, knownAssets map[string]struct{}) (base, quote string) {
	runes := []rune(string(symbol))
	maxLen := len(runes) - 1
	if maxLen > maxAssetNameLen {
		maxLen = maxAssetNameLen
	}
	for length := maxLen; length >= 2; length-- {
		candidate := string(runes[len(runes)-length:])
		if _, ok := knownAssets[candidate]; ok {
			return string(runes[:len(runes)-length]), candidate
		}
	}
	for length := maxLen; length >= 2; length-- {
		candidate := string(runes[:length])
		if _, ok := knownAssets[candidate]; ok {
			return candidate, string(runes[length:])
		}
	}
	return "", ""
}

func collectAssetNames(
	symbols []domain.ExchangeSymbol,
	assets map[string]struct{},
	symbolMap map[domain.Symbol]domain.ExchangeSymbol,
) {
	for _, symbol := range symbols {
		assets[symbol.BaseAsset] = struct{}{}
		assets[symbol.QuoteAsset] = struct{}{}
		_, ok := symbolMap[symbol.Symbol]
		if !ok {
			symbolMap[symbol.Symbol] = symbol
		}
	}
}

func (a *Adapter) AllSymbols(ctx context.Context, cached []domain.ExchangeSymbol) ([]domain.ExchangeSymbol, error) {
	symbolMap := make(map[domain.Symbol]domain.ExchangeSymbol)
	knownAssets := make(map[string]struct{})
	symbols, err := a.Symbols(ctx)
	if err != nil {
		return nil, fmt.Errorf("get exchange symbols: %w", err)
	}
	collectAssetNames(symbols, knownAssets, symbolMap)
	collectAssetNames(cached, knownAssets, symbolMap)
	s3Symbols, err := a.s3Client.listSymbols(ctx)
	if err != nil {
		return nil, fmt.Errorf("list s3 symbols: %w", err)
	}
	for _, symbol := range s3Symbols {
		_, ok := symbolMap[symbol]
		if ok {
			continue
		}
		base, quote := splitSymbol(symbol, knownAssets)
		if base == "" || quote == "" {
			slog.Warn("unable to parse exchange symbol", "symbol", symbol)
			continue
		}
		symbolMap[symbol] = domain.ExchangeSymbol{
			Exchange:   exchangeName,
			Symbol:     symbol,
			BaseAsset:  base,
			QuoteAsset: quote,
			Status:     domain.SymbolStatusDelisted,
		}
	}
	out := make([]domain.ExchangeSymbol, 0, len(symbolMap))
	for _, symbol := range symbolMap {
		out = append(out, symbol)
	}
	return out, nil
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
