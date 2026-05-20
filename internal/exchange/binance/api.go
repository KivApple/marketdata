package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"marketdata/internal/domain"
	"net/http"
	"strconv"
)

const exchangeInfoURL = "https://api.binance.com/api/v3/exchangeInfo"

type exchangeInfoSymbol struct {
	Symbol     string           `json:"symbol"`
	Status     string           `json:"status"`
	BaseAsset  string           `json:"baseAsset"`
	QuoteAsset string           `json:"quoteAsset"`
	Filters    []map[string]any `json:"filters"`
}

type exchangeInfoResponse struct {
	Symbols []exchangeInfoSymbol `json:"symbols"`
}

type apiClient struct {
	httpClient *http.Client
}

func newAPIClient(httpClient *http.Client) *apiClient {
	return &apiClient{
		httpClient: httpClient,
	}
}

func parseFilterFloat(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 64)
}

func decodeExchangeInfoFilter(m *domain.ExchangeSymbol, f map[string]any) error {
	filterType, _ := f["filterType"].(string)
	switch filterType {
	case "PRICE_FILTER":
		s, _ := f["tickSize"].(string)
		v, err := parseFilterFloat(s)
		if err != nil {
			return fmt.Errorf("%s tickSize: %w", m.Symbol, err)
		}
		m.TickSize = v
	case "LOT_SIZE":
		s, _ := f["stepSize"].(string)
		v, err := parseFilterFloat(s)
		if err != nil {
			return fmt.Errorf("%s stepSize: %w", m.Symbol, err)
		}
		m.StepSize = v
	case "NOTIONAL", "MIN_NOTIONAL":
		s, _ := f["minNotional"].(string)
		v, err := parseFilterFloat(s)
		if err != nil {
			return fmt.Errorf("%s minNotional: %w", m.Symbol, err)
		}
		m.MinNotional = v
	}
	return nil
}

func decodeExchangeInfoSymbol(s exchangeInfoSymbol) (domain.ExchangeSymbol, error) {
	m := domain.ExchangeSymbol{
		Exchange:   exchangeName,
		Symbol:     domain.Symbol(s.Symbol),
		Status:     domain.SymbolStatus(s.Status),
		BaseAsset:  s.BaseAsset,
		QuoteAsset: s.QuoteAsset,
	}
	for _, f := range s.Filters {
		if err := decodeExchangeInfoFilter(&m, f); err != nil {
			return domain.ExchangeSymbol{}, err
		}
	}
	return m, nil
}

func decodeExchangeInfo(r io.Reader) ([]domain.ExchangeSymbol, error) {
	var resp exchangeInfoResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	out := make([]domain.ExchangeSymbol, 0, len(resp.Symbols))
	for _, s := range resp.Symbols {
		m, err := decodeExchangeInfoSymbol(s)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func (c *apiClient) getExchangeInfo(ctx context.Context) ([]domain.ExchangeSymbol, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, exchangeInfoURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return decodeExchangeInfo(resp.Body)
}
