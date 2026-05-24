package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"marketdata/internal/domain"
	"marketdata/internal/exchange"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const exchangeInfoURL = "https://api.binance.com/api/v3/exchangeInfo"
const exchangeInfoTTL = 30 * time.Minute

const (
	apiMaxRetries      = 5
	apiMinRetryBackoff = 1 * time.Second
	apiMaxRetryBackoff = 60 * time.Second
	apiMaxRetryAfter   = 5 * time.Minute
)

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

type exchangeInfoData struct {
	fetchedAt  time.Time
	symbolsMap map[domain.Symbol]domain.ExchangeSymbol
}

type apiClient struct {
	httpClient       *http.Client
	exchangeInfoData *exchangeInfoData
	exchangeInfoMu   sync.Mutex
}

func newAPIClient(httpClient *http.Client) *apiClient {
	return &apiClient{
		httpClient: httpClient,
	}
}

func parseRetryAfter(h string) (time.Duration, bool) {
	if h == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(h); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(h); err == nil {
		if d := time.Until(t); d > 0 {
			return d, true
		}
	}
	return 0, false
}

func isRetryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || // 429
		code == http.StatusTeapot || // 418 (Binance IP ban)
		code >= 500
}

func (c *apiClient) doWithRetry(req *http.Request) (*http.Response, error) {
	backoff := apiMinRetryBackoff
	for attempt := 1; ; attempt++ {
		resp, err := c.httpClient.Do(req)
		if err == nil && !isRetryableStatus(resp.StatusCode) {
			return resp, nil
		}
		if attempt > apiMaxRetries {
			return resp, err
		}
		wait := backoff/2 + time.Duration(rand.Int63n(max(1, int64(backoff/2))))
		var status int
		if resp != nil {
			status = resp.StatusCode
			if ra, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
				wait = min(ra, apiMaxRetryAfter)
			}
			_ = resp.Body.Close()
		}
		slog.Warn(
			"Binance request failed, will retry",
			"url", req.URL.Path,
			"status", status,
			"err", err,
			"wait", wait,
			"attempt", attempt,
		)
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(wait):
		}
		backoff = min(backoff*2, apiMaxRetryBackoff)
	}
}

func parseFilterDecimal(s string) (decimal.Decimal, error) {
	if s == "" {
		return decimal.Zero, nil
	}
	return decimal.NewFromString(s)
}

func decodeExchangeInfoFilter(m *domain.ExchangeSymbol, f map[string]any) error {
	filterType, _ := f["filterType"].(string)
	switch filterType {
	case "PRICE_FILTER":
		s, _ := f["tickSize"].(string)
		v, err := parseFilterDecimal(s)
		if err != nil {
			return fmt.Errorf("%s tickSize: %w", m.Symbol, err)
		}
		m.TickSize = v
	case "LOT_SIZE":
		s, _ := f["stepSize"].(string)
		v, err := parseFilterDecimal(s)
		if err != nil {
			return fmt.Errorf("%s stepSize: %w", m.Symbol, err)
		}
		m.StepSize = v
	case "NOTIONAL", "MIN_NOTIONAL":
		s, _ := f["minNotional"].(string)
		v, err := parseFilterDecimal(s)
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

func decodeExchangeInfo(r io.Reader) (*exchangeInfoData, error) {
	var resp exchangeInfoResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	symbols := make(map[domain.Symbol]domain.ExchangeSymbol, len(resp.Symbols))
	for _, s := range resp.Symbols {
		m, err := decodeExchangeInfoSymbol(s)
		if err != nil {
			return nil, err
		}
		symbols[m.Symbol] = m
	}
	return &exchangeInfoData{
		fetchedAt:  time.Now(),
		symbolsMap: symbols,
	}, nil
}

func (d *exchangeInfoData) expired() bool {
	return d.fetchedAt.Add(exchangeInfoTTL).Before(time.Now())
}

func (d *exchangeInfoData) symbols() []domain.ExchangeSymbol {
	out := make([]domain.ExchangeSymbol, 0, len(d.symbolsMap))
	for _, s := range d.symbolsMap {
		out = append(out, s)
	}
	return out
}

func (c *apiClient) getExchangeInfo(ctx context.Context) ([]domain.ExchangeSymbol, error) {
	c.exchangeInfoMu.Lock()
	defer c.exchangeInfoMu.Unlock()
	if c.exchangeInfoData != nil && !c.exchangeInfoData.expired() {
		return c.exchangeInfoData.symbols(), nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, exchangeInfoURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	out, err := decodeExchangeInfo(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	c.exchangeInfoData = out
	return out.symbols(), nil
}

func parseKlinesRow(row []any, req exchange.CandlesRequest) (domain.Candle, time.Time, error) {
	if len(row) < 11 {
		return domain.Candle{}, time.Time{}, fmt.Errorf("expected >= 11 elements, got %d", len(row))
	}
	asFloat := func(i int) (float64, error) {
		v, ok := row[i].(float64)
		if !ok {
			return 0, fmt.Errorf("col %d: expected number, got %T", i, row[i])
		}
		return v, nil
	}
	asString := func(i int) (string, error) {
		v, ok := row[i].(string)
		if !ok {
			return "", fmt.Errorf("col %d: expected string, got %T", i, row[i])
		}
		return v, nil
	}
	parseStringDecimal := func(i int) (decimal.Decimal, error) {
		s, err := asString(i)
		if err != nil {
			return decimal.Zero, err
		}
		return decimal.NewFromString(s)
	}

	openTimeMs, err := asFloat(0)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("open_time: %w", err)
	}
	open, err := parseStringDecimal(1)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("open: %w", err)
	}
	high, err := parseStringDecimal(2)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("high: %w", err)
	}
	low, err := parseStringDecimal(3)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("low: %w", err)
	}
	closeP, err := parseStringDecimal(4)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("close: %w", err)
	}
	volume, err := parseStringDecimal(5)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("volume: %w", err)
	}
	closeTimeMs, err := asFloat(6)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("close_time: %w", err)
	}
	quoteVol, err := parseStringDecimal(7)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("quote_volume: %w", err)
	}
	countF, err := asFloat(8)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("count: %w", err)
	}
	takerBuyVol, err := parseStringDecimal(9)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("taker_buy_volume: %w", err)
	}
	takerBuyQuoteVol, err := parseStringDecimal(10)
	if err != nil {
		return domain.Candle{}, time.Time{}, fmt.Errorf("taker_buy_quote_volume: %w", err)
	}
	return domain.Candle{
		Exchange:            exchangeName,
		Symbol:              req.Symbol,
		Interval:            req.Interval,
		OpenTime:            time.UnixMilli(int64(openTimeMs)).UTC(),
		Open:                open,
		High:                high,
		Low:                 low,
		Close:               closeP,
		BaseVolume:          volume,
		QuoteVolume:         quoteVol,
		TradeCount:          int64(countF),
		TakerBuyBaseVolume:  takerBuyVol,
		TakerBuyQuoteVolume: takerBuyQuoteVol,
		Closed:              true,
	}, time.UnixMilli(int64(closeTimeMs)).UTC(), nil
}

func (c *apiClient) getCandlesPage(
	ctx context.Context,
	req exchange.CandlesRequest,
	limit int,
	maxOpenTime *time.Time,
) iter.Seq2[domain.Candle, error] {
	return func(yield func(domain.Candle, error) bool) {
		q := url.Values{}
		q.Set("symbol", string(req.Symbol))
		q.Set("interval", string(req.Interval))
		q.Set("startTime", strconv.FormatInt(max(0, req.From.UnixMilli()), 10))
		q.Set("endTime", strconv.FormatInt(max(0, req.To.UnixMilli()-1), 10))
		q.Set("limit", strconv.FormatInt(int64(limit), 10))
		u := "https://data-api.binance.vision/api/v3/klines?" + q.Encode()
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			yield(domain.Candle{}, fmt.Errorf("create klines request: %w", err))
			return
		}
		resp, err := c.doWithRetry(r)
		if err != nil {
			yield(domain.Candle{}, fmt.Errorf("klines request: %w", err))
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			yield(domain.Candle{}, fmt.Errorf("klines status %d", resp.StatusCode))
			return
		}
		var rows [][]any
		if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
			yield(domain.Candle{}, fmt.Errorf("decode klines: %w", err))
			return
		}
		now := time.Now()
		for _, row := range rows {
			candle, closeTime, err := parseKlinesRow(row, req)
			if err != nil {
				yield(domain.Candle{}, fmt.Errorf("parse klines row: %w", err))
				return
			}
			if closeTime.After(now) {
				continue
			}
			if candle.OpenTime.After(*maxOpenTime) {
				*maxOpenTime = candle.OpenTime
			}
			if !yield(candle, nil) {
				return
			}
		}
	}
}

func (c *apiClient) aliveSymbol(ctx context.Context, symbol domain.Symbol) (bool, error) {
	_, err := c.getExchangeInfo(ctx) // Ensure symbol list is fetched and isn't expired
	if err != nil {
		return false, err
	}
	c.exchangeInfoMu.Lock()
	defer c.exchangeInfoMu.Unlock()
	_, ok := c.exchangeInfoData.symbolsMap[symbol]
	return ok, nil
}

func (c *apiClient) getCandles(
	ctx context.Context,
	req exchange.CandlesRequest,
	maxOpenTime *time.Time,
) iter.Seq2[domain.Candle, error] {
	return func(yield func(domain.Candle, error) bool) {
		if !isValidInterval(req.Interval) {
			yield(domain.Candle{}, fmt.Errorf("invalid interval: %q", req.Interval))
			return
		}
		alive, err := c.aliveSymbol(ctx, req.Symbol)
		if err != nil {
			yield(domain.Candle{}, fmt.Errorf("symbol liveness check: %w", err))
			return
		}
		if !alive {
			return
		}
		const limit = 1000
		cursor := req.From
		for cursor.Before(req.To) {
			count := 0
			for candle, err := range c.getCandlesPage(
				ctx,
				exchange.CandlesRequest{
					Symbol:   req.Symbol,
					Interval: req.Interval,
					From:     cursor,
					To:       req.To,
				},
				limit,
				maxOpenTime,
			) {
				if err != nil {
					yield(domain.Candle{}, fmt.Errorf("get candles page: %w", err))
					return
				}
				count++
				if !yield(candle, nil) {
					return
				}
			}
			if count < limit {
				break
			}
			if maxOpenTime.After(cursor) {
				cursor = maxOpenTime.Add(1 * time.Millisecond)
			} else {
				break
			}
		}
	}
}
