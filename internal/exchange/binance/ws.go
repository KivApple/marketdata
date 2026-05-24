package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"marketdata/internal/domain"
	"marketdata/internal/exchange"
	"math/rand"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"
)

const (
	wsBaseURL         = "wss://stream.binance.com:9443/stream"
	minWsBackoff      = 1 * time.Second
	maxWsBackoff      = 30 * time.Second
	wsShardSize       = 512
	wsMaxConnLifetime = 23 * time.Hour
	wsReadTimeout     = 1 * time.Minute
)

type combinedStreamMsg struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type klinePayload struct {
	EventType string    `json:"e"`
	EventTime int64     `json:"E"`
	K         klineBody `json:"k"`
}

type klineBody struct {
	OpenTime    int64  `json:"t"`
	CloseTime   int64  `json:"T"`
	Symbol      string `json:"s"`
	Interval    string `json:"i"`
	Open        string `json:"o"`
	High        string `json:"h"`
	Low         string `json:"l"`
	Close       string `json:"c"`
	BaseVolume  string `json:"v"`
	QuoteVolume string `json:"q"`
	TakerBase   string `json:"V"`
	TakerQuote  string `json:"Q"`
	TradeCount  int64  `json:"n"`
	LastTradeID int64  `json:"L"`
	IsClosed    bool   `json:"x"`
}

func buildStreamURL(req exchange.StreamRequest) string {
	parts := make([]string, len(req.Symbols))
	for i, s := range req.Symbols {
		parts[i] = strings.ToLower(string(s)) + "@kline_" + string(req.Interval)
	}
	v := url.Values{}
	v.Set("streams", strings.Join(parts, "/"))
	return wsBaseURL + "?" + v.Encode()
}

func (k klineBody) toDomain() (domain.Candle, error) {
	candle := domain.Candle{
		Exchange:   exchangeName,
		Symbol:     domain.Symbol(k.Symbol),
		Interval:   domain.Interval(k.Interval),
		OpenTime:   time.UnixMilli(k.OpenTime).UTC(),
		TradeCount: k.TradeCount,
		Closed:     k.IsClosed,
	}
	fields := []struct {
		name string
		raw  string
		dst  *decimal.Decimal
	}{
		{"open", k.Open, &candle.Open},
		{"high", k.High, &candle.High},
		{"low", k.Low, &candle.Low},
		{"close", k.Close, &candle.Close},
		{"base volume", k.BaseVolume, &candle.BaseVolume},
		{"quote volume", k.QuoteVolume, &candle.QuoteVolume},
		{"taker base", k.TakerBase, &candle.TakerBuyBaseVolume},
		{"taker quote", k.TakerQuote, &candle.TakerBuyQuoteVolume},
	}
	for _, f := range fields {
		v, err := decimal.NewFromString(f.raw)
		if err != nil {
			return domain.Candle{}, fmt.Errorf("%s: %w", f.name, err)
		}
		*f.dst = v
	}
	return candle, nil
}

func parseKlineMsg(data []byte) (domain.Candle, error) {
	var p klinePayload
	if err := json.Unmarshal(data, &p); err != nil {
		return domain.Candle{}, fmt.Errorf("unmarshal: %w", err)
	}
	if p.EventType != "kline" {
		return domain.Candle{}, fmt.Errorf("unexpected event type: %q", p.EventType)
	}
	c, err := p.K.toDomain()
	if err != nil {
		return domain.Candle{}, fmt.Errorf("convert: %w", err)
	}
	return c, nil
}

func streamSymbols(ctx context.Context, req exchange.StreamRequest, out chan<- domain.Candle) error {
	ctx, cancel := context.WithTimeout(ctx, wsMaxConnLifetime)
	defer cancel()
	u := buildStreamURL(req)
	conn, _, err := websocket.Dial(ctx, u, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.CloseNow() }()
	slog.Info("ws connected")
	for {
		readCtx, cancel := context.WithTimeout(ctx, wsReadTimeout)
		var msg combinedStreamMsg
		err := wsjson.Read(readCtx, conn, &msg)
		cancel()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if !strings.Contains(msg.Stream, "@kline_") {
			slog.Warn("non-kline message, skipping", "stream", msg.Stream)
			continue
		}
		candle, err := parseKlineMsg(msg.Data)
		if err != nil {
			slog.Warn("kline parse failed, skipping message", "err", err)
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- candle:
		}
	}
}

func streamSymbolsRetryable(ctx context.Context, req exchange.StreamRequest, out chan<- domain.Candle) error {
	backoff := minWsBackoff
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start := time.Now()
		err := streamSymbols(ctx, req, out)
		if time.Since(start) > maxWsBackoff {
			backoff = minWsBackoff
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("ws connection closed, will reconnect", "err", err, "backoff", backoff)
		}
		backoffWithJitter := backoff/2 + time.Duration(rand.Int63n(max(1, int64(backoff/2))))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoffWithJitter):
		}
		backoff = min(backoff*2, maxWsBackoff)
	}
}

func shardSymbolsForStreaming(symbols []domain.Symbol) [][]domain.Symbol {
	shardCount := (len(symbols) + wsShardSize - 1) / wsShardSize
	shards := make([][]domain.Symbol, 0, shardCount)
	for shard := range slices.Chunk(symbols, wsShardSize) {
		shards = append(shards, shard)
	}
	return shards
}

func streamSymbolsSharded(ctx context.Context, req exchange.StreamRequest, out chan<- domain.Candle) error {
	if len(req.Symbols) == 0 {
		return errors.New("empty symbols")
	}
	if !isValidInterval(req.Interval) {
		return fmt.Errorf("invalid interval: %q", req.Interval)
	}
	shards := shardSymbolsForStreaming(req.Symbols)
	slog.Info(
		"starting ws stream",
		"interval", req.Interval,
		"symbols", len(req.Symbols),
		"shards", len(shards),
	)
	g, gctx := errgroup.WithContext(ctx)
	for _, shard := range shards {
		g.Go(func() error {
			return streamSymbolsRetryable(
				gctx,
				exchange.StreamRequest{
					Symbols:  shard,
					Interval: req.Interval,
				},
				out,
			)
		})
	}
	return g.Wait()
}
