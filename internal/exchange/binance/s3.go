package binance

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"iter"
	"marketdata/internal/domain"
	"marketdata/internal/exchange"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const visionBaseUrl = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/"
const visionKlinesPrefix = "data/spot/daily/klines/"
const testSymbol = "这是测试币456"
const granularityMonthly = "monthly"
const granularityDaily = "daily"
const archiveDownloadTimeout = 5 * time.Minute

type visionCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type visionContent struct {
	Key string `xml:"Key"`
}

type visionListResult struct {
	XMLName        xml.Name             `xml:"ListBucketResult"`
	NextMarker     string               `xml:"NextMarker"`
	IsTruncated    bool                 `xml:"IsTruncated"`
	CommonPrefixes []visionCommonPrefix `xml:"CommonPrefixes"`
	Contents       []visionContent      `xml:"Contents"`
}

type s3Client struct {
	httpClient        *http.Client
	archiveHttpClient *http.Client
}

func newS3Client(httpClient *http.Client) *s3Client {
	return &s3Client{
		httpClient: httpClient,
		archiveHttpClient: &http.Client{
			Timeout: archiveDownloadTimeout,
		},
	}
}

func (c *s3Client) listObjects(
	ctx context.Context,
	prefix,
	delimiter,
	marker string,
) (*visionListResult, error) {
	q := url.Values{}
	q.Set("prefix", prefix)
	if delimiter != "" {
		q.Set("delimiter", delimiter)
	}
	if marker != "" {
		q.Set("marker", marker)
	}
	u := visionBaseUrl + "?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("create s3 request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("s3 status %d", resp.StatusCode)
	}
	var page visionListResult
	if err := xml.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, fmt.Errorf("decode s3: %w", err)
	}
	return &page, nil
}

func (c *s3Client) listAllObjects(
	ctx context.Context,
	prefix,
	delimiter string,
) iter.Seq2[*visionListResult, error] {
	return func(yield func(*visionListResult, error) bool) {
		var marker string
		for {
			page, err := c.listObjects(ctx, prefix, delimiter, marker)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(page, nil) {
				return
			}
			if !page.IsTruncated {
				return
			}
			marker = page.NextMarker
			if marker == "" {
				if len(page.CommonPrefixes) > 0 {
					marker = page.CommonPrefixes[len(page.CommonPrefixes)-1].Prefix
				} else if len(page.Contents) > 0 {
					marker = page.Contents[len(page.Contents)-1].Key
				}
			}
			if marker == "" {
				return
			}
		}
	}
}

func (c *s3Client) listSymbols(ctx context.Context) ([]domain.Symbol, error) {
	out := make([]domain.Symbol, 0)
	for page, err := range c.listAllObjects(ctx, visionKlinesPrefix, "/") {
		if err != nil {
			return nil, err
		}
		for _, p := range page.CommonPrefixes {
			name := strings.TrimSuffix(strings.TrimPrefix(p.Prefix, visionKlinesPrefix), "/")
			if name == "" || name == testSymbol {
				continue
			}
			out = append(out, domain.Symbol(name))
		}
	}
	return out, nil
}

func dateFromKey(key, granularity string) (time.Time, error) {
	base := strings.TrimSuffix(key, ".zip")
	var layout string
	var n int
	switch granularity {
	case granularityMonthly:
		layout, n = "2006-01", 7
	case granularityDaily:
		layout, n = "2006-01-02", 10
	default:
		return time.Time{}, fmt.Errorf("unknown granularity: %s", granularity)
	}
	if len(base) < n {
		return time.Time{}, fmt.Errorf("key too short: %s", key)
	}
	return time.Parse(layout, base[len(base)-n:])
}

func (c *s3Client) getArchive(ctx context.Context, key string) ([]byte, error) {
	u := visionBaseUrl + key
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("create vision request: %w", err)
	}
	resp, err := c.archiveHttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vision request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vision archive status %d for %s", resp.StatusCode, key)
	}
	return io.ReadAll(resp.Body)
}

func parseUnixMillisOrMicros(s string) (time.Time, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	if n > 1e14 {
		return time.UnixMicro(n).UTC(), nil
	}
	return time.UnixMilli(n).UTC(), nil
}

func parseArchiveRow(rec []string, req exchange.CandlesRequest) (domain.Candle, error) {
	if len(rec) < 11 {
		return domain.Candle{}, fmt.Errorf("expected >= 11 cols, got %d", len(rec))
	}
	openTime, err := parseUnixMillisOrMicros(rec[0])
	if err != nil {
		return domain.Candle{}, fmt.Errorf("open_time: %w", err)
	}
	open, err := strconv.ParseFloat(rec[1], 64)
	if err != nil {
		return domain.Candle{}, fmt.Errorf("open: %w", err)
	}
	high, err := strconv.ParseFloat(rec[2], 64)
	if err != nil {
		return domain.Candle{}, fmt.Errorf("high: %w", err)
	}
	low, err := strconv.ParseFloat(rec[3], 64)
	if err != nil {
		return domain.Candle{}, fmt.Errorf("low: %w", err)
	}
	closeP, err := strconv.ParseFloat(rec[4], 64)
	if err != nil {
		return domain.Candle{}, fmt.Errorf("close: %w", err)
	}
	volume, err := strconv.ParseFloat(rec[5], 64)
	if err != nil {
		return domain.Candle{}, fmt.Errorf("volume: %w", err)
	}
	// rec[6] is close_time - skip; we derive from interval
	quoteVol, err := strconv.ParseFloat(rec[7], 64)
	if err != nil {
		return domain.Candle{}, fmt.Errorf("quote_volume: %w", err)
	}
	count, err := strconv.ParseUint(rec[8], 10, 64)
	if err != nil {
		return domain.Candle{}, fmt.Errorf("count: %w", err)
	}
	takerBuyVol, err := strconv.ParseFloat(rec[9], 64)
	if err != nil {
		return domain.Candle{}, fmt.Errorf("taker_buy_volume: %w", err)
	}
	takerBuyQuoteVol, err := strconv.ParseFloat(rec[10], 64)
	if err != nil {
		return domain.Candle{}, fmt.Errorf("taker_buy_quote_volume: %w", err)
	}
	return domain.Candle{
		Exchange:            exchangeName,
		Symbol:              req.Symbol,
		Interval:            req.Interval,
		OpenTime:            openTime,
		Open:                open,
		High:                high,
		Low:                 low,
		Close:               closeP,
		BaseVolume:          volume,
		QuoteVolume:         quoteVol,
		TradeCount:          int64(count),
		TakerBuyBaseVolume:  takerBuyVol,
		TakerBuyQuoteVolume: takerBuyQuoteVol,
		Closed:              true,
	}, nil
}

func parseArchive(
	ctx context.Context,
	zipBytes []byte,
	req exchange.CandlesRequest,
	maxOpenTime *time.Time,
) iter.Seq2[domain.Candle, error] {
	return func(yield func(domain.Candle, error) bool) {
		zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
		if err != nil {
			yield(domain.Candle{}, fmt.Errorf("open zip: %w", err))
			return
		}
		if len(zr.File) == 0 {
			yield(domain.Candle{}, fmt.Errorf("zip contains no files"))
			return
		}
		f, err := zr.File[0].Open()
		if err != nil {
			yield(domain.Candle{}, fmt.Errorf("open inner csv: %w", err))
			return
		}
		defer func() { _ = f.Close() }()
		csvr := csv.NewReader(f)
		csvr.FieldsPerRecord = -1
		first := true
		for {
			if err := ctx.Err(); err != nil {
				yield(domain.Candle{}, err)
				return
			}
			rec, err := csvr.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				yield(domain.Candle{}, fmt.Errorf("csv read: %w", err))
				return
			}
			candle, err := parseArchiveRow(rec, req)
			if err != nil {
				if first {
					// Optional CSV header, ignore it
					first = false
					continue
				} else {
					yield(domain.Candle{}, fmt.Errorf("parse row: %w", err))
					return
				}
			}
			first = false
			if candle.OpenTime.Before(req.From) || !candle.OpenTime.Before(req.To) {
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

func (c *s3Client) getCandles(
	ctx context.Context,
	req exchange.CandlesRequest,
	granularity string,
	maxOpenTime *time.Time,
) iter.Seq2[domain.Candle, error] {
	return func(yield func(domain.Candle, error) bool) {
		if !isValidInterval(req.Interval) {
			yield(domain.Candle{}, fmt.Errorf("invalid interval: %q", req.Interval))
			return
		}
		prefix := fmt.Sprintf("data/spot/%s/klines/%s/%s/", granularity, req.Symbol, req.Interval)
		for page, err := range c.listAllObjects(ctx, prefix, "") {
			if err != nil {
				yield(domain.Candle{}, fmt.Errorf("list objects: %w", err))
				return
			}
			for _, item := range page.Contents {
				if !strings.HasSuffix(item.Key, ".zip") {
					continue
				}
				period, err := dateFromKey(item.Key, granularity)
				if err != nil {
					continue
				}
				var periodEnd time.Time
				switch granularity {
				case granularityMonthly:
					periodEnd = period.AddDate(0, 1, 0)
				case granularityDaily:
					periodEnd = period.AddDate(0, 0, 1)
				}
				lastCandleOpen := periodEnd.Add(-intervalDuration(req.Interval))
				if lastCandleOpen.Before(req.From) || !period.Before(req.To) {
					continue
				}
				zipBytes, err := c.getArchive(ctx, item.Key)
				if err != nil {
					yield(domain.Candle{}, fmt.Errorf("get archive: %w", err))
					return
				}
				for candle, err := range parseArchive(ctx, zipBytes, req, maxOpenTime) {
					if err != nil {
						yield(domain.Candle{}, fmt.Errorf("parse archive: %w", err))
						return
					}
					if !yield(candle, nil) {
						return
					}
				}
			}
		}
	}
}

// intervalDuration converts interval name into duration,
// supports only intervals up to "1d" as week and month boundaries are hard
// to compute
func intervalDuration(interval domain.Interval) time.Duration {
	switch interval {
	case "1s":
		return time.Second
	case "1m":
		return time.Minute
	case "3m":
		return 3 * time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return 1 * time.Hour
	case "2h":
		return 2 * time.Hour
	case "4h":
		return 4 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "8h":
		return 8 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "1d":
		return 24 * time.Hour
	default:
		return 0
	}
}
