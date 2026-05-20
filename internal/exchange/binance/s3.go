package binance

import (
	"context"
	"encoding/xml"
	"fmt"
	"iter"
	"marketdata/internal/domain"
	"net/http"
	"net/url"
	"strings"
)

const visionBaseUrl = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/"
const visionKlinesPrefix = "data/spot/daily/klines/"
const testSymbol = "这是测试币456"

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
	httpClient *http.Client
}

func newS3Client(httpClient *http.Client) *s3Client {
	return &s3Client{
		httpClient: httpClient,
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
