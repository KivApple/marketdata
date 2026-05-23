package domain

import "time"

type Interval string

type Candle struct {
	Exchange            Exchange
	Symbol              Symbol
	Interval            Interval
	OpenTime            time.Time
	Open                float64
	High                float64
	Low                 float64
	Close               float64
	BaseVolume          float64
	QuoteVolume         float64
	TakerBuyBaseVolume  float64
	TakerBuyQuoteVolume float64
	TradeCount          int64
	Closed              bool
}
