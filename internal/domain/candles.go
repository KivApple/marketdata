package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

type Interval string

type Candle struct {
	Exchange            Exchange
	Symbol              Symbol
	Interval            Interval
	OpenTime            time.Time
	Open                decimal.Decimal
	High                decimal.Decimal
	Low                 decimal.Decimal
	Close               decimal.Decimal
	BaseVolume          decimal.Decimal
	QuoteVolume         decimal.Decimal
	TakerBuyBaseVolume  decimal.Decimal
	TakerBuyQuoteVolume decimal.Decimal
	TradeCount          uint64
	Closed              bool
}
