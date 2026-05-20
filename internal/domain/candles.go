package domain

import "time"

type (
	Exchange     string
	Symbol       string
	Interval     string
	SymbolStatus string
)

const SymbolStatusTrading = SymbolStatus("TRADING")
const SymbolStatusDelisted = SymbolStatus("DELISTED")

type ExchangeSymbol struct {
	Exchange    Exchange
	Symbol      Symbol
	BaseAsset   string
	QuoteAsset  string
	Status      SymbolStatus
	TickSize    float64
	StepSize    float64
	MinNotional float64
}

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
