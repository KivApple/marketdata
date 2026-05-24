package domain

import "github.com/shopspring/decimal"

type (
	Exchange     string
	Symbol       string
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
	TickSize    decimal.Decimal
	StepSize    decimal.Decimal
	MinNotional decimal.Decimal
}
