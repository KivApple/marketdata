package domain

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
	TickSize    float64
	StepSize    float64
	MinNotional float64
}
