package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ExchangeSymbols = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "exchange_symbols",
			Help: "The number of exchange symbols",
		},
		[]string{"exchange"},
	)
	ExchangeTradingSymbols = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "exchange_trading_symbols",
			Help: "The number of trading exchange symbols",
		},
		[]string{"exchange"},
	)
	CandlesIngestedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "candles_ingested_total",
			Help: "The total number of ingested candles",
		},
		[]string{"exchange"},
	)
	ClosedCandlesIngestedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "closed_candles_ingested_total",
			Help: "The total number of ingested closed candles",
		},
		[]string{"exchange"},
	)
	BackfillErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "backfill_errors_total",
			Help: "The total number of backfill errors",
		},
		[]string{"exchange"},
	)
	BackfillDuration = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "backfill_duration_seconds",
			Help: "Backfill duration in seconds",
		},
		[]string{"exchange"},
	)
)
