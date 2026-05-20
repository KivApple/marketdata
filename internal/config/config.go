package config

import (
	"fmt"
	"marketdata/internal/domain"
	"marketdata/internal/storage"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	ClickHouse     storage.ClickHouseConfig `envPrefix:"CLICKHOUSE_"`
	CandleInterval domain.Interval          `env:"CANDLE_INTERVAL" envDefault:"1m"`
}

func Load() (*Config, error) {
	_ = godotenv.Load()
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("parse env: %w", err)
	}
	return &cfg, nil
}
