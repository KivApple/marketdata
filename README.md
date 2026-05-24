# marketdata

Ingests OHLCV candles and exchange symbols from cryptocurrency exchanges (currently Binance — spot REST, WebSocket streams and historical S3 archives) into ClickHouse. The ingester continuously streams closed candles in real time and backfills history on startup. Prices and volumes are stored as `Decimal128(8)` for lossless precision.

Exposes `/api/health` and Prometheus metrics at `/api/metrics`. Schema migrations run automatically on startup.

## Run

```sh
docker run -d --name marketdata \
  -p 8080:8080 \
  -e CLICKHOUSE_DSN='clickhouse://user:password@clickhouse:9000/marketdata' \
  -e CANDLE_INTERVAL=1m \
  ghcr.io/kivapple/marketdata:latest
```

## Configuration

| Variable           | Required | Default | Description                                                                 |
| ------------------ | -------- | ------- | --------------------------------------------------------------------------- |
| `CLICKHOUSE_DSN`   | yes      | —       | ClickHouse DSN, e.g. `clickhouse://user:pass@host:9000/marketdata`          |
| `CANDLE_INTERVAL`  | no       | `1m`    | Candle timeframe to ingest (`1s`, `1m`, `15m`, `1h`, …)                     |
| `LISTEN_ADDR`      | no       | `:8080` | HTTP listen address for health and metrics endpoints                        |

`.env` files in the working directory are loaded automatically.

## Storage footprint

A full Binance spot backfill at **15m** fits in **under 10 GiB** in ClickHouse.
**1m** candles should land **under 150 GiB**, 
and **1s** candles a few hundred GiB — finer timeframes
compress better thanks to repeating OHLC and 
zero-volume runs.

## License

MIT — see [LICENSE](LICENSE).
