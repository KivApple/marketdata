FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/marketdata ./cmd/marketdata

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /out/marketdata /usr/local/bin/marketdata

ENTRYPOINT ["/usr/local/bin/marketdata"]
