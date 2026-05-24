#!/bin/sh
docker build --platform linux/amd64 -t ghcr.io/kivapple/marketdata:latest . && \
docker push ghcr.io/kivapple/marketdata:latest
