#!/usr/bin/env bash
# Write→searchable latency at fixed write rates. Appends one JSON line per run
# to bench/results/latency.jsonl. Usage: bench/latency.sh [runs] [duration]
set -euo pipefail
cd "$(dirname "$0")/.."
runs=${1:-3}
duration=${2:-60s}
dc() { docker compose -f deploy/docker-compose.yml "$@"; }

dc up -d --build --wait postgres redpanda elasticsearch cdc indexer >/dev/null
rows=$(dc exec -T postgres psql -U app -d catalog -tAc "SELECT count(*) FROM products")
if (( rows < 1000000 )); then
  dc run --rm loadgen -seed $((1000000 - rows))
fi
dc run --rm reconcile -wait 10m >/dev/null

for rate in 1000 5000 10000; do
  for run in $(seq "$runs"); do
    out=$(dc run --rm -T loadgen -rate "$rate" -duration "$duration" -workers "${WORKERS:-256}" 2>/dev/null | tail -1)
    load=$(uptime | sed 's/.*load averages*: //')
    echo "{\"run\":$run,\"host_load\":\"$load\",\"at\":\"$(date -u +%FT%TZ)\",${out#\{}" | tee -a bench/results/latency.jsonl
  done
done
dc run --rm reconcile -wait 5m
