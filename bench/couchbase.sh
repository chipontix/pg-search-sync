#!/usr/bin/env bash
# Couchbase → go-dcp-elasticsearch path: write→searchable latency and DCP lag
# at fixed write rates, then reconcile Couchbase against products_cb.
# Appends JSON lines to bench/results/couchbase.jsonl. Usage: bench/couchbase.sh [runs] [duration]
set -euo pipefail
cd "$(dirname "$0")/.."
runs=${1:-3}
duration=${2:-60s}
dc() { docker compose -f deploy/docker-compose.yml --profile couchbase "$@"; }
cb=(-e CB_QUERY_URL=http://couchbase:8093 -e ES_INDEX=products_cb)

dc build -q dcp-indexer
dc up -d --wait elasticsearch couchbase >/dev/null
dc up -d dcp-indexer >/dev/null   # waits for couchbase-init
until dc exec -T elasticsearch curl -fs localhost:9200/products_cb >/dev/null 2>&1; do sleep 2; done
docs=$(dc exec -T couchbase curl -s -u admin:password localhost:8093/query/service \
  --data-urlencode 'statement=SELECT RAW COUNT(*) FROM catalog' | tr -d ' \n' | sed -E 's/.*"results":\[([0-9]+)\].*/\1/')
if (( docs < 100000 )); then
  dc run --rm -T "${cb[@]}" loadgen -source couchbase -seed $((100000 - docs))
fi
dc run --rm -T "${cb[@]}" reconcile -source couchbase -wait 5m >/dev/null

for rate in 1000 5000; do
  for run in $(seq "$runs"); do
    out=$(dc run --rm -T "${cb[@]}" -e DCP_METRICS=http://dcp-indexer:18092/metrics loadgen \
      -source couchbase -rate "$rate" -duration "$duration" -workers "${WORKERS:-128}" 2>/dev/null | tail -1)
    load=$(uptime | sed 's/.*load averages*: //')
    echo "{\"run\":$run,\"host_load\":\"$load\",\"at\":\"$(date -u +%FT%TZ)\",${out#\{}" | tee -a bench/results/couchbase.jsonl
  done
done
dc run --rm -T "${cb[@]}" reconcile -source couchbase -wait 5m | tee -a bench/results/couchbase.jsonl
