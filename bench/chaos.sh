#!/usr/bin/env bash
# Failure matrix. Each scenario runs a steady write load, injects one fault,
# heals it, stops the load and then requires bench reconcile to report zero
# missing/stale/extra documents. Appends a JSON line per scenario to
# $CHAOS_RESULTS/chaos.jsonl (default bench/results). Usage: bench/chaos.sh [scenario...]
set -euo pipefail
cd "$(dirname "$0")/.."
RATE=${RATE:-2000}
LOAD=${LOAD:-120}       # seconds of writes per scenario
FAULT_AT=${FAULT_AT:-20}
RESULTS=${CHAOS_RESULTS:-bench/results}
# COMPOSE_EXTRA: an extra compose file layered on top (e.g. other host ports).
dc() { docker compose -f deploy/docker-compose.yml ${COMPOSE_EXTRA:+-f "$COMPOSE_EXTRA"} --profile chaos "$@"; }
hwm() { dc exec -T redpanda rpk topic describe "$1" -p | awk 'NR>1{s+=$6} END{print s+0}'; }

# gaos scenario: 429 for every _bulk between $1 and $2 (UTC), proxy otherwise.
# rate N instead: every (N+1)-th _bulk gets 429.
gaos_scenario() {
  local bulk
  if [[ $1 == rate ]]; then
    bulk="\"rate\": $2"
  else
    bulk="\"start\": \"$1\", \"end\": \"$2\""
  fi
  local proxy='{"result": {"type": "redirect", "content": {"host": "http://elasticsearch:9200"}}}'
  local reject='{"status": 429, "result": {"type": "static", "content": {"error": {"type": "es_rejected_execution_exception", "reason": "injected by gaos"}, "status": 429}}}'
  if [[ $1 == rate ]]; then local accept=$proxy ignore=$reject; else local accept=$reject ignore=$proxy; fi
  cat > deploy/gaos/scenario.json <<JSON
{
  "service": {"es": {"port": 9200, "path": {
    "/_bulk": {"scenario": "bulk", "method": "POST"},
    "/products": {"scenario": "proxy", "method": "PUT"}}}},
  "scenario": {
    "bulk": {"name": "bulk", $bulk, "accept": $accept, "ignore": $ignore},
    "proxy": {"name": "proxy", "accept": $proxy}
  }
}
JSON
}
utc_in() { date -u -v+"$1"S +%Y-%m-%dT%H:%M:%S.000000Z 2>/dev/null || date -u -d "+$1 seconds" +%Y-%m-%dT%H:%M:%S.000000Z; }

use_gaos() {
  dc up -d --build --force-recreate gaos >/dev/null 2>&1
  INDEXER_ES_URL=http://gaos:9200 dc up -d indexer >/dev/null 2>&1
}
use_es() { dc up -d indexer >/dev/null 2>&1; dc rm -sf gaos >/dev/null 2>&1; }
kill_for() { dc kill -s KILL "$1" >/dev/null; sleep "$2"; dc start "$1" >/dev/null; }

fault() {
  case $1 in
    kill-indexer)       sleep "$FAULT_AT"; kill_for indexer 10 ;;
    kill-indexer-3x)    for _ in 1 2 3; do sleep "$FAULT_AT"; kill_for indexer 5; done ;;
    kill-cdc)           sleep "$FAULT_AT"; kill_for cdc 10 ;;
    kill-broker)        sleep "$FAULT_AT"; kill_for redpanda 20 ;;
    restart-postgres)   sleep "$FAULT_AT"; dc restart -t 0 postgres >/dev/null ;;
    es-429-every-2nd)   : ;;
    es-429-60s)         : ;;
    es-429-60s-kill-indexer) sleep $((FAULT_AT + 45)); kill_for indexer 5 ;;
    # kill while kafka-cronsumer drains the retry topic after the 429 window
    es-429-60s-kill-in-drain)
      sleep $((FAULT_AT + 10 + 60 + ${DRAIN_AT:-8}))
      dc exec -T redpanda rpk group describe products-indexer > "$RESULTS/chaos-logs/$2.group-at-kill.txt" 2>&1 || true
      kill_for indexer 5 ;;
    *) echo "unknown scenario $1" >&2; exit 2 ;;
  esac
}

run() {
  local s=$1 tag since
  tag="$s.$(date -u +%H%M%S)"
  since=$(date -u +%FT%TZ)
  mkdir -p "$RESULTS/chaos-logs"
  # Settle first: the 429 window is absolute, so nothing slow may sit between
  # setting it and starting the load.
  dc run --rm -T reconcile -wait 10m >/dev/null
  case $s in
    es-429-every-2nd) gaos_scenario rate 1; use_gaos ;;
    es-429-60s|es-429-60s-kill-indexer|es-429-60s-kill-in-drain) gaos_scenario "$(utc_in $((FAULT_AT + 10)))" "$(utc_in $((FAULT_AT + 70)))"; use_gaos ;;
    *) use_es ;;
  esac
  local retry0 dlq0 retry1 dlq1
  retry0=$(hwm products.retry) dlq0=$(hwm products.dlq)

  dc rm -sf chaos-load >/dev/null 2>&1 || true
  docker rm -f chaos-load >/dev/null 2>&1 || true
  dc run -d -T --name chaos-load loadgen -rate "$RATE" -duration "${LOAD}s" -warmup 0s -sample-every 1000000000 >/dev/null
  fault "$s" "$tag"
  docker wait chaos-load >/dev/null
  docker logs chaos-load > "$RESULTS/chaos-logs/$tag.log" 2>&1
  local load; load=$(grep '"writes"' "$RESULTS/chaos-logs/$tag.log" | tail -1); docker rm chaos-load >/dev/null

  local t0=$SECONDS rec status=ok
  rec=$(dc run --rm -T reconcile -wait 15m 2>/dev/null | tail -1) || status=FAILED
  local converge=$((SECONDS - t0))
  retry1=$(hwm products.retry) dlq1=$(hwm products.dlq)
  dc logs --no-color --since "$since" indexer > "$RESULTS/chaos-logs/$tag.indexer.log" 2>&1 || true
  [[ $s == es-* ]] && use_es
  local writes; writes=$(sed -E 's/.*"writes":([0-9]+).*/\1/' <<<"$load")
  echo "{\"scenario\":\"$s\",\"result\":\"$status\",\"rate\":$RATE,\"writes\":$writes,\"retry_topic_msgs\":$((retry1 - retry0)),\"dlq_msgs\":$((dlq1 - dlq0)),\"converge_s\":$converge,\"reconcile\":$rec,\"at\":\"$(date -u +%FT%TZ)\"}" \
    | tee -a "$RESULTS/chaos.jsonl"
}

dc up -d --build --wait postgres redpanda elasticsearch cdc indexer >/dev/null
scenarios=("$@")
(( ${#scenarios[@]} )) || scenarios=(kill-indexer kill-indexer-3x kill-cdc kill-broker restart-postgres es-429-every-2nd es-429-60s es-429-60s-kill-indexer es-429-60s-kill-in-drain)
for s in "${scenarios[@]}"; do run "$s"; done
