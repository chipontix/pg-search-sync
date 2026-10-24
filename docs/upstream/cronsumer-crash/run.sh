#!/usr/bin/env bash
# Needs a broker on localhost:19092 (e.g. the pg-search-sync compose Redpanda).
# Kills the consumer with SIGKILL five times while it is processing, then lets
# a last instance drain the topic and counts which keys were never processed.
set -euo pipefail
cd "$(dirname "$0")"
go build -o repro .
rpk() { docker compose -f ../../../deploy/docker-compose.yml exec -T redpanda rpk "$@"; }
rpk topic delete repro.retry >/dev/null 2>&1 || true
rpk group delete repro >/dev/null 2>&1 || true
rm -f processed.log; touch processed.log
./repro produce "${N:-200}"
count() { sort -u processed.log | wc -l | tr -d ' '; }
for i in 1 2 3 4 5; do
  ./repro consume 2>/dev/null & pid=$!
  start=$(count)
  until (( $(count) >= start + ${STEP:-10} )) || (( $(count) >= ${N:-200} )); do sleep 0.05; done
  kill -9 "$pid"; wait "$pid" 2>/dev/null || true
  echo "kill $i after $(count) unique keys processed"
done
./repro consume 2>/dev/null & pid=$!
for _ in $(seq 90); do (( $(count) >= ${N:-200} )) && break; sleep 1; done
sleep 3; kill "$pid"; wait "$pid" 2>/dev/null || true
echo "processed unique: $(count) / ${N:-200}, processed total (with duplicates): $(wc -l < processed.log | tr -d ' ')"
echo "never processed: $(comm -23 <(for i in $(seq 0 $((${N:-200} - 1))); do printf 'k%03d\n' "$i"; done) <(sort -u processed.log) | tr '\n' ' ')"
rpk group describe repro | grep -E "repro.retry|TOPIC" || true
