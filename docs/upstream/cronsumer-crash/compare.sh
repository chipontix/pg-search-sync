#!/usr/bin/env bash
# One SIGKILL run of run.sh against a fresh single-node Kafka (apache/kafka:3.9.1)
# on localhost:$PORT, then the group's committed offset; the container is removed.
# Run on main for v1.8.11 and on branch cronsumer-commit-after-handled for the fix:
#   docs/upstream/cronsumer-crash/compare.sh v1.8.11 1500 20 2
#   docs/upstream/cronsumer-crash/compare.sh fork 1500 20 2
# Writes bench/results/cronsumer-crash/<label>-<WORK_MS>ms.log.
set -euo pipefail
cd "$(dirname "$0")/../../.."
label=$1; export WORK_MS=$2 N=$3 STEP=$4
PORT=${PORT:-25092}; export REPRO_BROKER=localhost:$PORT
name=pgss-cronsumer-kafka
out=bench/results/cronsumer-crash/$label-${WORK_MS}ms.log
mkdir -p bench/results/cronsumer-crash
docker rm -f $name >/dev/null 2>&1 || true
docker run -d --name $name -p $PORT:$PORT \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:$PORT,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$PORT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka:3.9.1 >/dev/null
trap 'docker rm -f $name >/dev/null' EXIT
until docker exec $name /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:$PORT --list >/dev/null 2>&1; do sleep 1; done
mod=$(cd docs/upstream/cronsumer-crash && go list -m -f '{{if .Replace}}{{.Replace.Path}}@{{.Replace.Version}}{{else}}{{.Path}}@{{.Version}}{{end}}' github.com/Trendyol/kafka-cronsumer)
{
  echo "# $label WORK_MS=$WORK_MS N=$N STEP=$STEP cronsumer=$mod at=$(date -u +%FT%TZ) host_load=\"$(sysctl -n vm.loadavg 2>/dev/null || cat /proc/loadavg)\""
  docs/upstream/cronsumer-crash/run.sh 2>&1
  docker exec $name /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:$PORT \
    --describe --group repro --timeout 10000 2>&1 | awk 'NF' || true
} | tee "$out"
