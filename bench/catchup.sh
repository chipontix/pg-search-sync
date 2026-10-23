#!/usr/bin/env bash
# Catch-up of N rows for one connector. Usage:
#   CPUS=1 MEM=1g bench/catchup.sh gopqcdc|debezium [rows]
# Appends a JSON line to bench/results/catchup.jsonl.
set -euo pipefail
cd "$(dirname "$0")/catchup"
connector=$1
rows=${2:-10000000}
export CPUS=${CPUS:-1} MEM=${MEM:-1g}
dc() { docker compose "$@"; }
psql() { dc exec -T postgres psql -U app -d catalog -tAq "$@"; }
rpk() { dc exec -T redpanda rpk "$@"; }
case $connector in
  gopqcdc) topic=products.cdc slot=products_slot ;;
  debezium) topic=pg.public.products slot=debezium ;;
  *) echo "unknown connector $connector" >&2; exit 2 ;;
esac

dc down -v -t 5 >/dev/null 2>&1
dc up -d --wait postgres redpanda >/dev/null
rpk topic create "$topic" -p 6 -r 1 >/dev/null
dc up -d "$connector" >/dev/null
if [[ $connector == debezium ]]; then
  until dc exec -T debezium curl -fs localhost:8083/connectors >/dev/null 2>&1; do sleep 2; done
  dc exec -T debezium curl -fs -X POST -H 'Content-Type: application/json' \
    --data-binary @- localhost:8083/connectors < debezium.json >/dev/null
fi
until [[ $(psql -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$slot' AND active") == 1 ]]; do sleep 1; done
sleep 5
dc stop -t 30 "$connector" >/dev/null

echo "writing $rows rows with $connector stopped" >&2
for ((done_ = 0; done_ < rows; done_ += 100000)); do
  n=$(( rows - done_ < 100000 ? rows - done_ : 100000 ))
  psql -c "INSERT INTO products (sku, title, brand, category, price_cents, stock)
           SELECT 'SKU-' || i, 'catch-up product ' || i, 'Brand ' || (i % 50), 'cat-' || (i % 20), i % 1000000, i % 1000
           FROM generate_series($done_ + 1, $done_ + $n) i"
done
lag=$(psql -c "SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots WHERE slot_name='$slot'")
echo "slot $slot is behind by $lag" >&2

dc start "$connector" >/dev/null
start=$(date +%s.%N)
id=$(dc ps -q --no-trunc "$connector")
cg=/sys/fs/cgroup/docker/$id
samples=$(mktemp "$HOME/.catchup-samples.XXXX")
# Sample the container cgroup once a second: wall time, CPU µs, anon memory, total memory.
docker run --rm -d --name catchup-sampler -v /sys/fs/cgroup:/cg:ro -v "$samples:/out" alpine sh -c \
  "while :; do echo \$(date +%s.%N) \$(awk '/^usage_usec/{print \$2}' /cg/docker/$id/cpu.stat) \$(awk '/^anon /{print \$2}' /cg/docker/$id/memory.stat) \$(cat /cg/docker/$id/memory.current) >> /out; sleep 1; done" >/dev/null

total=0
while (( total < rows )); do
  sleep 1
  total=$(rpk topic describe "$topic" -p | awk 'NR>1{s+=$6} END{print s+0}')
done
end=$(date +%s.%N)
docker rm -f catchup-sampler >/dev/null
# True high-water mark of the cgroup (includes page cache), next to the sampled anon (≈RSS).
peak_cg=$(docker run --rm -v /sys/fs/cgroup:/cg:ro alpine cat "/cg/docker/$id/memory.peak")

elapsed=$(echo "$end - $start" | bc)
read -r cpu_s peak_cores peak_anon peak_mem < <(awk '
  NR==1 {t0=$1; u0=$2; pt=$1; pu=$2}
  {if (NR>1 && $1>pt) {c=($2-pu)/1e6/($1-pt); if (c>pc) pc=c}; pt=$1; pu=$2
   if ($3>pa) pa=$3; if ($4>pm) pm=$4; u=$2}
  END {printf "%.1f %.2f %.0f %.0f\n", (u-u0)/1e6, pc, pa/1048576, pm/1048576}' "$samples")
rm -f "$samples"
line=$(printf '{"connector":"%s","rows":%d,"messages":%d,"cpus":"%s","mem_limit":"%s","elapsed_s":%.1f,"rows_per_s":%.0f,"cpu_s":%s,"peak_cores":%s,"peak_anon_mb":%s,"peak_mem_mb":%s,"cgroup_peak_mb":%d,"wal_behind":"%s","host_load":"%s","at":"%s"}' \
  "$connector" "$rows" "$total" "$CPUS" "$MEM" "$elapsed" "$(echo "$rows / $elapsed" | bc)" "$cpu_s" "$peak_cores" "$peak_anon" "$peak_mem" "$((peak_cg / 1048576))" \
  "$lag" "$(uptime | sed 's/.*load averages*: //')" "$(date -u +%FT%TZ)")
echo "$line" | tee -a ../results/catchup.jsonl
dc down -v -t 5 >/dev/null 2>&1
