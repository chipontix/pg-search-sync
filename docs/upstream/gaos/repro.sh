#!/usr/bin/env bash
# Reproduces three Trendyol/gaos bugs at 771d620 (needs go, git, curl, python3):
#   1. data races under concurrent requests (built with -race)
#   2. the redirect result drops the query string
#   3. a span with only "end" ignores it
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$work"' EXIT
git clone -q https://github.com/Trendyol/gaos "$work/gaos"
git -C "$work/gaos" checkout -q 771d620
(cd "$work/gaos" && go build -race -o "$work/gaos-race" .)

"$work/gaos-race" run -s "$here/s.json" > "$work/race.log" 2>&1 &
sleep 2
seq 1 400 | xargs -P 32 -I{} curl -s -o /dev/null -X POST localhost:19555/x || true
kill %1; wait %1 2>/dev/null || true
echo "1. data race reports: $(grep -c 'WARNING: DATA RACE' "$work/race.log")"

python3 "$here/echo.py" & sleep 0.5
"$work/gaos-race" run -s "$here/s2.json" > /dev/null 2>&1 &
sleep 2
echo "2. upstream saw: $(curl -s 'localhost:19557/proxy?refresh=wait_for&x=1')   (sent /proxy?refresh=wait_for&x=1)"
echo "3. span with end=2020-01-01 answered: $(curl -s -w ' %{http_code}' localhost:19557/span)   (expected the ignore action, 503)"
