#!/usr/bin/env bash
set -euo pipefail
NS=bouine-test
OUT=/results/5.5_purge_broadcast; mkdir -p "$OUT"

kubectl -n "$NS" port-forward svc/bouine 18080:80 18090:9000 &
PF_PID=$!; sleep 3
TOKEN="dev-secret"

k6 run --env TARGET=http://127.0.0.1:18080/hit --env RATE=10000 \
  --env DURATION=60s --out json="$OUT/load.json" \
  /scenarios/lib/const_rate.js &
K6_PID=$!

# Burst 100 purges/sec for 10s
sleep 15
echo "$(date): starting purge burst" | tee "$OUT/timeline.txt"
for i in $(seq 1 100); do
  python3 -c "
import urllib.request, json
req = urllib.request.Request(
  'http://127.0.0.1:18090/v1/purge',
  data=json.dumps({'url':'http://127.0.0.1:18080/hit'}).encode(),
  headers={'Authorization':'Bearer $TOKEN','Content-Type':'application/json'},
  method='POST')
urllib.request.urlopen(req)
" &
done; wait
echo "$(date): purge burst done" | tee -a "$OUT/timeline.txt"

wait "$K6_PID" || true
kill "$PF_PID" 2>/dev/null || true
