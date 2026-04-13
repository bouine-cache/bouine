#!/usr/bin/env bash
set -euo pipefail
NS=bouine-test
OUT=/results/4.3_peer_fetch_pressure; mkdir -p "$OUT"

# Port-forward directly to pod 2 — its keys are owned by pods 0/1 → forces peer-fetch
kubectl -n "$NS" port-forward pod/bouine-2 18080:80 &
PF_PID=$!; sleep 3

for rate in 5000 10000 25000; do
  echo "=== §4.3 peer-fetch pressure $rate RPS ==="
  k6 run --env TARGET=http://127.0.0.1:18080/hit --env RATE="$rate" \
    --env DURATION=60s --out json="$OUT/${rate}rps.json" \
    /scenarios/lib/const_rate.js
done

kill "$PF_PID" 2>/dev/null || true
