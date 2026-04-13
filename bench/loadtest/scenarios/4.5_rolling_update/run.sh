#!/usr/bin/env bash
set -euo pipefail
NS=bouine-test
OUT=/results/4.5_rolling_update; mkdir -p "$OUT"

kubectl -n "$NS" port-forward svc/bouine 18080:80 &
PF_PID=$!; sleep 3

# Background load — track errors
k6 run --env TARGET=http://127.0.0.1:18080/hit --env RATE=10000 \
  --env DURATION=180s --out json="$OUT/load.json" \
  /scenarios/lib/const_rate.js &
K6_PID=$!

sleep 30
echo "$(date): triggering rolling restart" | tee "$OUT/timeline.txt"
kubectl -n "$NS" rollout restart statefulset/bouine
kubectl -n "$NS" rollout status statefulset/bouine --timeout=180s | \
  while IFS= read -r line; do echo "$(date): $line"; done | tee -a "$OUT/timeline.txt"

wait "$K6_PID" || true
kill "$PF_PID" 2>/dev/null || true
echo "§4.5 done — check $OUT/load.json for error rate during rollout"
