#!/usr/bin/env bash
set -euo pipefail
NS=bouine-test
OUT=/results/4.2_gossip_convergence; mkdir -p "$OUT"

kubectl -n "$NS" port-forward svc/bouine 18080:80 &
PF_PID=$!; sleep 3

# Background load
k6 run --env TARGET=http://127.0.0.1:18080/hit --env RATE=50000 \
  --env DURATION=180s --out json="$OUT/load.json" \
  /scenarios/lib/const_rate.js &
K6_PID=$!

sleep 30
echo "$(date): killing bouine-2" | tee "$OUT/timeline.txt"
kubectl -n "$NS" delete pod bouine-2 --force --grace-period=0

# Watch until bouine-2 rejoins (check cluster peers)
for i in $(seq 1 60); do
  peers=$(kubectl exec -n "$NS" bouine-0 -- /bouine cluster peers 2>/dev/null | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)
  echo "$(date): peers=$peers" | tee -a "$OUT/timeline.txt"
  [ "$peers" -ge 3 ] && { echo "$(date): cluster restored" | tee -a "$OUT/timeline.txt"; break; }
  sleep 2
done

wait "$K6_PID" || true
kill "$PF_PID" 2>/dev/null || true
