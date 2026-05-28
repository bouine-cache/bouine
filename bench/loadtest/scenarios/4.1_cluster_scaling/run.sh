#!/usr/bin/env bash
# Runs against the K8s cluster. Scale replicas 1→2→3→5→10, measure throughput.
set -euo pipefail
NS=bouine-test
OUT=/results/4.1_cluster_scaling; mkdir -p "$OUT"

for replicas in 1 2 3 5 10; do
  echo "=== §4.1 $replicas nodes ==="
  kubectl scale statefulset/bouine -n "$NS" --replicas="$replicas"
  kubectl rollout status statefulset/bouine -n "$NS" --timeout=120s
  sleep 10  # let ring buffers stabilise

  kubectl -n "$NS" port-forward svc/bouine 18080:80 &
  PF_PID=$!; sleep 3

  k6 run --env TARGET=http://127.0.0.1:18080/hit --env RATE=10000 \
    --env DURATION=60s --out json="$OUT/${replicas}nodes.json" \
    /scenarios/lib/const_rate.js

  kill "$PF_PID" 2>/dev/null || true
  sleep 2
done
kubectl scale statefulset/bouine -n "$NS" --replicas=3
