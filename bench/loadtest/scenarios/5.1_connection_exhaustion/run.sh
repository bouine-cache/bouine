#!/usr/bin/env bash
set -euo pipefail
OUT=/results/5.1_connection_exhaustion; mkdir -p "$OUT"
for conns in 1000 5000 10000 50000; do
  echo "=== §5.1 $conns concurrent connections ==="
  k6 run --env TARGET="${BOUINE_ADDR:-http://bouine:8080}/hit" \
    --env VUS="$conns" --env DURATION=60s \
    --out json="$OUT/${conns}conns.json" /scenarios/5.1_connection_exhaustion/k6.js
done
