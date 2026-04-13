#!/usr/bin/env bash
set -euo pipefail
OUT=/results/5.3_slow_origin; mkdir -p "$OUT"
echo "=== §5.3 slow origin 5s latency 1k RPS ==="
k6 run --env TARGET="${BOUINE_ADDR:-http://bouine:8080}/slow?ms=5000" \
  --env RATE=1000 --env DURATION=120s \
  --out json="$OUT/bouine.json" /scenarios/lib/const_rate.js
