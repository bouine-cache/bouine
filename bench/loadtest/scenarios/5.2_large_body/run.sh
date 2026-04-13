#!/usr/bin/env bash
set -euo pipefail
OUT=/results/5.2_large_body; mkdir -p "$OUT"
echo "=== §5.2 large body 10 MiB at 100 RPS 60s ==="
k6 run --env TARGET="${BOUINE_ADDR:-http://bouine:8080}/large?kb=10240" \
  --env RATE=100 --env DURATION=60s \
  --out json="$OUT/bouine.json" /scenarios/lib/const_rate.js
