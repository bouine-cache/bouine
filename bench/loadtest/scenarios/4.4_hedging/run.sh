#!/usr/bin/env bash
set -euo pipefail
OUT=/results/4.4_hedging; mkdir -p "$OUT"

for label in without_hedge with_hedge; do
  addr="${BOUINE_ADDR:-http://bouine:8080}"
  echo "=== §4.4 $label ==="
  k6 run --env TARGET="$addr/outlier" --env RATE=5000 \
    --env DURATION=120s --out json="$OUT/${label}.json" \
    /scenarios/lib/const_rate.js
done
