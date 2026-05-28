#!/usr/bin/env bash
set -euo pipefail
OUT=/results/5.4_request_collapsing; mkdir -p "$OUT"
echo "=== §5.4 request collapsing: 10k concurrent requests to 1 URL ==="
k6 run --env TARGET="${BOUINE_ADDR:-http://bouine:8080}/slow?ms=200" \
  --out json="$OUT/bouine.json" /scenarios/5.4_request_collapsing/k6.js
