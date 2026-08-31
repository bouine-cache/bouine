#!/usr/bin/env bash
set -euo pipefail
OUT=/results/3.5_vary_blowup; mkdir -p "$OUT"
echo "=== §3.5 bouine Vary blow-up 1k RPS 60s ==="
k6 run -q --env BASE_URL="$BOUINE_ADDR" --env ADMIN="$BOUINE_ADMIN" \
  --out json="$OUT/bouine.json" /scenarios/3.5_vary_blowup/k6.js \
  >"$OUT/bouine.log" 2>&1 \
  && echo "  bouine OK" || echo "  bouine FAILED (log: $OUT/bouine.log)"
