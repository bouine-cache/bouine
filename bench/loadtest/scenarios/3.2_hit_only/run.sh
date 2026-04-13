#!/usr/bin/env bash
# §3.2 — Cache-hit-only baseline
# Pre-warms 10k keys then measures pure hit-path at 50k RPS for 120s.
set -euo pipefail
OUT=/results/3.2_hit_only
mkdir -p "$OUT"

for TUT in bouine nginx varnish envoy; do
  ADDR="${TUT^^}_ADDR"; addr="${!ADDR}"
  # Warm-up: populate 10k keys
  echo "Warming $TUT..."
  k6 run --env TARGET="$addr" --env MODE=warmup \
    /scenarios/3.2_hit_only/k6_warmup.js --quiet

  echo "=== §3.2 $TUT hit-only 50k RPS 120s ==="
  k6 run \
    --env TARGET="$addr/hit" \
    --env RATE=50000 \
    --env DURATION=120s \
    --out json="$OUT/${TUT}.json" \
    /scenarios/lib/const_rate.js
done
