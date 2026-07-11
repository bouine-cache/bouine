#!/usr/bin/env bash
# §0.1 — Uncapped throughput ramp
# Ramps from 1k to 200k RPS to find the true max throughput ceiling.
# Pre-warms 10k keys then runs the ramp for 2.5 minutes per proxy.
set -euo pipefail
OUT=/results/0.1_uncapped
mkdir -p "$OUT"

for TUT in bouine nginx varnish envoy; do
  ADDR="${TUT^^}_ADDR"; addr="${!ADDR}"
  # Warm-up: populate 10k keys
  echo "Warming $TUT..."
  k6 run --env TARGET="$addr" --env MODE=warmup \
    /scenarios/3.2_hit_only/k6_warmup.js --quiet

  echo "=== §0.1 $TUT uncapped ramp 1k→200k RPS ==="
  k6 run \
    --env TARGET="$addr/hit" \
    --out json="$OUT/${TUT}.json" \
    /scenarios/0.1_uncapped_throughput/k6_ramp.js
done
