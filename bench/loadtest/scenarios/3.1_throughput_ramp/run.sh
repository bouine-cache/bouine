#!/usr/bin/env bash
# §3.1 — Single-node throughput ramp
# Ramps from 1k to 100k RPS in steps, 60s each, against all 4 TUTs.
# Requires: k6, results dir mounted at /results
set -euo pipefail

RATES=(1000 5000 10000 25000 50000 100000)
DURATION=60
OUT=/results/3.1_throughput_ramp
mkdir -p "$OUT"

for TUT in bouine nginx varnish envoy; do
  ADDR="${TUT^^}_ADDR"
  addr="${!ADDR}"
  echo "=== §3.1 $TUT ===" | tee -a "$OUT/$TUT.log"
  for rate in "${RATES[@]}"; do
    echo "  rate=$rate RPS duration=${DURATION}s"
    k6 run \
      --env TARGET="$addr/hit" \
      --env RATE="$rate" \
      --env DURATION="${DURATION}s" \
      --out json="$OUT/${TUT}_${rate}rps.json" \
      /scenarios/lib/const_rate.js 2>&1 | tee -a "$OUT/$TUT.log"
  done
done
echo "§3.1 done — results in $OUT"
