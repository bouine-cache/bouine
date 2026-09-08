#!/usr/bin/env bash
# §3.1 — Single-node throughput ramp
# Ramps RPS in steps against all 4 TUTs. The 50k/100k legs are trimmed
# from the default: 50k duplicates §3.2's sustained 50k measurement, and
# 100k exceeds what the single-node compose topology can drive cleanly
# (k6 drops iterations; the numbers are client-side artifacts, not proxy
# limits). Restore ad hoc with:
#   RATES_OVERRIDE="1000 5000 10000 25000 50000 100000" bash run.sh
# Requires: k6, results dir mounted at /results
set -euo pipefail

if [ -n "${RATES_OVERRIDE:-}" ]; then
  read -ra RATES <<< "$RATES_OVERRIDE"
else
  RATES=(1000 5000 10000 25000)
fi
DURATION=60
OUT=/results/3.1_throughput_ramp
mkdir -p "$OUT"

for TUT in bouine nginx varnish envoy; do
  ADDR="${TUT^^}_ADDR"
  addr="${!ADDR}"
  echo "=== §3.1 $TUT ==="
  for rate in "${RATES[@]}"; do
    # k6 output goes to a per-rate log file in the results dir (pulled
    # by CI as artifacts, greppable on failure) — the console gets one
    # line of verdict, not 60 lines of 1-per-second progress.
    k6 run -q \
      --env TARGET="$addr/hit" \
      --env RATE="$rate" \
      --env DURATION="${DURATION}s" \
      --out json="$OUT/${TUT}_${rate}rps.json" \
      /scenarios/lib/const_rate.js >"$OUT/${TUT}_${rate}rps.log" 2>&1 \
      && echo "  rate=$rate RPS ${DURATION}s OK" \
      || echo "  rate=$rate RPS FAILED (log: $OUT/${TUT}_${rate}rps.log)"
  done
done
echo "§3.1 done — results in $OUT"
