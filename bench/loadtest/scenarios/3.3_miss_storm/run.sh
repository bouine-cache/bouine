#!/usr/bin/env bash
set -euo pipefail
OUT=/results/3.3_miss_storm; mkdir -p "$OUT"
for TUT in bouine nginx varnish envoy; do
  ADDR="${TUT^^}_ADDR"; addr="${!ADDR}"
  echo "=== §3.3 $TUT miss storm 10k RPS 120s ==="
  k6 run -q --env TARGET="$addr/miss" --env RATE=10000 --env DURATION=120s \
    --out json="$OUT/${TUT}.json" /scenarios/lib/const_rate.js \
    >"$OUT/${TUT}.log" 2>&1 \
    && echo "  $TUT OK" || echo "  $TUT FAILED (log: $OUT/${TUT}.log)"
done
