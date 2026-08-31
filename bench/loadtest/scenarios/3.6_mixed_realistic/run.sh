#!/usr/bin/env bash
set -euo pipefail
OUT=/results/3.6_mixed_realistic; mkdir -p "$OUT"
for TUT in bouine nginx varnish envoy; do
  ADDR="${TUT^^}_ADDR"; addr="${!ADDR}"
  echo "=== §3.6 $TUT mixed 10k RPS 300s ==="
  k6 run -q --env BASE_URL="$addr" --out json="$OUT/${TUT}.json" \
    /scenarios/3.6_mixed_realistic/k6.js \
    >"$OUT/${TUT}.log" 2>&1 \
    && echo "  $TUT OK" || echo "  $TUT FAILED (log: $OUT/${TUT}.log)"
done
