#!/usr/bin/env bash
set -euo pipefail
OUT=/results/3.4_working_set_overflow; mkdir -p "$OUT"
for TUT in bouine nginx varnish envoy; do
  ADDR="${TUT^^}_ADDR"; addr="${!ADDR}"
  echo "=== §3.4 $TUT working set overflow 5k RPS 300s ==="
  k6 run --env BASE_URL="$addr" --out json="$OUT/${TUT}.json" \
    /scenarios/3.4_working_set_overflow/k6.js
done
