#!/usr/bin/env bash
# §3.7 — Payload integrity under eviction churn (data-integrity net).
# 50k unique 64 KiB deterministic payloads vs a 64 MiB hot budget:
# every response byte-validated; competitors are the control group.
set -euo pipefail
OUT=/results/3.7_payload_integrity
mkdir -p "$OUT"

for TUT in bouine nginx varnish envoy; do
  ADDR="${TUT^^}_ADDR"; addr="${!ADDR}"
  echo "=== §3.7 $TUT payload integrity 2k RPS 60s ==="
  k6 run -q \
    --env BASE_URL="$addr" \
    --out json="$OUT/${TUT}.json" \
    /scenarios/3.7_payload_integrity/k6.js >"$OUT/${TUT}.log" 2>&1 \
    && echo "  $TUT OK" || echo "  $TUT FAILED (log: $OUT/${TUT}.log)"
done
