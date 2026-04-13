#!/usr/bin/env bash
# §5.6.d Config reload via dashboard under load
# Single node under 25k RPS; trigger config reload and measure
# parse duration, data-plane p99 during apply, dropped requests.
set -euo pipefail
OUT=/results/5.6d_config_reload; mkdir -p "$OUT"
TOKEN="${ADMIN_TOKEN:-dev-secret}"
DATA_ADDR="${BOUINE_ADDR:-http://bouine:8080}"
ADMIN_ADDR="${BOUINE_ADMIN:-http://bouine:9000}"
CFG_PATH="${BOUINE_CFG_PATH:-/etc/bouine/config.yaml}"

echo "=== §5.6.d Config reload via dashboard under load ==="

# Start 25k RPS load (130s — enough for warmup + reload + tail)
k6 run \
  --env TARGET="$DATA_ADDR" \
  --env RATE=25000 \
  --env DURATION=130s \
  --out json="$OUT/load.json" \
  /scenarios/3.6_mixed_realistic/k6.js &
K6_PID=$!

sleep 30
echo "$(date): triggering config reload" | tee "$OUT/timeline.txt"

# Method 1: POST /v1/reload (admin API)
T0=$(python3 -c "import time; print(time.perf_counter())")
python3 -c "
import urllib.request, json, time
t0 = time.perf_counter()
req = urllib.request.Request(
  '$ADMIN_ADDR/v1/reload',
  data=b'{}',
  headers={'Authorization':'Bearer $TOKEN','Content-Type':'application/json'},
  method='POST')
try:
  resp = urllib.request.urlopen(req, timeout=10)
  body = json.loads(resp.read())
  elapsed = (time.perf_counter()-t0)*1000
  print(f'reload ok {elapsed:.1f}ms body={body}')
except Exception as e:
  elapsed = (time.perf_counter()-t0)*1000
  print(f'reload err {elapsed:.1f}ms {e}')
" 2>&1 | tee -a "$OUT/timeline.txt"

echo "$(date): reload triggered" | tee -a "$OUT/timeline.txt"

# Record goroutine count right after reload
sleep 2
python3 -c "
import urllib.request, os
admin = os.environ.get('BOUINE_ADMIN','$ADMIN_ADDR')
try:
    resp = urllib.request.urlopen(f'{admin}/metrics', timeout=5)
    lines = resp.read().decode().splitlines()
    for l in lines:
        if 'goroutine' in l or 'go_goroutines' in l or 'reload' in l.lower():
            print(l)
except Exception as e: print('metrics scrape failed:', e)
" 2>/dev/null | tee "$OUT/post_reload_metrics.txt" || true

wait "$K6_PID" || true

# CPU pprof snapshot
python3 -c "
import urllib.request, os
admin = os.environ.get('BOUINE_ADMIN','$ADMIN_ADDR')
try:
    resp = urllib.request.urlopen(f'{admin}/debug/pprof/goroutine?debug=1', timeout=10)
    with open('$OUT/goroutines.txt','wb') as f: f.write(resp.read())
except Exception as e: print('pprof fetch failed:', e)
" 2>/dev/null || true

echo "Done. Results in $OUT"
