#!/usr/bin/env bash
# §5.6.c Purge/ban via dashboard under load
# 3-node cluster at 10k RPS; operator issues 10 purges + 5 bans via admin API.
# Measures data-plane p99, ops-log write latency, peer broadcast time,
# and admin response time.
set -euo pipefail
NS=bouine-test
OUT=/results/5.6c_dashboard_invalidation; mkdir -p "$OUT"
TOKEN="${ADMIN_TOKEN:-dev-secret}"
DATA_ADDR="${BOUINE_ADDR:-http://bouine:8080}"
ADMIN_ADDR="${BOUINE_ADMIN:-http://bouine:9000}"

echo "=== §5.6.c Purge/ban via dashboard under load ==="

# Warm the cache
echo "Warming cache..."
k6 run \
  --env TARGET="$DATA_ADDR" \
  --env RATE=2000 \
  --env DURATION=20s \
  --out json="$OUT/warmup.json" \
  /scenarios/lib/const_rate.js 2>&1 | tail -5

# Start background load
k6 run \
  --env TARGET="$DATA_ADDR" \
  --env RATE=10000 \
  --env DURATION=90s \
  --out json="$OUT/load.json" \
  /scenarios/lib/const_rate.js &
K6_PID=$!

# Wait until load is flowing
sleep 15

echo "$(date): starting invalidation burst" | tee "$OUT/timeline.txt"

# 10 purges spread over 30s
for i in $(seq 1 10); do
  T0=$(python3 -c "import time; print(time.perf_counter())")
  python3 -c "
import urllib.request, json, time
t0 = time.perf_counter()
req = urllib.request.Request(
  '$ADMIN_ADDR/v1/purge',
  data=json.dumps({'url':'$DATA_ADDR/hit'}).encode(),
  headers={'Authorization':'Bearer $TOKEN','Content-Type':'application/json'},
  method='POST')
try:
  resp = urllib.request.urlopen(req, timeout=5)
  elapsed = (time.perf_counter()-t0)*1000
  print(f'purge $i ok {elapsed:.1f}ms')
except Exception as e:
  elapsed = (time.perf_counter()-t0)*1000
  print(f'purge $i err {elapsed:.1f}ms {e}')
" 2>&1 | tee -a "$OUT/timeline.txt"
  sleep 3
done

# 5 bans
for i in $(seq 1 5); do
  python3 -c "
import urllib.request, json, time
t0 = time.perf_counter()
req = urllib.request.Request(
  '$ADMIN_ADDR/v1/ban',
  data=json.dumps({'pattern':'path ~ ^/hit','ttl':60}).encode(),
  headers={'Authorization':'Bearer $TOKEN','Content-Type':'application/json'},
  method='POST')
try:
  resp = urllib.request.urlopen(req, timeout=5)
  elapsed = (time.perf_counter()-t0)*1000
  print(f'ban $i ok {elapsed:.1f}ms')
except Exception as e:
  elapsed = (time.perf_counter()-t0)*1000
  print(f'ban $i err {elapsed:.1f}ms {e}')
" 2>&1 | tee -a "$OUT/timeline.txt"
  sleep 6
done

echo "$(date): invalidation burst done" | tee -a "$OUT/timeline.txt"

wait "$K6_PID" || true

# Fetch ops log
python3 -c "
import urllib.request, os
admin = os.environ.get('BOUINE_ADMIN','$ADMIN_ADDR')
try:
    req = urllib.request.Request(f'{admin}/v1/ops-log',
      headers={'Authorization':'Bearer $TOKEN'})
    resp = urllib.request.urlopen(req, timeout=5)
    with open('$OUT/ops_log.json','wb') as f: f.write(resp.read())
except Exception as e: print('ops-log fetch failed:', e)
" 2>/dev/null || true

echo "Done. Results in $OUT"
