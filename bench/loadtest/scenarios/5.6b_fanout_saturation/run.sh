#!/usr/bin/env bash
# §5.6.b Fan-out saturation at scale
# 1 dashboard session polling overview on a 10-node cluster under 50k RPS.
# Measures fan-out RTT, stale badge %, admin queue depth, data-plane p99.
set -euo pipefail
NS=bouine-test
OUT=/results/5.6b_fanout_saturation; mkdir -p "$OUT"
TOKEN="${ADMIN_TOKEN:-dev-secret}"
DURATION=120s
ADMIN_ADDR="${BOUINE_ADMIN:-http://bouine:9000}"
DATA_ADDR="${BOUINE_ADDR:-http://bouine:8080}"

echo "=== §5.6.b Fan-out saturation at scale ==="

# Background: run sustained 50k RPS mixed load
k6 run \
  --env TARGET="$DATA_ADDR" \
  --env RATE=50000 \
  --env DURATION="$DURATION" \
  --out json="$OUT/load.json" \
  /scenarios/3.6_mixed_realistic/k6.js &
K6_PID=$!

# Background: poll the overview endpoint every 5s, record response time
python3 -c "
import urllib.request, time, json, os

admin = os.environ.get('BOUINE_ADMIN','$ADMIN_ADDR')
out   = '$OUT'
token = '$TOKEN'

results = []
start   = time.time()
while time.time() - start < 110:
    t0 = time.perf_counter()
    try:
        req = urllib.request.Request(
            f'{admin}/v1/peer/metrics',
            headers={'Authorization': f'Bearer {token}'}
        )
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read())
        elapsed_ms = (time.perf_counter() - t0) * 1000
        peer_count = len(data) if isinstance(data, list) else 1
        results.append({'ts': time.time(), 'fanout_ms': elapsed_ms,
                         'peers': peer_count, 'error': None})
    except Exception as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        results.append({'ts': time.time(), 'fanout_ms': elapsed_ms,
                         'peers': 0, 'error': str(e)})
    time.sleep(5)

with open(f'{out}/fanout_timings.json', 'w') as f:
    json.dump(results, f, indent=2)

# Summarise
if results:
    times = [r['fanout_ms'] for r in results]
    errors = sum(1 for r in results if r['error'])
    print(f'Fan-out samples={len(times)} p50={sorted(times)[len(times)//2]:.1f}ms '
          f'p99={sorted(times)[int(len(times)*0.99)]:.1f}ms errors={errors}')
" &
FANOUT_PID=$!

wait "$K6_PID" || true
wait "$FANOUT_PID" || true

# Snapshot Prometheus metrics from bouine admin
python3 -c "
import urllib.request, os
admin = os.environ.get('BOUINE_ADMIN','$ADMIN_ADDR')
try:
    resp = urllib.request.urlopen(f'{admin}/metrics', timeout=5)
    with open('$OUT/metrics.prom', 'wb') as f: f.write(resp.read())
except Exception as e: print('metrics scrape failed:', e)
" 2>/dev/null || true

echo "Done. Results in $OUT"
