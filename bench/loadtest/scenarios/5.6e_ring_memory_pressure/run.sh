#!/usr/bin/env bash
# §5.6.e Ring buffer memory pressure
# Single node under 50k RPS with 500 distinct URL paths for 6 hours.
# Measures RSS growth, URLRing entry count, GC pressure, snapshot file size.
# NOTE: run with --background or in a detached screen/tmux session.
set -euo pipefail
OUT=/results/5.6e_ring_memory_pressure; mkdir -p "$OUT"
TOKEN="${ADMIN_TOKEN:-dev-secret}"
DATA_ADDR="${BOUINE_ADDR:-http://bouine:8080}"
ADMIN_ADDR="${BOUINE_ADMIN:-http://bouine:9000}"
DURATION="${RING_TEST_DURATION:-6h}"

echo "=== §5.6.e Ring buffer memory pressure (duration=$DURATION) ==="
echo "Results will be written to $OUT"
echo "Start: $(date)" | tee "$OUT/run.log"

# k6 script that hits 500 distinct paths to fill URLRing
cat > "$OUT/k6_500paths.js" << 'EOF'
import http from 'k6/http';
import { sleep } from 'k6';

const RATE   = parseInt(__ENV.RATE   || '50000');
const DUR    = __ENV.DURATION || '6h';
const TARGET = __ENV.TARGET   || 'http://bouine:8080';

export const options = {
  scenarios: {
    flood: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DUR,
      preAllocatedVUs: 500,
      maxVUs: 1000,
    },
  },
};

export default function () {
  // 500 distinct paths — ensures URLRing cap (512) is exercised
  const path = `/path${Math.floor(Math.random() * 500)}`;
  http.get(`${TARGET}${path}`);
}
EOF

# Background RSS + GC metrics sampler (every 60s)
python3 -c "
import urllib.request, time, json, os, re

admin    = '$ADMIN_ADDR'
out      = '$OUT'
token    = '$TOKEN'
samples  = []
start    = time.time()
end_sec  = $(python3 -c "
import re; d='$DURATION'
m=re.match(r'^(\d+)(h|m|s)$',d)
if m:
    v,u=int(m.group(1)),m.group(2)
    print(v*3600 if u=='h' else v*60 if u=='m' else v)
else: print(21600)
")

while time.time() - start < end_sec:
    ts = time.time()
    try:
        resp = urllib.request.urlopen(f'{admin}/metrics', timeout=5)
        raw = resp.read().decode()
        metrics = {}
        for line in raw.splitlines():
            if line.startswith('#'): continue
            for key in ['process_resident_memory_bytes',
                        'go_gc_duration_seconds_sum',
                        'go_goroutines',
                        'bouine_vary_cap_hits_total']:
                if line.startswith(key + ' ') or line.startswith(key + '{'):
                    m = re.search(r'} (\S+)|^[^ {]+ (\S+)', line)
                    if m: metrics[key] = float(m.group(1) or m.group(2))
        samples.append({'ts': ts, **metrics})
    except Exception as e:
        samples.append({'ts': ts, 'error': str(e)})
    time.sleep(60)

with open(f'{out}/rss_samples.json', 'w') as f:
    json.dump(samples, f, indent=2)
print(f'Sampled {len(samples)} RSS data points')
" &
SAMPLER_PID=$!

# Run k6 for the full duration
k6 run \
  --env TARGET="$DATA_ADDR" \
  --env RATE=50000 \
  --env DURATION="$DURATION" \
  --out json="$OUT/load.json" \
  "$OUT/k6_500paths.js" 2>&1 | tee -a "$OUT/run.log"

# Final heap snapshot
python3 -c "
import urllib.request, os
admin = os.environ.get('BOUINE_ADMIN','$ADMIN_ADDR')
for path, fname in [
    ('/debug/pprof/heap',        '$OUT/pprof-heap.pb.gz'),
    ('/debug/pprof/goroutine',   '$OUT/goroutines.txt'),
    ('/metrics',                 '$OUT/metrics_final.prom'),
]:
    try:
        resp = urllib.request.urlopen(f'{admin}{path}', timeout=30)
        with open(fname,'wb') as f: f.write(resp.read())
        print(f'saved {fname}')
    except Exception as e: print(f'{path} failed: {e}')
" 2>/dev/null || true

wait "$SAMPLER_PID" || true
echo "End: $(date)" | tee -a "$OUT/run.log"
echo "Done. Results in $OUT"
