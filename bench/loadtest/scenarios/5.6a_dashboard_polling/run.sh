#!/usr/bin/env bash
# §5.6.a Dashboard idle vs active polling
# Measure data-plane p99 impact of 0/1/5 concurrent dashboard sessions
# on a 3-node cluster under 25k RPS (mixed workload).
set -euo pipefail
NS=bouine-test
OUT=/results/5.6a_dashboard_polling; mkdir -p "$OUT"
TOKEN="${ADMIN_TOKEN:-dev-secret}"
DURATION=90s

# Detect whether we're running in Docker Compose or Kubernetes
if kubectl get ns "$NS" 2>/dev/null | grep -q Active; then
  MODE=k8s
  kubectl -n "$NS" port-forward svc/bouine 18080:80 18090:9000 &
  PF_PID=$!; sleep 3
  DATA_ADDR=http://127.0.0.1:18080
  ADMIN_ADDR=http://127.0.0.1:18090
else
  MODE=compose
  DATA_ADDR="${BOUINE_ADDR:-http://bouine:8080}"
  ADMIN_ADDR="${BOUINE_ADMIN:-http://bouine:9000}"
fi

echo "=== §5.6.a Dashboard idle vs active polling ==="
echo "Mode=$MODE  data=$DATA_ADDR  admin=$ADMIN_ADDR"

# Helper: run k6 mixed workload for DURATION at 25k VUs
run_load() {
  local label="$1"
  k6 run \
    --env TARGET="$DATA_ADDR" \
    --env RATE=25000 \
    --env DURATION="$DURATION" \
    --out json="$OUT/load_${label}.json" \
    /scenarios/3.6_mixed_realistic/k6.js 2>&1 | tee "$OUT/k6_${label}.log"
}

# Helper: open N dashboard polling sessions (wget loop)
open_sessions() {
  local n="$1"
  for i in $(seq 1 "$n"); do
    (while true; do
       wget -q -O /dev/null --header="Cookie: $(get_cookie)" \
         "$ADMIN_ADDR/dashboard/" 2>/dev/null || true
       sleep 5
     done) &
    echo $! >> "$OUT/session_pids.txt"
  done
}

get_cookie() {
  # Login once and capture the session cookie for polling
  python3 -c "
import urllib.request, json, http.cookiejar
jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
req = urllib.request.Request('$ADMIN_ADDR/dashboard/login',
  data=json.dumps({'token':'$TOKEN'}).encode(),
  headers={'Content-Type':'application/json'}, method='POST')
try: opener.open(req)
except: pass
for c in jar: print(f'{c.name}={c.value}')
" 2>/dev/null || echo "bouine-session=dev"
}

kill_sessions() {
  if [ -f "$OUT/session_pids.txt" ]; then
    while read -r pid; do kill "$pid" 2>/dev/null || true; done < "$OUT/session_pids.txt"
    rm -f "$OUT/session_pids.txt"
  fi
}

# --- Condition A: no sessions ---
echo "--- Condition A: no dashboard sessions ---"
run_load "A_idle"
echo "condition=A sessions=0" > "$OUT/conditions.txt"

sleep 5

# --- Condition B: 1 session ---
echo "--- Condition B: 1 dashboard session ---"
open_sessions 1
run_load "B_1session"
kill_sessions
echo "condition=B sessions=1" >> "$OUT/conditions.txt"

sleep 5

# --- Condition C: 5 sessions ---
echo "--- Condition C: 5 dashboard sessions ---"
open_sessions 5
run_load "C_5sessions"
kill_sessions
echo "condition=C sessions=5" >> "$OUT/conditions.txt"

[ "${MODE}" = "k8s" ] && kill "$PF_PID" 2>/dev/null || true
echo "Done. Results in $OUT"
