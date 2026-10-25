#!/usr/bin/env bash
# test/cachetests/run-cluster.sh — Run the http-tests/cache-tests
# conformance suite against a 3-node STRONG-MODE bouine cluster.
#
# The single-node harness (run.sh) cannot see cluster-only defects:
# Vary variant routing across owners, the metadata-only resolver's
# peer answer class, peer-fetch gates, and cluster invalidation. This
# script boots three bouine nodes in strong mode sharing the
# cache-tests origin and runs the full suite against EACH node in
# sequence — every node must pass, so the score is the minimum across
# the fleet.
#
# The setup section duplicates run.sh on purpose: run.sh is the
# nightly CI gate and stays untouched; divergence risk here is a
# manually-invoked diagnostic, not a pipeline.
#
# Prerequisites: Node.js 18+, npm, Go 1.27+
# Usage: make conformance-cluster

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CACHETESTS_DIR="$SCRIPT_DIR/.cache-tests"
RESULTS_DIR="$SCRIPT_DIR/results"
ORIGIN_PORT=8000
# Cluster ports live outside the single-node script's 8080/9099 range
# so both jobs can run side by side.
HTTP_PORTS=(8091 8092 8093)
ADMIN_PORTS=(9199 9198 9197)
GOSSIP_PORTS=(17147 17148 17149)
BOUINE_PIDS=()
ORIGIN_PID=""

cleanup() {
    echo ">>> Cleaning up..."
    for pid in "${BOUINE_PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    [ -n "$ORIGIN_PID" ] && kill "$ORIGIN_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

# 1. Build bouine.
echo ">>> Building bouine..."
(cd "$REPO_ROOT" && make build)
BOUINE_BIN="$REPO_ROOT/bin/bouine"

# 2. Clone or update cache-tests (same policy as run.sh).
if [ ! -d "$CACHETESTS_DIR/.git" ]; then
    echo ">>> Cloning http-tests/cache-tests..."
    rm -rf "$CACHETESTS_DIR"
    if command -v gh >/dev/null 2>&1; then
        gh repo clone http-tests/cache-tests "$CACHETESTS_DIR" -- --depth=1 || rm -rf "$CACHETESTS_DIR"
    fi
    if [ ! -d "$CACHETESTS_DIR/.git" ]; then
        git clone --depth=1 https://github.com/http-tests/cache-tests.git "$CACHETESTS_DIR"
    fi
else
    echo ">>> Updating cache-tests..."
    (cd "$CACHETESTS_DIR" && git pull --ff-only 2>/dev/null || true)
fi

echo ">>> Installing cache-tests dependencies..."
(cd "$CACHETESTS_DIR" && npm install --silent --no-package-lock 2>&1 | tail -3)

# 3. Start the cache-tests origin (shared by all three nodes).
echo ">>> Starting cache-tests origin on port $ORIGIN_PORT..."
PIDFILE=$(mktemp)
(cd "$CACHETESTS_DIR" && npm_config_port=$ORIGIN_PORT npm_package_config_port=$ORIGIN_PORT \
    npm_package_config_protocol=http npm_config_protocol=http \
    npm_package_config_pidfile="$PIDFILE" npm_config_pidfile="$PIDFILE" \
    node test-engine/server/server.mjs > /dev/null 2>&1) &
ORIGIN_PID=$!
sleep 3
if ! node -e "fetch('http://127.0.0.1:$ORIGIN_PORT/').then(r=>{process.exit(r.status<500?0:1)}).catch(()=>process.exit(1))" 2>/dev/null; then
    echo "FAIL: cache-tests origin did not start"
    exit 1
fi
echo ">>> Origin is up."

# 4. Boot the 3-node strong-mode cluster.
SEEDS="["
for i in 0 1 2; do
    [ $i -gt 0 ] && SEEDS+=", "
    SEEDS+="\"127.0.0.1:${GOSSIP_PORTS[$i]}\""
done
SEEDS+="]"

for i in 0 1 2; do
    CFG=$(mktemp)
    cat > "$CFG" <<YAML
listen:
  http: "127.0.0.1:${HTTP_PORTS[$i]}"
  admin: "127.0.0.1:${ADMIN_PORTS[$i]}"
storage:
  hot_max_bytes: 256MiB
cluster:
  node_name: bouine-cluster-$i
  mode: strong
  join: $SEEDS
  hop_limit: 2
upstream_pools:
  - name: cache-tests
    targets: ["127.0.0.1:$ORIGIN_PORT"]
routes:
  - match: {}
    pool: cache-tests
YAML
    echo ">>> Starting node $i on port ${HTTP_PORTS[$i]}..."
    "$BOUINE_BIN" serve --config "$CFG" --log-level error --log-format text &
    BOUINE_PIDS+=($!)
done

for i in 0 1 2; do
    for _ in $(seq 1 30); do
        if node -e "fetch('http://127.0.0.1:${ADMIN_PORTS[$i]}/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))" 2>/dev/null; then
            break
        fi
        sleep 1
    done
done
echo ">>> Cluster is up (3 nodes, strong mode)."

# 5. Run the full suite against EACH node in sequence: every node
# serves the whole workload through the cluster (owner routing, peer
# fetches, resolver two-steps), so a cluster-only defect has to
# survive three full passes to hide.
mkdir -p "$RESULTS_DIR"
declare -a PASS_COUNTS=()
for i in 0 1 2; do
    RESULTS_FILE="$RESULTS_DIR/bouine-cluster-node$i.json"
    echo ""
    echo ">>> Running cache-tests against node $i (http://127.0.0.1:${HTTP_PORTS[$i]}/) ..."
    (cd "$CACHETESTS_DIR" && npm_config_base="http://127.0.0.1:${HTTP_PORTS[$i]}" \
        npm_package_config_base="http://127.0.0.1:${HTTP_PORTS[$i]}" \
        npm_config_id="" npm_package_config_id="" \
        node --no-warnings test-engine/cli.mjs) \
        > "$RESULTS_FILE" 2>"$RESULTS_DIR/bouine-cluster-node$i.log" || true

    PASS_COUNTS[$i]=$(node -e "
      const fs = require('fs');
      const raw = fs.readFileSync('$RESULTS_FILE', 'utf8').trim();
      try {
        const r = JSON.parse(raw);
        const entries = Object.values(r);
        let pass = 0;
        for (const arr of entries) {
          if (!Array.isArray(arr) || arr.length === 0 || arr[0] === 'Setup') { pass++; continue; }
        }
        console.log(pass + '/' + entries.length);
      } catch(e) {
        console.log('parse-error/0');
      }
    " 2>/dev/null)
    echo ">>> Node $i score: ${PASS_COUNTS[$i]}"
done

# 6. Summary: the cluster's score is its WORST node.
echo ""
echo "=== Cluster Cache-Tests Conformance Summary ==="
for i in 0 1 2; do
    echo "  node $i: ${PASS_COUNTS[$i]}"
done
WORST=$(node -e "
  const counts = process.argv.slice(1).map(s => parseInt(s.split('/')[0], 10));
  const totals = process.argv.slice(1).map(s => parseInt(s.split('/')[1], 10));
  const worstPass = Math.min(...counts);
  const total = totals[0];
  console.log(worstPass + '/' + total + ' (' + (100*worstPass/total).toFixed(1) + '%)');
" "${PASS_COUNTS[@]}" 2>/dev/null)
echo "  cluster (min): $WORST"
echo "==============================================="

echo ""
echo ">>> Cluster conformance run complete."
