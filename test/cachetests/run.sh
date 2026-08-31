#!/usr/bin/env bash
# test/cachetests/run.sh — Run the http-tests/cache-tests conformance
# suite against a local bouine instance.
#
# Prerequisites: Node.js 18+, npm, Go 1.27+
# Usage: make conformance

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CACHETESTS_DIR="$SCRIPT_DIR/.cache-tests"
RESULTS_DIR="$SCRIPT_DIR/results"
ORIGIN_PORT=8000
BOUINE_HTTP_PORT=8080
BOUINE_ADMIN_PORT=9099
BOUINE_PID=""
ORIGIN_PID=""

cleanup() {
    echo ">>> Cleaning up..."
    [ -n "$BOUINE_PID" ] && kill "$BOUINE_PID" 2>/dev/null || true
    [ -n "$ORIGIN_PID" ] && kill "$ORIGIN_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

# 1. Build bouine.
echo ">>> Building bouine..."
(cd "$REPO_ROOT" && make build)
BOUINE_BIN="$REPO_ROOT/bin/bouine"

# 2. Clone or update cache-tests.
if [ -d "$CACHETESTS_DIR/.git" ]; then
    echo ">>> Updating cache-tests..."
    (cd "$CACHETESTS_DIR" && git pull --ff-only 2>/dev/null || true)
else
    echo ">>> Cloning http-tests/cache-tests..."
    rm -rf "$CACHETESTS_DIR"
    git clone --depth=1 https://github.com/http-tests/cache-tests.git "$CACHETESTS_DIR"
fi

echo ">>> Installing cache-tests dependencies..."
(cd "$CACHETESTS_DIR" && npm install --silent --no-package-lock 2>&1 | tail -3)

# 3. Start the cache-tests origin server via npm (sets env vars).
echo ">>> Starting cache-tests origin on port $ORIGIN_PORT..."
PIDFILE=$(mktemp)
(cd "$CACHETESTS_DIR" && npm_config_port=$ORIGIN_PORT npm_package_config_port=$ORIGIN_PORT \
    npm_package_config_protocol=http npm_config_protocol=http \
    npm_package_config_pidfile="$PIDFILE" npm_config_pidfile="$PIDFILE" \
    node test-engine/server/server.mjs > /dev/null 2>&1) &
ORIGIN_PID=$!
sleep 3

# Verify origin is up.
echo ">>> Checking origin health..."
ORIGIN_OK=0
for i in 1 2 3 4 5; do
    if node -e "fetch('http://127.0.0.1:$ORIGIN_PORT/').then(r=>{process.exit(r.status<500?0:1)}).catch(()=>process.exit(1))" 2>/dev/null; then
        ORIGIN_OK=1
        break
    fi
    sleep 1
done
if [ "$ORIGIN_OK" -ne 1 ]; then
    echo "FAIL: cache-tests origin did not start on port $ORIGIN_PORT"
    exit 1
fi
echo ">>> Origin is up."

# 4. Write bouine config and start bouine.
BOUINE_CONFIG=$(mktemp)
EXPERIMENTAL_YAML="${BOUINE_FAST_PATH:-}"
REACTOR_YAML=""
if [ "${BOUINE_H1_REACTOR:-}" = "true" ]; then
    REACTOR_YAML="  h1_reactor: true"
fi
if [ "$EXPERIMENTAL_YAML" = "true" ]; then
    cat > "$BOUINE_CONFIG" <<YAML
listen:
  http: "127.0.0.1:$BOUINE_HTTP_PORT"
  admin: "127.0.0.1:$BOUINE_ADMIN_PORT"
storage:
  hot_max_bytes: 256MiB
upstream_pools:
  - name: cache-tests
    targets: ["127.0.0.1:$ORIGIN_PORT"]
routes:
  - match: {}
    pool: cache-tests
experimental:
  h1_fast_path: true
${REACTOR_YAML}
YAML
    echo ">>> H1 fast path ENABLED for conformance run${REACTOR_YAML:+ (+ h1_reactor)}"
else
    cat > "$BOUINE_CONFIG" <<YAML
listen:
  http: "127.0.0.1:$BOUINE_HTTP_PORT"
  admin: "127.0.0.1:$BOUINE_ADMIN_PORT"
storage:
  hot_max_bytes: 256MiB
upstream_pools:
  - name: cache-tests
    targets: ["127.0.0.1:$ORIGIN_PORT"]
routes:
  - match: {}
    pool: cache-tests
YAML
fi

echo ">>> Starting bouine on port $BOUINE_HTTP_PORT..."
"$BOUINE_BIN" serve --config "$BOUINE_CONFIG" --log-level error --log-format text &
BOUINE_PID=$!
sleep 2

# Verify bouine is up.
echo ">>> Checking bouine health..."
if ! node -e "fetch('http://127.0.0.1:$BOUINE_ADMIN_PORT/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))" 2>/dev/null; then
    echo "FAIL: bouine did not start"
    exit 1
fi
echo ">>> bouine is up."

# 5. Run cache-tests CLI.
mkdir -p "$RESULTS_DIR"
RESULTS_FILE="$RESULTS_DIR/bouine.json"

echo ""
echo ">>> Running cache-tests against http://127.0.0.1:$BOUINE_HTTP_PORT/ ..."
echo ""

(cd "$CACHETESTS_DIR" && npm_config_base="http://127.0.0.1:$BOUINE_HTTP_PORT" \
    npm_package_config_base="http://127.0.0.1:$BOUINE_HTTP_PORT" \
    npm_config_id="" npm_package_config_id="" \
    node --no-warnings test-engine/cli.mjs) \
    > "$RESULTS_FILE" 2>"$RESULTS_DIR/bouine.log" || true

echo ""
echo ">>> Results saved to $RESULTS_FILE"

# 6. Summary.
if [ ! -s "$RESULTS_FILE" ]; then
    echo "WARN: results file is empty."
    exit 1
fi

node -e "
  const fs = require('fs');
  const raw = fs.readFileSync('$RESULTS_FILE', 'utf8').trim();
  try {
    const r = JSON.parse(raw);
    const entries = Object.entries(r);
    let pass = 0, fail = 0, setup = 0;
    for (const [id, arr] of entries) {
      if (!Array.isArray(arr) || arr.length === 0) { pass++; continue; }
      if (arr[0] === 'Setup') { setup++; continue; }
      fail++;
    }
    console.log('');
    console.log('=== Cache-Tests Conformance Summary ===');
    console.log('  Total:       ' + entries.length);
    console.log('  Pass:        ' + pass + ' (' + (100*pass/entries.length).toFixed(1) + '%)');
    console.log('  Fail:        ' + fail);
    console.log('  Setup error: ' + setup);
    console.log('========================================');
  } catch(e) {
    console.log('Could not parse results: ' + e.message);
    console.log('First 200 chars: ' + raw.substring(0, 200));
  }
" 2>/dev/null || echo "Could not parse results JSON"

echo ""
echo ">>> Conformance run complete."
