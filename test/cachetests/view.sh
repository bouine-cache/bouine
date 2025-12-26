#!/usr/bin/env bash
# test/cachetests/view.sh — Run conformance tests then open the
# cache-tests comparison UI in a browser with bouine alongside
# Varnish, NGINX, and other proxies.
#
# Usage:
#   make conformance-view

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CACHETESTS_DIR="$SCRIPT_DIR/.cache-tests"
RESULTS_DIR="$SCRIPT_DIR/results"

# 1. Run conformance suite if results don't exist or --force.
if [ ! -f "$RESULTS_DIR/bouine.json" ] || [ "${1:-}" = "--force" ]; then
    echo ">>> Running conformance suite first..."
    bash "$SCRIPT_DIR/run.sh"
fi

# 2. Ensure cache-tests repo is cloned.
if [ ! -d "$CACHETESTS_DIR/.git" ]; then
    echo ">>> Cloning cache-tests..."
    git clone --depth=1 https://github.com/http-tests/cache-tests.git "$CACHETESTS_DIR"
    (cd "$CACHETESTS_DIR" && npm install --silent 2>&1 | tail -3)
fi

# 3. Copy bouine results into the cache-tests results directory.
cp "$RESULTS_DIR/bouine.json" "$CACHETESTS_DIR/results/bouine.json"
echo ">>> Copied bouine.json into cache-tests results."

# 4. Inject bouine entry into results/index.mjs if not already there.
if ! grep -q 'bouine.json' "$CACHETESTS_DIR/results/index.mjs"; then
    # Insert bouine entry at the beginning of the array, after "export default [".
    node -e "
      const fs = require('fs');
      const path = '$CACHETESTS_DIR/results/index.mjs';
      let content = fs.readFileSync(path, 'utf8');
      const entry = \`  {
    file: 'bouine.json',
    name: 'bouine',
    type: 'rev-proxy',
    version: '$(cd "$REPO_ROOT" && git describe --tags --always --dirty 2>/dev/null || echo dev)'
  },\n\`;
      content = content.replace('export default [\\n', 'export default [\\n' + entry);
      fs.writeFileSync(path, content);
    "
    echo ">>> Added bouine to results/index.mjs."
fi

# 5. Start a local HTTP server and open the browser.
PORT=8888
echo ""
echo ">>> Serving cache-tests UI at http://localhost:$PORT"
echo ">>> Press Ctrl+C to stop."
echo ""

# Open browser (macOS: open, Linux: xdg-open).
if command -v open >/dev/null 2>&1; then
    (sleep 1 && open "http://localhost:$PORT") &
elif command -v xdg-open >/dev/null 2>&1; then
    (sleep 1 && xdg-open "http://localhost:$PORT") &
fi

cd "$CACHETESTS_DIR"
npx --yes serve -l "$PORT" -s . 2>&1
