#!/usr/bin/env bash
# Runs the topology harness matrix for one code state and writes results
# under bench/results/topology-<label>/. Usage:
#   bench/topology/run-baseline.sh <label> [binary]
# The label is typically "before" or "after"; the binary defaults to
# bin/topology built from the current tree.
set -euo pipefail

LABEL="${1:?usage: run-baseline.sh <label> [binary]}"
BIN="${2:-./bin/topology}"
OUT="bench/results/topology-${LABEL}"
mkdir -p "$OUT"

git rev-parse HEAD > "$OUT/git.txt"

run() {
  name="$1"; shift
  echo ">>> ${name}"
  "$BIN" "$@" 2>&1 | tee "$OUT/${name}.txt"
}

# Tier L1 hit-heavy across load levels, tracing off: F1's effect (peer-hit
# copy) plus F12's (none here — all keys storable).
for rate in 250 500 1000 2000; do
  run "hit-heavy-${rate}-off" -profile hit-heavy -rate "${rate}" -warmup 10s -duration 15s
done

# Tracing variants at the canonical rate: F4's effect on the slow-path
# share (misses ~5-12%) at unsampled-heavy (0.01) and always-sampled (1.0).
run "hit-heavy-1000-tracing001" -profile hit-heavy -rate 1000 -warmup 10s -duration 15s -tracing 0.01
run "hit-heavy-1000-tracing10" -profile hit-heavy -rate 1000 -warmup 10s -duration 15s -tracing 1.0

# Single remote-owned unstorable key: F12's effect (wasted peer RPC on the
# two non-owners) and its per-node latency signature.
run "herd-unstorable-500" -profile herd-unstorable -rate 500 -warmup 5s -duration 15s
