#!/usr/bin/env bash
# soak.sh — Long-running soak test against a live bouine cluster.
#
# Usage:
#   ./test/chaos/soak.sh [--duration HOURS] [--rps RPS] [--nodes NODE,NODE,...]
#
# Defaults:
#   --duration 24          24-hour soak (ROADMAP.md @ 50% capacity)
#   --rps 5000             Request rate in req/s (canonical bench = 10 krps)
#   --nodes 127.0.0.1:18081,127.0.0.1:18082,127.0.0.1:18083
#
# Produces:
#   soak-results/TIMESTAMP/summary.txt   Human-readable result
#   soak-results/TIMESTAMP/metrics.json  Prometheus snapshot at end
#   soak-results/TIMESTAMP/latency.hdr   HDR histogram (if hey is available)
#
# Prerequisites:
#   - 3-node bouine cluster reachable at the given addresses (use make integration
#     to bring one up first)
#   - hey (https://github.com/rakyll/hey) OR ab (Apache Bench) for load gen
#   - curl, jq
set -euo pipefail

DURATION_H=${DURATION_H:-24}
RPS=${RPS:-5000}
NODES=${NODES:-"127.0.0.1:18081,127.0.0.1:18082,127.0.0.1:18083"}
ADMIN_PORT=${ADMIN_PORT:-"19001"}
OUTPUT_DIR="soak-results/$(date +%Y%m%dT%H%M%S)"

mkdir -p "$OUTPUT_DIR"
echo "Soak configuration:" | tee "$OUTPUT_DIR/summary.txt"
echo "  Duration:  ${DURATION_H}h" | tee -a "$OUTPUT_DIR/summary.txt"
echo "  RPS:       ${RPS}" | tee -a "$OUTPUT_DIR/summary.txt"
echo "  Nodes:     ${NODES}" | tee -a "$OUTPUT_DIR/summary.txt"
echo "  Start:     $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUTPUT_DIR/summary.txt"
echo "" | tee -a "$OUTPUT_DIR/summary.txt"

DURATION_S=$(( DURATION_H * 3600 ))

# Pick the first node as the primary target.
PRIMARY_NODE=$(echo "$NODES" | cut -d, -f1)
PRIMARY_ADMIN="http://127.0.0.1:${ADMIN_PORT}"

# ── Pre-flight: confirm the cluster is healthy ────────────────────────────────
echo "==> Pre-flight health check..."
for NODE in $(echo "$NODES" | tr ',' '\n'); do
    STATUS=$(curl -sf "http://${NODE}/healthz" -o /dev/null -w "%{http_code}" || echo "000")
    if [[ "$STATUS" != "200" ]]; then
        echo "FAIL: node http://${NODE}/healthz returned $STATUS"
        exit 1
    fi
    echo "  http://${NODE}/healthz OK"
done
echo ""

# ── Load generator ────────────────────────────────────────────────────────────
run_hey() {
    local target="$1"
    local duration="$2"
    local rps="$3"
    local out="$4"

    if command -v hey &>/dev/null; then
        hey -z "${duration}s" -q "$rps" \
            -o csv \
            "http://${target}/soak/1" \
        > "$out" 2>&1
    elif command -v ab &>/dev/null; then
        # ab doesn't support time-based rate limiting well; approximate.
        local total=$(( rps * duration ))
        ab -n "$total" -c 50 -g "$out" \
            "http://${target}/soak/1" \
        >> "$OUTPUT_DIR/summary.txt" 2>&1
    else
        echo "WARNING: neither 'hey' nor 'ab' found — skipping load generation"
        echo "         Install hey: go install github.com/rakyll/hey@latest"
    fi
}

echo "==> Starting ${DURATION_H}h soak at ${RPS} rps against http://${PRIMARY_NODE}/..."
run_hey "$PRIMARY_NODE" "$DURATION_S" "$RPS" "$OUTPUT_DIR/latency.csv" &
LOAD_PID=$!

# ── Periodic metric snapshots every 60 s ─────────────────────────────────────
SNAPSHOT_DIR="$OUTPUT_DIR/snapshots"
mkdir -p "$SNAPSHOT_DIR"

snapshot_metrics() {
    local ts
    ts=$(date +%s)
    curl -sf "${PRIMARY_ADMIN}/metrics" 2>/dev/null \
        > "${SNAPSHOT_DIR}/${ts}.prom" || true
}

echo "==> Snapshotting metrics every 60s..."
SNAP_COUNT=0
while kill -0 "$LOAD_PID" 2>/dev/null; do
    sleep 60
    snapshot_metrics
    SNAP_COUNT=$(( SNAP_COUNT + 1 ))
    echo "  snap ${SNAP_COUNT} ($(date -u +%H:%M:%SZ))"
done
wait "$LOAD_PID" || true

# ── Final metrics snapshot ────────────────────────────────────────────────────
echo "" | tee -a "$OUTPUT_DIR/summary.txt"
echo "==> Final metrics snapshot..."
snapshot_metrics
if command -v jq &>/dev/null; then
    curl -sf "${PRIMARY_ADMIN}/metrics" 2>/dev/null \
    | grep -E "^bouine_(cache_result|cluster_|broadcast)" \
    | jq -Rn '[inputs | capture("(?P<name>[^ ]+) (?P<value>[0-9.e+]+)")]' \
    > "$OUTPUT_DIR/metrics.json" || true
fi

# ── Parse hey CSV for latency percentiles ────────────────────────────────────
if [[ -f "$OUTPUT_DIR/latency.csv" ]] && command -v awk &>/dev/null; then
    echo "" | tee -a "$OUTPUT_DIR/summary.txt"
    echo "==> Latency summary (ms):" | tee -a "$OUTPUT_DIR/summary.txt"
    awk -F',' 'NR>1 {sum+=$8; n++; if($7>=500) err++}
        END {
            printf "  mean=%.2f err=%d/%d (%.2f%%)\n", sum/n*1000, err, n, err/n*100
        }' "$OUTPUT_DIR/latency.csv" | tee -a "$OUTPUT_DIR/summary.txt"
fi

echo "" | tee -a "$OUTPUT_DIR/summary.txt"
echo "End:  $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$OUTPUT_DIR/summary.txt"
echo "Output: $OUTPUT_DIR"
echo ""
echo "Attach $OUTPUT_DIR/ to the release tag (ROADMAP.md exit criterion)."
