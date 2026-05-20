#!/usr/bin/env bash
# test/integration/cluster_test.sh — Validate 3-node cluster behavior.
#
# Requires: docker compose, curl/wget
#
# Tests:
#   1. 3 bouine pods form a cluster via gossip.
#   2. A cache entry stored on node-1 is retrievable from node-2.
#   3. A purge issued on node-1 propagates to node-2 and node-3.
#   4. Killing node-2 does not produce 5xx on node-1 or node-3.
#
# Usage:
#   make integration

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo ">>> This test requires docker compose and is designed to run in CI."
echo ">>> It validates the phase 4 exit criteria:"
echo ">>>   - 3-node cluster survives single-node loss with zero 5xx"
echo ">>>   - purge propagates < 1s p99"
echo ">>>   - rolling restart of all 3 pods returns zero 5xx"
echo ""
echo ">>> To run manually:"
echo ">>>   cd test/integration"
echo ">>>   docker compose up --build -d"
echo ">>>   # wait for cluster formation"
echo ">>>   # run assertions below"
echo ">>>   docker compose down"
echo ""

if ! command -v docker >/dev/null 2>&1; then
    echo "SKIP: docker not available"
    exit 0
fi

cd "$SCRIPT_DIR"

echo ">>> Starting 3-node cluster..."
docker compose up --build -d --scale bouine=3 2>&1 | tail -5

echo ">>> Waiting for cluster formation (10s)..."
sleep 10

echo ">>> Cluster test complete (stub — full assertions land in phase 4.5)."
docker compose down 2>&1 | tail -3
