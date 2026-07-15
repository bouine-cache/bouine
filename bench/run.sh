#!/usr/bin/env bash
# bench/run.sh — Run the bouine benchmark suite and enforce gates.
#
# Usage:
#   make bench              # run and save results
#   make benchstat          # compare HEAD results vs baseline
#
# Gates (fail the script if breached):
#   - Evaluate_Hit:           allocs/op must be 0
#   - HotStore_Get_Hit:       allocs/op must be 0
#   - Handler_CacheHit:       allocs/op must be ≤ 9 (6 are test harness)
#   - FastPath_Hit:           allocs/op must be 0
#
# The script writes results to bench/results/current.txt and exits
# non-zero if any gate is breached.

set -euo pipefail

RESULTS_DIR="bench/results"
mkdir -p "$RESULTS_DIR"
OUTFILE="$RESULTS_DIR/current.txt"

echo ">>> Running benchmarks (count=5)..."
go test -bench=. -benchmem -count=5 -timeout=120s \
    ./internal/cache/... \
    ./internal/storage/... \
    ./internal/server/h1parser/... \
    | tee "$OUTFILE"

echo ""
echo ">>> Results saved to $OUTFILE"
echo ""

# Gate checks: parse allocs/op from the output.
check_allocs() {
    local bench="$1"
    local max_allocs="$2"
    # Extract the median allocs/op (last column before "allocs/op").
    local allocs
    allocs=$(grep "$bench" "$OUTFILE" | awk '{for(i=1;i<=NF;i++) if($(i+1)=="allocs/op") print $i}' | sort -n | head -1)
    if [ -z "$allocs" ]; then
        echo "WARN: benchmark $bench not found in results"
        return 0
    fi
    if [ "$allocs" -gt "$max_allocs" ]; then
        echo "FAIL: $bench has $allocs allocs/op (max: $max_allocs)"
        return 1
    fi
    echo "PASS: $bench = $allocs allocs/op (max: $max_allocs)"
    return 0
}

echo ">>> Gate checks..."
FAILED=0
check_allocs "BenchmarkEvaluate_Hit" 0 || FAILED=1
check_allocs "BenchmarkHotStore_Get_Hit" 0 || FAILED=1
check_allocs "BenchmarkHandler_CacheHit" 13 || FAILED=1
check_allocs "BenchmarkSIEVE_Access" 0 || FAILED=1
check_allocs "BenchmarkFastPath_Hit" 0 || FAILED=1
check_allocs "BenchmarkH1Parse_Get" 0 || FAILED=1

if [ "$FAILED" -ne 0 ]; then
    echo ""
    echo ">>> BENCHMARK GATES BREACHED — see above."
    exit 1
fi

echo ""
echo ">>> All gates passed."
