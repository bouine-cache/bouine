#!/usr/bin/env bash
# bench/run.sh — Run the bouine benchmark suite.
#
# Usage:
#   bench/run.sh gate    # run only BenchmarkGate_* and enforce alloc budgets
#   bench/run.sh all     # run every benchmark in cache/storage/h1parser (no gates)
#
# Tests are skipped via `-run='^$'` in both modes; unit tests run in their
# own CI gate (`make test`, `prek run --all-files`).
#
# Benchmark naming convention:
#   BenchmarkGate_*    Hot-path, alloc-budgeted, time-driven. Enforced in gate mode.
#   BenchmarkSingle_*  Single-shot, skips under time-driven benchtime. Never gated.
#   Benchmark*          Regular time-driven benchmarks. Run in `all` mode only.
#
# The `gate` mode is the CI/PR gate: it runs all BenchmarkGate_* benchmarks
# with count=5 and fails if any allocs/op budget is breached. It also fails
# if a BenchmarkGate_* benchmark ran but has no budget defined (drift), or
# if a budget is defined but the benchmark didn't run (stale budget).
#
# The `all` mode is for deep analysis: it runs the full -bench=. set with
# count=3 and performs no gate checks. BenchmarkSingle_* self-skip under
# time-driven benchtime so -bench=. is safe.
#
# Both modes write results to bench/results/current.txt.
#
# Gate budgets (allocs/op, keyed by the suffix after BenchmarkGate_):
#   Evaluate_Hit:                     0
#   HotStore_Get_Hit:                 0
#   Handler_CacheHit_ReusableWriter:  0  (zero-alloc hit path achieved)
#   Handler_CacheMiss_Cacheable:      13  (batch2: raw-header precheck, no
#                                      unique-key interning, sharded singleflight;
#                                      main was at 24 after pkg/unique interning
#                                      added an entry-node alloc per miss on
#                                      Go 1.24+ — verified by alloc profile)
#   SIEVE_Access:                     0
#   Cachaner_Access:                   0
#   Cachaner_AccessSlowPath:           0
#   Cachaner_EvictBounded:             0
#   FastPath_Hit:                     0
#   FastPath_HitWithWrite:             0  (includes WriteTo consumption)
#   H1Parse_Get:                      0
#   Reactor_Hit:                      0  (epoll reactor batch serving;
#                                      parse+TryHit+serialize+flush)

set -euo pipefail

MODE="${1:-gate}"

RESULTS_DIR="bench/results"
mkdir -p "$RESULTS_DIR"
OUTFILE="$RESULTS_DIR/current.txt"

PACKAGES=(
    ./internal/cache/...
    ./internal/storage/...
    ./internal/server/h1parser/...
    ./internal/observability/...
)

# Gate benchmarks are selected by prefix — no explicit list to maintain.
GATE_BENCH='^BenchmarkGate_'

# Allocs/op budgets keyed by the suffix after BenchmarkGate_.
declare -A BUDGETS=(
    [Evaluate_Hit]=0
    [HotStore_Get_Hit]=0
    [Handler_CacheHit_ReusableWriter]=0
    [Handler_CacheMiss_Cacheable]=13
    [SIEVE_Access]=0
    [Cachaner_Access]=0
    [Cachaner_AccessSlowPath]=0
    [Cachaner_EvictBounded]=0
    [FastPath_Hit]=0
    [FastPath_HitWithWrite]=0
    [H1Parse_Get]=0
    [Reactor_Hit]=0
    [Middleware_Miss]=11
    [Middleware_Miss_NoLog]=0
)

run_bench() {
    local bench_pattern="$1"
    local count="$2"
    local timeout="$3"
    echo ">>> Running benchmarks (count=${count}, pattern=${bench_pattern}, timeout=${timeout})..."
    go test -run='^$' -bench="${bench_pattern}" -benchmem -count="${count}" -timeout="${timeout}" \
        "${PACKAGES[@]}" \
        | tee "$OUTFILE"
    echo ""
    echo ">>> Results saved to $OUTFILE"
    echo ""
}

# parse_allocs <bench_name> -> prints min allocs/op across runs
parse_allocs() {
    grep "$1" "$OUTFILE" | awk '{for(i=1;i<=NF;i++) if($(i+1)=="allocs/op") print $i}' | sort -n | head -1
}

# check_allocs <bench_name> <max_allocs>
check_allocs() {
    local bench="$1"
    local max_allocs="$2"
    local allocs
    allocs=$(parse_allocs "$bench")
    if [ -z "$allocs" ]; then
        echo "FAIL: $bench not found in results"
        return 1
    fi
    if [ "$allocs" -gt "$max_allocs" ]; then
        echo "FAIL: $bench has $allocs allocs/op (max: $max_allocs)"
        return 1
    fi
    echo "PASS: $bench = $allocs allocs/op (max: $max_allocs)"
    return 0
}

run_gates() {
    echo ">>> Gate checks..."
    local failed=0

    # Extract all BenchmarkGate_* names that actually ran.
    local found
    found=$(grep -oE 'BenchmarkGate_[A-Za-z0-9_]+' "$OUTFILE" | sort -u)

    # 1. Every benchmark that ran must have a budget.
    for bench in $found; do
        local suffix="${bench#BenchmarkGate_}"
        if [ -z "${BUDGETS[$suffix]+x}" ]; then
            echo "FAIL: $bench ran but has no alloc budget defined (add to BUDGETS)"
            failed=1
        fi
    done

    # 2. Every budget must have a matching benchmark that ran.
    for suffix in "${!BUDGETS[@]}"; do
        if ! echo "$found" | grep -q "BenchmarkGate_${suffix}"; then
            echo "FAIL: budget defined for BenchmarkGate_${suffix} but it didn't run"
            failed=1
        fi
    done

    # 3. Check allocs/op against budgets.
    for suffix in "${!BUDGETS[@]}"; do
        check_allocs "BenchmarkGate_${suffix}" "${BUDGETS[$suffix]}" || failed=1
    done

    if [ "$failed" -ne 0 ]; then
        echo ""
        echo ">>> BENCHMARK GATES BREACHED — see above."
        exit 1
    fi
    echo ""
    echo ">>> All gates passed."
}

run_diff() {
    if ! command -v benchstat >/dev/null 2>&1; then
        echo ">>> benchstat not found; installing..."
        go install golang.org/x/perf/cmd/benchstat@latest
    fi
    if [ ! -f "$RESULTS_DIR/baseline.txt" ]; then
        echo ">>> no baseline — copy $OUTFILE to $RESULTS_DIR/baseline.txt first"
        return 0
    fi
    echo ">>> benchstat diff vs baseline..."
    benchstat "$RESULTS_DIR/baseline.txt" "$OUTFILE"
}

case "$MODE" in
    gate)
        run_bench "$GATE_BENCH" 5 120s
        run_gates
        run_diff
        ;;
    all)
        run_bench "." 3 600s
        run_diff
        ;;
    *)
        echo "usage: $0 {gate|all}" >&2
        exit 2
        ;;
esac
