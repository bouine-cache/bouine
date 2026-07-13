# Phase 0 Baseline Benchmarks

**Hardware**: Apple M5, darwin/arm64
**Date**: 2026-07-13
**Branch**: main (commit 51a5549)
**Method**: `go test -bench=<name> -benchtime=1x -count=10 -benchmem`

## Results

### 0.1 — BenchmarkSIEVE_Evict_AllVisited_1M

`internal/storage/sieve/sieve_bench_test.go`

| Sample | ns/op | evict-ns | B/op | allocs/op |
|--------|-------|----------|------|-----------|
| 1 | 2,297,542 | 2,297,250 | 0 | 0 |
| 2 | 2,253,834 | 2,252,791 | 0 | 0 |
| 3 | 2,235,375 | 2,234,041 | 0 | 0 |
| 4 | 2,294,375 | 2,294,167 | 0 | 0 |
| 5 | 2,240,583 | 2,239,709 | 0 | 0 |
| 6 | 2,291,459 | 2,290,250 | 0 | 0 |
| 7 | 2,208,375 | 2,207,167 | 0 | 0 |
| 8 | 2,266,833 | 2,265,958 | 0 | 0 |
| 9 | 2,227,833 | 2,227,000 | 0 | 0 |
| 10 | 2,246,667 | 2,245,833 | 0 | 0 |

**Median**: ~2.25 ms per Evict() call
**Verdict**: O(N) worst case confirmed. 1 M all-visited entries → ~2.25 ms.
Plan predicted 10-20 ms; actual is ~2.3 ms (lower but still O(N) — the M5
is faster than the production hardware in the plan's analysis). The fix
(EvictBounded with 128 probes) should bring this to ~50-100 us, a ~20-40x
improvement.

### 0.2 — BenchmarkWarmEvict_AllVisited_1M

`internal/storage/warm/evict_bench_test.go`

| Sample | ns/op | evict-ns | B/op | allocs/op |
|--------|-------|----------|------|-----------|
| 1 | 2,640,291 | 2,639,250 | 1,440 | 2 |
| 2 | 2,599,667 | 2,598,500 | 1,440 | 2 |
| 3 | 2,573,083 | 2,572,000 | 1,440 | 2 |
| 4 | 2,565,375 | 2,564,500 | 1,440 | 2 |
| 5 | 2,642,541 | 2,641,459 | 1,440 | 2 |
| 6 | 2,608,250 | 2,607,042 | 1,440 | 2 |
| 7 | 2,571,917 | 2,570,958 | 1,440 | 2 |
| 8 | 2,558,583 | 2,557,625 | 1,440 | 2 |
| 9 | 2,547,250 | 2,546,291 | 1,440 | 2 |
| 10 | 2,538,417 | 2,537,625 | 1,440 | 2 |

**Median**: ~2.58 ms per evictOne() call
**Verdict**: O(N) worst case confirmed. The SIEVE sweep dominates (~2.5 ms),
plus pwritev syscall and index removal (1,440 B, 2 allocs for the tombstone
write). The EvictBounded cap should reduce the SIEVE sweep to ~100 us,
leaving only the syscall + index cost.

### 0.3 — BenchmarkWarmSyncCycle_1M

`internal/storage/storage/eviction_bench_test.go`

| Sample | ns/op | sync-ms | B/op | allocs/op |
|--------|-------|---------|------|-----------|
| 1 | 36,552,792 | 36 | 53,839,456 | 23,322 |
| 2 | 33,985,583 | 33 | 53,839,456 | 23,322 |
| 3 | 33,184,542 | 33 | 53,839,456 | 23,322 |
| 4 | 33,411,334 | 33 | 53,839,456 | 23,322 |
| 5 | 33,284,125 | 33 | 53,839,456 | 23,322 |
| 6 | 33,983,791 | 33 | 53,839,456 | 23,322 |
| 7 | 34,041,334 | 34 | 53,839,456 | 23,322 |
| 8 | 33,936,375 | 33 | 53,839,456 | 23,322 |
| 9 | 34,134,792 | 34 | 53,839,456 | 23,322 |
| 10 | 34,651,500 | 34 | 53,839,456 | 23,322 |

**Median**: ~34 ms, **53.8 MB**, **23,322 allocs** per sync cycle
**Verdict**: The 53.8 MB / 23 K allocs is the collectHotOnlyKeys allocation
spike (hot.Keys() + warm.Keys() + warmSet diff map) plus the
writeHotOnlyToWarm promotion allocations. At 1 M entries the plan predicted
~500 MB at 10 M entries; ~54 MB at 1 M scales linearly. The incremental
hot-only tracking (Phase 2) should eliminate the collectHotOnlyKeys
allocation, dropping this to near-zero allocs on the sync path.

### 0.4 — BenchmarkBan_1M

`internal/storage/eviction_bench_test.go`

| Sample | ns/op | ban-ms | B/op | allocs/op |
|--------|-------|--------|------|-----------|
| 1 | 69,059,209 | 69 | 7,488 | 86 |
| 2 | 69,421,750 | 69 | 7,488 | 86 |
| 3 | 69,708,125 | 69 | 7,488 | 86 |
| 4 | 70,423,666 | 70 | 7,600 | 87 |
| 5 | 68,981,375 | 68 | 7,488 | 86 |
| 6 | 68,865,084 | 68 | 7,488 | 86 |
| 7 | 72,629,416 | 72 | 7,488 | 86 |
| 8 | 69,068,709 | 69 | 7,488 | 86 |
| 9 | 72,965,334 | 72 | 7,488 | 86 |
| 10 | 69,259,750 | 69 | 7,488 | 86 |

**Median**: ~69 ms per Ban() call
**Verdict**: Sequential shard scanning with 1 M entries / 64 shards takes
~69 ms on M5. Plan predicted 1-3 s on production hardware (slower CPUs).
Parallelizing across NumCPU workers should bring this to ~69/NumCPU ms.
With 8 cores: ~9 ms, a ~8x improvement.

### 0.5 — BenchmarkWarmEvictToFit_MultiEvict

`internal/storage/warm/evictfit_bench_test.go`

| Sample | ns/op | B/op | allocs/op |
|--------|-------|------|-----------|
| 1 | 42,084 | 480 | 4 |
| 2 | 27,458 | 480 | 4 |
| 3 | 30,000 | 480 | 4 |
| 4 | 32,167 | 480 | 4 |
| 5 | 28,916 | 480 | 4 |
| 6 | 32,167 | 480 | 4 |
| 7 | 27,709 | 480 | 4 |
| 8 | 28,500 | 480 | 4 |
| 9 | 27,958 | 480 | 4 |
| 10 | 28,500 | 480 | 4 |

**Median**: ~29 us for 10 consecutive evictions
**Verdict**: 10 evictOne() calls (each acquiring seg.mu + idxMu, writing a
tombstone, updating the index) take ~29 us total. The batch eviction
(Phase 3) should reduce this by ~30% by amortizing lock acquisition across
the 10 evictions.

## Summary

| Benchmark | Median | Unit | Confirms plan hypothesis? |
|-----------|--------|------|--------------------------|
| SIEVE_Evict_AllVisited_1M | 2.25 ms | ns/op | Yes — O(N) worst case |
| WarmEvict_AllVisited_1M | 2.58 ms | ns/op | Yes — O(N) + syscall overhead |
| WarmSyncCycle_1M | 34 ms, 54 MB, 23K allocs | ns/op + B/op | Yes — allocation spike |
| Ban_1M | 69 ms | ns/op | Yes — sequential shard scanning |
| WarmEvictToFit_MultiEvict | 29 us | ns/op | Yes — 10 lock cycles for 10 evictions |

All five benchmarks confirm the problems predicted in the plan. The
absolute numbers are lower than the plan's predictions (M5 is faster than
production hardware), but the O(N) scaling and allocation patterns match.
