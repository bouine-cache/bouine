# ADR-0026: SIEVE sweep cap

- **Status**: Accepted
- **Date**: 2026-07-13
- **Deciders**: @theotime.leveque
- **Phase**: 1

## Context

Under heavy read load with 1 M+ entries, SIEVE's `Evict()` degrades to
O(N) worst case. The scan is bounded at `maxProbes = l.len * 2`: when all
entries have `visited=true` (the steady state under read-heavy traffic),
the hand sweeps the entire list clearing visited bits, then sweeps again
to find the first unvisited entry. At 1 M warm entries under `idxMu.Lock`,
this blocks all warm Gets for ~2.5 ms per call (measured on Apple M5;
expected 10-20 ms on production hardware).

The `evictPreferBacked` function in the hot tier calls `Evict()` up to 5
times (4 skip iterations + 1 fallback), making the hot tier worst case
O(10N).

## Decision

We cap the SIEVE sweep at a fixed `maxSweepProbes` constant per tier:
- **Hot tier**: `maxSweepProbes = 128` (in `internal/storage/hot.go`)
- **Warm tier**: `maxSweepProbes = 256` (in `internal/storage/warm/warm.go`)

We add `EvictBounded(maxProbes int) (K, bool)` to `sieve.List`. `Evict()`
is refactored to call `EvictBounded(l.len * 2)` for backward compatibility.

When the hand probes more than `maxSweepProbes` entries without finding
an unvisited one, `EvictBounded` returns `(zero, false)` instead of
continuing the full sweep.

The caller handles the false return:
- **Hot tier**: `evictPreferBacked()` returns `(zero, false)`. The inline
  eviction loop and sweeper break on `!ok`. The shard stays over budget
  temporarily; the next `Put` re-signals the sweeper, which retries.
- **Warm tier**: `evictToFit` returns `ErrOverBudget`. `Put` rejects the
  write. The warm sync loop skips promotion. No data loss.

The warm tier gets a 2x larger cap because warm entries are disk-backed
and larger (each eviction frees more bytes, so fewer total evictions are
needed). The higher probe budget gives SIEVE more room to find
unprotected entries before falling back to `ErrOverBudget`, which has a
higher cost (origin fetch) than a hot-tier miss.

## Consequences

### Positive

- O(N) worst case becomes O(maxSweepProbes) = O(128) for hot, O(256)
  for warm. Measured improvement: ~2900x for SIEVE, ~1100x for warm
  evictOne at 1 M entries.
- Zero allocations on the capped path (returns false without writing a
  tombstone).
- The `evictPreferBacked` worst case drops from O(10N) to O(640) (5 x
  128 probes).

### Negative / trade-offs

- **Warm tier rejects more Puts under heavy read load**: `ErrOverBudget`
  returns to the caller, which falls through to origin. This is the
  correct behavior — serving from origin is better than blocking all
  warm Gets for 10-20 ms.
- **Hot tier budget overshoot**: the shard stays over `perShardMax` by
  one Put's object size until the sweeper retries. Already the existing
  behavior when `evictPreferBacked` exhausts `maxEvictSkips`.
- **SIEVE aging property**: capping the sweep means some visited entries
  keep their visited bit across multiple Evict calls. This is fine —
  they will eventually be swept when the hand reaches them. The
  algorithm's "second chance" property is preserved; we just limit how
  many second chances we hand out per call.

### Risks

- **Progress guarantee**: `EvictBounded` advances the hand and clears
  visited bits during the capped sweep. On the next call, the hand
  resumes from the advanced position and encounters an entry whose
  visited bit was just cleared — it evicts immediately. The system is
  NOT stuck; it makes progress across multiple capped calls, each
  bounded at 128/256 probes. Under a pure read workload with no writes,
  there is no over-budget condition triggering eviction in the first
  place.

## Alternatives considered

- **Unbounded sweep (status quo)**: rejected. 2.5 ms per Evict call at
  1 M entries blocks all warm Gets under `idxMu.Lock`.
- **Adaptive probe cap based on list length**: rejected. Adds complexity
  and makes worst-case latency unpredictable. A fixed cap is simpler and
  bounds the worst case deterministically.
- **Separate sweeper goroutine for warm tier**: deferred. The sweep cap
  makes the synchronous path fast enough; an async sweeper can be added
  later if benchmarks show it's still needed.

## References

- Eviction hardening plan: `docs/plans/eviction-hardening.md` Phase 1
- SIEVE paper: Zhang et al., "SIEVE is Simpler than LRU", NSDI 2024
- Benchmark results: `bench/results/eviction-hardening-baseline.md`
