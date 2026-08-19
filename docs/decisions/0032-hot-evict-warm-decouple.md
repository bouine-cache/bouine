# ADR-0032: Decouple hot SIEVE eviction from warm tombstone

- **Status**: Accepted
- **Date**: 2026-08-18
- **Deciders**: @chris.dupin
- **Phase**: phase 2

## Context

Issue [#484](https://github.com/bouine-cache/bouine/issues/484): under the
`prod` stress workload (Zipf 80/20, 51k keys, `varied` TTL mode
10/60/300/3600 s), hit ratio decays 90% → 66% over T+6 m → T+55 m and the
hot tier turns over 32× (554 k evictions / 17 k entries). The baseline
confirmed warm evictions = 0 across the entire 1 h run — the smoking gun.

**Root cause:** `HotStore.notifyEvict` fires `h.onEvict(key)` for every
backed entry evicted by SIEVE. The callback (wired in `NewTieredStore`)
enqueues the key onto `tombstoneQueue`, and `drainTombstones` calls
`warm.Delete(key)` — destroying the warm copy that made the entry
"cheap to evict" in the first place. Under TTL-mix churn, short-TTL
buckets continuously expire → miss → `Put` (with `visited=false`) →
SIEVE eviction pressure → `evictPreferBacked` evicts a backed hot entry
and tombstones its warm backup. The 60 s `WarmSyncInterval` re-marks
the re-fetched entry as backed, regenerating the "eviction-preferred"
pool. 554 k / 17 k = 32× turnover is exactly this loop.

A contributing factor is that `Put` inserts with `visited=false`: a
freshly fetched/re-promoted key has zero SIEVE protection until its
next `Get` hits the slow path. Under bursty refetch it can be re-evicted
before any subsequent `Get` marks it visited.

## Decision

Two changes to the hot SIEVE-eviction → warm lifecycle:

1. **Unprotect on SIEVE eviction.** On SIEVE-eviction of a backed hot
   entry, `warm.Unprotect(key)` the warm copy instead of tombstoning
   it. The warm copy persists (recoverable on the next miss via warm
   hit → promote) and its `protected` flag is cleared so warm SIEVE can
   evict it under its own pressure. Tombstoning remains in effect for
   the five non-SIEVE hot-removal paths (reaper, lazy ban, eager ban,
   Delete, Put-overwrite) where the warm copy genuinely should be
   deleted.

   The `notifyEvict` function gains an `evictReason` parameter
   (`evictReasonSIEVE`, `evictReasonReaper`, `evictReasonBan`,
   `evictReasonDelete`) so the callback can be selectively dispatched.
   For SIEVE evictions, a second callback `OnEvictDemoted` fires
   instead of `OnEvict`, enqueuing the key onto a new
   `warmUnprotectQueue` (async, same pattern as `tombstoneQueue`).
   `warm.Unprotect` is called from the drain step outside the hot
   shard lock.

2. **Insert with `visited=true`.** In `Put`, after `s.evict.Access`,
   call `se.MarkVisited()`. This gives every newly inserted entry one
   SIEVE sweep of protection. Zero hit-path cost: `Put` runs on the
   miss path and on warm-hit promotion, neither of which is the
   zero-alloc hot-Get fast path. `MarkVisited` is a single
   `atomic.Bool` store — no allocation.

## Consequences

### Positive
- Eliminates the 32× dual-tier destruction feedback loop under TTL-mix
  churn: 0 cold misses (warm hits → re-promote).
- Warm SIEVE can now reclaim demoted entries (they are no longer
  stranded-protected). Warm evictions go from 0 to non-zero under
  pressure, as expected.
- Fresh inserts survive at least one SIEVE sweep, reducing
  bursty-refetch re-eviction.
- Zero hit-path allocation impact (bench-gate confirmed: `allocs/op ==
  0` on all `BenchmarkGate_*` hit-path benchmarks).
- No conformance regression (339/365 = 92.9%, identical to `main`).

### Negative / trade-offs
- Warm disk may grow faster under churn, because warm no longer gets
  hot-SIEVE-driven tombstones. Bounded by `WarmMaxBytes` /
  `WarmMaxDiskBytes` / `MinFreeDisk` budgets and compaction. Demoted
  entries are SIEVE-evictable (not stranded), so warm SIEVE reclaims
  them under pressure.
- `evictPreferBacked` becomes less useful: evicting a backed entry no
  longer frees warm space (it demotes instead). The preference is still
  correct (backed evictions are recoverable via warm hit → promote),
  but the warm-space-reclaim side effect is gone.
- `visited=true` on insert may shift the SIEVE sweep pattern: the hand
  advances more slowly under miss-stream-dominated workloads. This is
  the intended effect and is bounded by `maxSweepProbes`.

### Risks
- **Lock ordering:** `warm.Unprotect` takes `warm.idxMu`. The existing
  warm→hot path (`warm.evictOneLocked → OnEvict → hot.ClearBacked`) takes
  `warm.idxMu → hot.shard.mu`. Calling `warm.Unprotect` synchronously
  under the hot shard lock would create `hot.shard.mu → warm.idxMu` —
  a lock-ordering cycle and deadlock risk. **Mitigation:** `OnEvictDemoted`
  enqueues to `warmUnprotectQueue` (buffered channel), drained in
  `drainQueues()` and `runWarmSyncCycle()` outside the hot lock. This
  mirrors the existing async `tombstoneQueue` pattern.
- **Queue overflow:** `warmUnprotectQueue` is buffered (default 65536).
  On overflow the key falls back to `tombstoneQueue` so the warm entry
  is deleted (reclaimed) rather than stranded as permanently protected.
  `dropped_warm_unprotects` in the drain log counts these fallbacks;
  treat any non-zero value as a capacity signal — sustained overflow
  means the drain interval is too slow for the eviction rate.
- **Pre-existing Put-overwrite race (not introduced by this change):**
  `hot.Put` overwriting a backed key enqueues a tombstone via
  `notifyEvict(reason=Delete)`, then `TieredStore.Put` immediately
  calls `warm.Put` + `warm.Protect` for the new object. The async
  tombstone drain later calls `warm.Delete(key)` on the same key,
  racing the fresh warm write and potentially deleting it. This
  affects reaper/ban/Delete tombstones correctly but the
  Put-overwrite case can cause a cold miss on the next access. It is
  pre-existing and rare relative to SIEVE eviction. Tracked as a
  follow-up.

## Alternatives considered

1. **Global `OnEvict = nil` (first draft):** Rejected — breaks the
   warm `Protect` invariant. `pickEvictVictim` skips protected entries,
   so warm SIEVE cannot reclaim stranded-protected entries, and compaction
   does not remove protected live entries. Warm fills with un-evictable
   dead weight until `ErrOverBudget`.

2. **Fix #3 from the issue (popularity-gated tombstoning):** Rejected —
   `windowHits` is reset to 0 on every `Put` (fresh `hotEntry` from the
   pool), so in the bursty-refetch window between `Put` and the first
   `Get`, `windowHits == 0` — Fix #3 would still tombstone freshly-refetched
   popular keys on their first eviction cycle, which is the exact loop
   this issue is trying to break. `Unprotect` is strictly better:
   it retains the data regardless of popularity and lets warm SIEVE apply
   its own visited-bit signal.

## References

- Issue [#484](https://github.com/bouine-cache/bouine/issues/484)
- ADR-0026: SIEVE sweep cap (`maxSweepProbes`)
- ADR-0031: Pluggable eviction framework
- `internal/storage/hot.go`: `notifyEvict`, `evictReason`, `evictPreferBacked`
- `internal/storage/tiered.go`: `warmUnprotectQueue`, `drainWarmUnprotects`
- `internal/storage/warm/warm.go`: `Unprotect`, `Protect` lifecycle
