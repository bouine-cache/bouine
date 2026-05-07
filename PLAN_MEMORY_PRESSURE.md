# PLAN_MEMORY_PRESSURE.md — HIT p99 latency under eviction pressure

## TL;DR

A load test (innerspace, 6000 unique paths × 15.6 KB against a 64 Mo hot
tier, 100 VUs, 15 min, in-cluster) measured **HIT p99 degrading from 5 ms
(baseline) to 100 ms under sustained eviction**. Hit ratio fell to ~37 %,
RSS held at 83 Mi, zero OOMKills.

The brief that triggered this plan asked for *"atomic visited-bit +
sharded cache."* **Both already exist** in the current codebase:

| Requested optimisation | Status | Evidence |
|---|---|---|
| Atomic SIEVE visited bit | ✅ shipped | `internal/storage/sieve/sieve.go:26` — `visited atomic.Bool`, read under RLock via `Entry.Visited()` |
| Sharded hot cache, per-shard lock | ✅ shipped | `internal/storage/hot.go:23-47` — `[]shard`, each with its own `sync.RWMutex`, power-of-two count = `min(NumCPU,64)` |
| RLock fast path for hits | ✅ shipped | `internal/storage/hot.go:113-135` |

So the degradation is **not** caused by a missing visited-bit atomicity or
a missing shard split. Profiling the hit path against the current code
identifies **two real root causes**, documented below, and this plan
addresses those.

---

## 1. Root-cause analysis

### Cause A — global `bansMu` serialises every cache hit (primary)

`HotStore.Get` fast path calls `matchesActiveBan(obj)` on **every** hit
(`hot.go:120`). That function unconditionally acquires a **process-global**
mutex (`hot.go:31` `bansMu sync.Mutex`, taken at `hot.go:306`) even when
the ban list is empty:

```go
func (h *HotStore) matchesActiveBan(obj *api.Object) bool {
    h.bansMu.Lock()          // <-- global lock on EVERY hit
    defer h.bansMu.Unlock()
    if len(h.activeBans) == 0 {
        return false         // empty 99.9% of the time, but lock already taken
    }
    ...
}
```

The per-shard `RWMutex` work done just above is wasted: all shards
re-converge on one global mutex immediately afterwards. Under 100–200
concurrent VUs this is a single serialisation point on the hottest path
in the system. The more cores/VUs, the worse the contention tail — which
is exactly the 5 ms → 100 ms p99 signature we observed.

This cost is present at **all** times, not just under eviction; eviction
pressure simply raised request concurrency enough to expose it.

### Cause B — synchronous eviction holds the shard write lock (secondary)

`HotStore.Put` runs the SIEVE eviction loop inline while holding the shard
write lock (`hot.go:170-183`):

```go
s.mu.Lock()
defer s.mu.Unlock()
for s.bytes+size > perShardMax && s.evict.Len() > 0 {
    evKey, ok := s.evictPreferWarm()   // hand sweep + map delete, under WLock
    ...
}
```

Two compounding effects:

1. **Writer starves readers.** Go's `sync.RWMutex` blocks *new* `RLock`
   acquirers as soon as a writer is waiting. During the overflow test 62 %
   of requests were MISSes → constant `Put` → the write lock on each shard
   is almost always held or contended, so HIT fast-path `RLock` calls queue
   behind eviction work.
2. **Unbounded eviction batch.** A single `Put` may evict many entries in
   one locked loop when several large objects must be freed, lengthening
   the worst-case lock hold time.

### Cause C — GC pressure (contributing, config-level)

`GOMEMLIMIT=72MiB` against a 64 Mo cache leaves ~8 MB for stacks, request
buffers and metadata. RSS reached 83 Mi → the GC was running hot to stay
near the limit, adding stop-the-world pauses in the 1–100 ms band. This is
a deployment/config concern (innerspace `k8s/`), not a bouine code change,
and is tracked here only for completeness.

---

## 2. Goals & non-goals

**Goals**
- Restore HIT p99 to ≤ 5 ms under ≥ 1.4× working-set overflow at 200 VUs.
- Keep the hit path lock-free in the common (no-ban) case.
- Remove eviction work from the request critical path.
- No change to external behaviour, config schema, or the `Store` interface.

**Non-goals**
- Admission policy (TinyLFU) — separate effort; only revisited in §6.
- Warm-tier sizing/config — deployment concern.
- GC tuning — handled in innerspace deployment, not here.

---

## 3. Phase 1 — Lock-free ban check on the hit path (highest ROI)

**Change.** Gate `matchesActiveBan` behind an atomic fast path so the
global mutex is never taken when no bans are active.

Add to `HotStore`:

```go
type HotStore struct {
    ...
    banCount atomic.Int64 // number of active lazy bans; 0 == fast path
}
```

Maintain it wherever `activeBans` is mutated:
- `Ban()` after append (`hot.go:248`): `h.banCount.Store(int64(len(h.activeBans)))`
- `matchesActiveBan` pruning (`hot.go:319`): update after rebuilding `live`.

Rewrite the guard:

```go
func (h *HotStore) matchesActiveBan(obj *api.Object) bool {
    if h.banCount.Load() == 0 {     // lock-free: the overwhelmingly common case
        return false
    }
    h.bansMu.Lock()
    defer h.bansMu.Unlock()
    ...
}
```

**Why it works.** The blog (and virtually every steady-state deployment)
has zero active bans almost all the time. A single relaxed atomic load
replaces a global `Lock/Unlock` on every hit, eliminating the cross-shard
serialisation point entirely.

**Effort.** ~15 lines. **Risk.** Minimal — semantics unchanged when bans
exist; the atomic is only a fast-path gate.

**Acceptance.** `BenchmarkHotGet` (see §5) shows flat p99 as GOMAXPROCS
scales 1→8; before the change p99 rises super-linearly with cores.

---

## 4. Phase 2 — Move eviction off the request critical path

Two options, in increasing order of effort. Phase 2a is the recommended
first step; 2b is optional if 2a doesn't fully close the gap.

### 2a — Evict outside the data lock

Keep eviction triggered by `Put`, but don't hold the shard write lock
during the SIEVE hand-sweep. Collect victims first, drop the lock, then
free. The current code already separates "pick victim" (`evictPreferWarm`)
from "delete from map"; restructure so the **decision** of how much to
free is computed, the lock released, and large `[]byte` bodies dropped
without the lock held. The map mutation itself stays under a short lock,
but the GC-visible body references are cleared outside it.

Concretely: cap the eviction batch per `Put` to a small constant (e.g. 4
victims) so no single `Put` can hold the lock for an unbounded sweep;
remaining over-budget bytes are reclaimed by the background sweeper (2b)
or the next `Put`.

### 2b — Background eviction goroutine (optional)

Add a per-store sweeper goroutine fed by a buffered channel:

```go
type HotStore struct {
    ...
    evictSignal chan int    // shard index needing eviction
    done        chan struct{}
}
```

- `Put` computes over-budget under the lock, sets the new entry, releases
  the lock, then does a **non-blocking** send of the shard index to
  `evictSignal` (drop if full — coalesces bursts).
- The sweeper drains the channel, locks the named shard briefly, evicts
  down to the per-shard budget, unlocks. Eviction latency leaves the
  request path entirely; `Put` p99 becomes a map insert.

**Trade-off.** The cache may transiently exceed `maxBytes` by the size of
in-flight `Put`s before the sweeper catches up. Bound this by keeping the
inline batch cap from 2a as a safety valve, and size `evictSignal` to
`len(shards)`. Document the transient overshoot in the `HotConfig`
comment.

**Lifecycle.** Start the sweeper in `NewHotStore`; stop it via a new
`Close()` wired into the engine shutdown sequence
(`cmd/bouine/cmd/engine.go` `registerShutdownSteps`).

**Effort.** 2a ~40 lines; 2b ~80 lines + shutdown wiring + tests.
**Risk.** 2b introduces a goroutine and a transient-overshoot invariant —
needs the chaos/soak test in §5 to validate memory stays bounded.

---

## 5. Validation

### Micro-benchmarks (`internal/storage/hot_bench_test.go`)

Extend the existing bench file:

1. `BenchmarkHotGet_NoBans_Parallel` — `b.RunParallel`, GOMAXPROCS 1/2/4/8,
   pre-warmed shard, 100 % hit. Assert ns/op stays ~flat across cores
   after Phase 1 (proves the global lock is gone).
2. `BenchmarkHotPut_Overflow` — working set 1.5× `MaxBytes`, measure
   `Put` ns/op and allocs before/after Phase 2.
3. `BenchmarkHotMixed_80_20` — 80 % Get / 20 % Put at 1.4× overflow;
   report p99 via a latency histogram (use `b.ReportMetric`).

Gate: Phase 1 must show `BenchmarkHotGet_NoBans_Parallel` ns/op variance
< 15 % from 1→8 cores.

### Integration / soak

- `test/chaos` — add `TestChaos_HotOverflowLatency`: in-process driver,
  1.5× overflow, 30 s, assert no goroutine leak and (2b) RSS bounded
  within `MaxBytes × 1.1`.
- Re-run the innerspace in-cluster Phase 5 (`make loadtest-ic-memory-pressure`,
  6000 paths) and compare against this baseline:

| Metric | Before (this run) | Target after P1+P2 |
|---|---|---|
| HIT p99 | 100 ms | ≤ 5 ms |
| MISS p99 | 500 ms | ≤ 500 ms (unchanged; origin-bound) |
| Hit ratio | 37 % | unchanged (capacity-bound, not the goal) |
| RSS | 83 Mi | ≤ 90 Mi, stable |
| OOMKilled | 0 | 0 |

Hit ratio is **not** a target — it's bounded by cache capacity vs working
set. This plan targets *latency of the hits that do happen*, not their
frequency.

---

## 6. Out of scope (future work)

- **Admission filter (TinyLFU / frequency sketch).** The overflow test's
  6000 single-shot URLs are pure cache pollution — they're never
  re-requested, yet each evicts a potentially hot object. An admission
  filter that only promotes objects seen ≥ 2× would preserve hit ratio
  under scan-heavy workloads. Note: a "W-TinyLFU phantom" was removed in
  an earlier simplification pass; reintroducing a *minimal* doorkeeper is
  the natural follow-up once latency (this plan) is fixed.
- **Per-shard byte budget skew.** `maxBytes / numShards` assumes uniform
  key distribution; `xxhash` of `api.Key` is uniform enough for the blog,
  but a global byte counter with per-shard soft limits would be fairer
  under skewed key spaces.

---

## 7. Implementation order & sizing

| Step | Scope | Effort | Risk |
|---|---|---|---|
| 1 | Atomic `banCount` fast path (`hot.go`) | ~15 LOC | low |
| 2 | `BenchmarkHotGet_NoBans_Parallel` + gate | ~40 LOC | low |
| 3 | Phase 2a — bounded inline eviction batch | ~40 LOC | low |
| 4 | Phase 2b — background sweeper + `Close()` wiring | ~80 LOC | medium |
| 5 | Chaos soak test + re-run innerspace Phase 5 | ~60 LOC | low |

Phase 1 alone is expected to recover most of the regression because the
global `bansMu` is the dominant contention point on the hit path. Ship it
first, re-measure, and only proceed to Phase 2b if HIT p99 is still above
the 5 ms target under overflow.
