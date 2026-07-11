# Plan: Predictive Refresh Prioritization

Status: Draft (revision 8 — amended after 8 Linus review rounds)  
Date: 2026-07-11  
Depends on: ADR-0016 (refresh-before-expiry), ADR-0021 (popularity gate), ADR-0022 (persist cycles)

## 1. Problem Statement

Refresh-before-expiry (ADR-0016) proactively refreshes cached objects before
their TTL expires. The popularity gate (ADR-0021, `refresh_min_hits`) was
designed to filter out unpopular objects. Persist cycles (ADR-0022) were
designed to bridge short TTL and long inter-access times.

**The system fails at high cardinality.** Routes with millions of distinct
paths and short TTLs (e.g. 30s) generate massive refresh traffic. With 1M
objects and 30s TTL, the scheduler fires ~33k conditional GETs/s. The
popularity gate does not help because of two root causes:

### 1.1 Root cause: `Object.Hits` is cumulative, not per-window

`refreshFrom304` (`handler.go:1015`) preserves `stale.Hits` into the refreshed
object. `doBackgroundRefresh` (`handler.go:607`) does `obj.Hits = stale.Hits`.
**Hits never resets.** An object that accumulated 100 hits during its first TTL
window will carry Hits=100 into every subsequent window. The popularity gate
checks `obj.Hits < minHits` (`handler.go:1365`), but since Hits is cumulative,
any object that was *ever* popular passes the gate forever.

### 1.2 Root cause: `Object.Hits` only increments once per SIEVE cycle

`Object.Hits` is incremented in `hot.go:261` only on the SIEVE slow path —
when the `visited` bit transitions from false to true. Every subsequent hit
in the same SIEVE cycle takes the fast path (`hot.go:234`) and does not
increment `Hits`. This means:

- With `refresh_min_hits: 1`, the gate works as a boolean "was this accessed"
  signal — the ADR-0021 design intent.
- With `refresh_min_hits: 2` or higher, the gate is **impossible to pass**
  in a single TTL window, because Hits can only reach 1 per SIEVE cycle.

Additionally, the SIEVE eviction hand can clear the `visited` bit while the
object is still fresh, causing Hits to increment more than once per TTL window
if the hand sweeps through. This makes Hits a noisy, eviction-dependent
approximation of access count, not a clean per-window hit counter.

### 1.3 Root cause: no cost/benefit weighting

All objects are refreshed with equal priority. A 512-byte JSON response and a
4MB image are both refreshed after the same hit-count threshold. But the 4MB
image costs 8000x more to re-fetch from origin on a cache miss. Refreshing the
image prevents an expensive origin fetch; refreshing the JSON prevents a cheap
one. The gate treats them identically.

### 1.4 Root cause: no upstream protection

There is a per-route concurrency limit (`refresh_concurrency`, default 8) but
no rate limit. With 1M objects and 30s TTL, even with concurrency=8, the
scheduler pops entries as fast as slots free up. There is no cap on
refreshes-per-second. The upstream can be saturated by refresh traffic alone,
even when every refresh results in a lightweight 304.

### 1.5 Root cause: proactive refresh for cold long-tail

Every newly cached object gets one unconditional refresh cycle
(`isRefresh=false` bypasses the gate). For a route with 1M objects, that's 1M
conditional GETs in the first TTL window, even if most objects will never be
accessed again. The SWR mechanism (reactive refresh) is self-selecting — only
objects that are actually accessed trigger a refresh — but it is not used as
the primary strategy for high-cardinality routes.

## 2. Design Overview

Five changes, ordered by impact-to-complexity ratio:

1. **Per-window hit counter** — add a separate `windowHits` counter on
   `hotEntry` that increments on **every** Get (both fast and slow paths).
   Reset it to 0 on each refresh store. The popularity gate uses `windowHits`
   instead of `Object.Hits`. This is a storage-layer change (L2).

2. **Reset `Object.Hits` on refresh** — stop carrying `stale.Hits` into the
   refreshed object. `Object.Hits` reverts to its original purpose (SIEVE
   eviction signal) and the popularity gate switches to `windowHits`.

3. **Cost-weighted refresh score** — multiply `windowHits` by object size
   to produce a refresh priority score. Gate on score in addition to hit
   count. Uses the existing `Object.BodySize` field. One new config field.

4. **Per-route refresh rate limit** — token-bucket rate limiter that caps
   refresh fetches per second per route. Directly prevents upstream
   saturation. One new config field.

5. **Reactive-first mode for high-cardinality routes** — opt-in mode that
   skips proactive refresh entirely and relies on SWR for all objects. Only
   objects that prove popularity (by being accessed while stale) get promoted
   to proactive refresh. Requires changing SWR stores to use `isRefresh=true`
   so the popularity gate applies. One new config field.

Changes 1 and 2 are tightly coupled and must ship together. Changes 3, 4, 5
are independent and can ship in any order after 1+2.

## 3. Change 1: Per-window hit counter (storage layer)

### 3.1 Problem

`Object.Hits` is incremented only on the SIEVE slow path (`hot.go:261`),
making it a "visited bit flip count" rather than a hit count. It cannot serve
as a per-window popularity signal for `refresh_min_hits > 1`.

### 3.2 Solution

Add a `windowHits atomic.Int64` field to `hotEntry` (`internal/storage/hot.go`).
Increment it on **every** successful Get — both the fast path (line 234) and
the slow path (line 261). This is a true per-object hit counter, independent
of the SIEVE visited bit.

### 3.3 Implementation

**`internal/storage/hot.go`** — modify `hotEntry`:

```go
type hotEntry struct {
    obj         *api.Object
    sieve       *sieve.Entry[api.Key]
    hasBackup   bool
    windowHits  atomic.Int64  // incremented on every Get; reset on refresh
}
```

**Fast path** (`hot.go:234`) — add one `atomic.Add`:

```go
if e != nil && e.sieve.Visited() {
    e.windowHits.Add(1)   // ← new
    obj := e.obj
    s.mu.RUnlock()
    // ... rest unchanged
}
```

A concurrent `Put` may replace the entry between the RLock and the `Add`.
The increment goes to the old entry (about to be replaced) and is lost.
This is correct: the hit was on the old object, and the new entry starts
at 0 (the refresh resets the window).

**Slow path** (`hot.go:261`) — add one `atomic.Add`:

```go
if e != nil {
    s.evict.Access(key, func(k api.Key) *sieve.Entry[api.Key] {
        return e.sieve
    })
    e.obj.Hits++
    e.windowHits.Add(1)   // ← new
    obj = e.obj
}
```

**New method** — add `WindowHits(key) int64` to `HotStore`:

```go
// WindowHits returns the per-window hit count for key, or 0 if not found.
// Called by the cache layer during refresh to evaluate the popularity gate.
func (h *HotStore) WindowHits(key api.Key) int64 {
    s := h.shard(key)
    s.mu.RLock()
    e := s.entries[key]
    var n int64
    if e != nil {
        n = e.windowHits.Load()
    }
    s.mu.RUnlock()
    return n
}
```

**Store interface** — add `WindowHits(key api.Key) int64` to the
`storage.Store` interface. `TieredStore` delegates to `HotStore`. The warm
tier returns 0 (objects in warm-only have no hot-tier counter; they are by
definition not being actively served from the hot tier, so `windowHits=0`
is correct — they should not pass the popularity gate).

**No reset method needed** — `Put` always creates a new `hotEntry` struct
(`hot.go:349`: `s.entries[key] = &hotEntry{obj: obj, sieve: se}`). The zero
value of `atomic.Int64` is 0, so every `Put` — including refresh stores —
naturally resets `windowHits` to 0. Adding a `ResetWindowHits` method would
be dead code. The cache layer reads `windowHits` before the `Put` (in
`triggerBgRefresh` or `doBackgroundRevalidate`) and passes the value through
to `storeObject`. The `Put` inside `storeObject` resets the counter as a
side effect of creating the new entry.

### 3.4 Hit-path impact

The fast path gains one `atomic.Int64.Add` — a single locked instruction on
x86/arm64. Benchmarks must prove this adds < 1 ns/op and zero allocations.
The AGENTS.md hit-path budget is < 5 us CPU per request at p50 with zero
allocations. An atomic add is within budget.

### 3.5 Warm-tier interaction

When an object is promoted from warm to hot (on-demand), a new `hotEntry` is
created with `windowHits = 0`. This is correct: the object was not being
served from the hot tier, so it has no hot-tier hits in the current window.

When an object is evicted from hot to warm-only, its `windowHits` is lost.
If it is promoted again, it starts at 0. This is acceptable: if it wasn't
being accessed enough to stay in the hot tier, `windowHits` was likely low.

### 3.6 SIEVE interaction

`windowHits` is independent of the SIEVE `visited` bit. The eviction hand
sweeping through and clearing `visited` does not affect `windowHits`. This
fixes the noise problem identified in §1.2 — `windowHits` is a clean count,
not an eviction-dependent approximation.

`Object.Hits` (the SIEVE signal) remains unchanged. It is still incremented
on the slow path and used by SIEVE eviction internally (if ever). The
popularity gate no longer reads it.

## 4. Change 2: Reset `Object.Hits` and switch gate to `windowHits`

### 4.1 Problem

`refreshFrom304` (`handler.go:1015`) preserves `stale.Hits` into the refreshed
object. `doBackgroundRefresh` (`handler.go:607`) does `obj.Hits = stale.Hits`.
Hits never resets, making the popularity gate measure lifetime popularity.

### 4.2 Solution

1. Stop preserving `Object.Hits` across refresh cycles. Set `Hits = 0` on
   every refresh store. `Object.Hits` reverts to its SIEVE-internal purpose.

2. Before the refresh store, read `windowHits` from the store and pass it as
   `staleHits` to `storeObject`. The popularity gate checks `staleHits`
   instead of `obj.Hits`.

3. The refresh store (`store.Put`) creates a new `hotEntry` with
   `windowHits = 0` (zero value), automatically resetting the counter for
   the new TTL window. No explicit reset call is needed.

### 4.3 Implementation

**`refreshFrom304`** (`handler.go:1015`):
- Change `refreshed.Hits = stale.Hits` to `refreshed.Hits = 0`.

**`doBackgroundRefresh` 200 path** (`handler.go:607`):
- Change `obj.Hits = stale.Hits` to `obj.Hits = 0`.

**`storeObject`** (`handler.go:1344`):
- Add `staleHits int64` parameter.
- Replace `obj.Hits < uint64(h.refreshMinHits)` with `staleHits < int64(h.refreshMinHits)`.
- No reset call needed — `store.Put` creates a new `hotEntry` with
  `windowHits = 0` as a side effect.

**`triggerBgRefresh`** (`handler.go:480`):
- After the `store.Get` succeeds and before spawning the goroutine, read
  `staleHits := h.store.WindowHits(key)`. Reading immediately after `Get`
  minimizes the race window where the entry could be evicted between `Get`
  and `WindowHits`.
- Pass `staleHits` through to `doBackgroundRefresh` (new parameter) →
  `storeObject`.

**Call sites** (all paths into `storeObject`):
- 304 refresh: `storeObject(ctx, key, refreshed, req, true, staleHits)`
- 200 refresh: `storeObject(ctx, key, obj, req, true, staleHits)`
- SWR 304 refresh: `storeObject(ctx, key, refreshed, r, true, staleHits)` —
  **changed from `false` to `true`** (see §4.4 for rationale).
  `staleHits` is read via `h.store.WindowHits(key)` in `doBackgroundRevalidate`
  before `collapsedFetch` (see §7.7).
- SWR 200 refresh: `storeObject(ctx, key, obj, r, true, staleHits)` —
  **changed from `false` to `true`**.
  `staleHits` is read via `h.store.WindowHits(key)` in `doBackgroundRevalidate`
  before `collapsedFetch` (see §7.7).
- Initial store (foreground): `storeObject(ctx, key, obj, r, false, 0)`
- Foreground 304 revalidation: `storeObject(ctx, key, refreshed, req, false, 0)`
- Foreground 200 store (`writeAndMaybeStore`): `storeObject(ctx, key, obj, r, false, 0)`
- POST/PUT invalidation re-cache: `storeObject(ctx, key, obj, getReq, false, 0)`

For non-refresh stores (`isRefresh=false`), `staleHits=0` is correct: the
popularity gate is bypassed, so the value is irrelevant.

### 4.4 SWR behavioral change

Changing SWR stores from `isRefresh=false` to `isRefresh=true` is a behavioral
change that affects **all** routes, not just reactive-first ones. Today, every
SWR-triggered refresh unconditionally schedules proactive refresh for the next
TTL window. After the change, SWR-triggered refresh is subject to the
popularity gate: if `staleHits < refreshMinHits`, the object is not
re-scheduled.

This is the correct behavior: if an object was only accessed once (during SWR)
and `refresh_min_hits` is set higher, it should not be promoted to perpetual
proactive refresh. For routes without `refresh_min_hits` (gate disabled,
default), `isRefresh=true` has no effect — the gate is skipped and the object
is unconditionally scheduled, same as today.

**Migration note:** Routes that rely on SWR to bootstrap proactive refresh
without a popularity gate are unaffected (gate disabled by default). Routes
with `refresh_min_hits > 0` get stricter gating, which is the intended
behavior.

### 4.5 Impact on admin/stats

The admin API and `api.Stats` will show `Object.Hits` as a SIEVE-internal
counter (low numbers, resets on refresh) instead of cumulative hits. The
dashboard should surface `windowHits` (via a new admin endpoint or metric)
for operator visibility. Cumulative hit counts remain available in Prometheus
(`bouine_requests_total` with `cache_result="hit"`).

### 4.6 Migration

No on-disk format change. `Object.Hits` is still a `uint64` serialized in the
warm-tier codec. Objects loaded from an old warm tier will have their
cumulative Hits value, which will be used as the SIEVE signal and reset to 0
on the next refresh. The `windowHits` counter starts at 0 for all objects
loaded from warm (they have no hot-tier history). No migration code needed.

## 5. Change 3: Cost-weighted refresh score

### 5.1 Problem

The popularity gate treats all objects equally regardless of size. A 512-byte
object with 1 hit and a 4MB object with 1 hit both pass `refresh_min_hits: 1`.
But the 4MB object prevents a 4MB origin fetch on the next miss; the 512-byte
object prevents a 512-byte fetch. The cost-benefit ratio differs by 8000x.

### 5.2 Solution

Introduce a refresh priority score that combines per-window hits with object
size:

```
refresh_score = staleHits × obj.BodySize
```

When `refresh_min_score > 0`, the gate checks
`staleHits × obj.BodySize >= refresh_min_score` in addition to the hit-count
gate. Both gates must pass to re-schedule.

### 5.3 Config

New field on `RouteCache`:

```go
// RefreshMinScore is the minimum refresh priority score required for
// re-scheduling after a background refresh. The score is computed as
// staleHits × obj.BodySize, where staleHits is the per-window hit count
// from the previous TTL window (see Change 1). This weights the refresh
// decision by object size: a 4 MB object with 1 hit (score 4,194,304)
// outranks a 512 B object with 100 hits (score 51,200).
// Zero (default) disables the score gate — only refresh_min_hits
// applies. When both are set, both gates must pass.
RefreshMinScore int64 `yaml:"refresh_min_score,omitempty" json:"refresh_min_score,omitempty"`
```

Validation: `refresh_min_score` requires `refresh_before_expiry: true` and
`refresh_min_hits > 0` (the score gate refines the hit-count gate).

### 5.4 Implementation

**`storeObject`** — factor the gate check into a single function to avoid
duplicated persist/unregister blocks (identified in Linus review):

```go
// shouldRefresh returns true if the object passes both the hit-count and
// score popularity gates. Returns false if either gate fails; the caller
// handles persist-cycle logic.
func (h *Handler) shouldRefresh(staleHits int64, obj *api.Object) bool {
    if staleHits < int64(h.refreshMinHits) {
        return false
    }
    if h.refreshMinScore > 0 && staleHits*obj.BodySize < h.refreshMinScore {
        return false
    }
    return true
}
```

In `storeObject`, replace the two separate gate blocks with:

```go
if isRefresh && h.refreshMinHits > 0 && !h.shouldRefresh(staleHits, obj) {
    if h.refreshPersistCycles > 0 && h.refreshRegistry.DecrementPersist(key) {
        h.scheduler.Schedule(key, obj.StoredAt.Add(obj.TTL-h.refreshMargin))
        h.refreshMetrics.IncSkips("persist_cycle")
        return
    }
    h.refreshRegistry.Unregister(key)
    h.refreshMetrics.IncSkips("below_min_hits")
    return
}
```

The skip reason is `below_min_hits` for both gate failures. If operators need
to distinguish, they can check `bouine_refresh_skips_total{reason="below_min_hits"}`
against the score threshold via the dashboard. Adding a separate reason is
unnecessary cardinality.

### 5.5 Example

Route with `refresh_min_hits: 1`, `refresh_min_score: 100000` (100 KB):

| Object | BodySize | staleHits | Hit gate | Score | Score gate | Decision |
|--------|----------|-----------|----------|-------|------------|----------|
| 512 B JSON | 512 | 5 | pass (5 ≥ 1) | 2,560 | fail | skip |
| 100 KB HTML | 102,400 | 2 | pass | 204,800 | pass | refresh |
| 4 MB image | 4,194,304 | 1 | pass | 4,194,304 | pass | refresh |
| 4 MB image | 4,194,304 | 0 | fail | 0 | fail | skip |

### 5.6 Integer overflow

`staleHits` is `int64`, `BodySize` is `int64`. The product can overflow for
extreme values. In practice, staleHits for a single TTL window is bounded by
the request rate (a 30s window with 10k RPS yields at most 300k hits).
BodySize is capped by `max_object_size` (default 64 MiB = 67,108,864). The
product is at most ~2^43, well within `int64` range. No overflow protection
needed.

## 6. Change 4: Per-route refresh rate limit

### 6.1 Problem

The scheduler fires refreshes as fast as `refreshSem` slots free up. With
concurrency=8 and 100ms average origin response time, that's up to 80
refreshes/s per route. For a route with 1M objects and 30s TTL, the scheduler
has ~33k entries due to fire every 30s. The upstream is perpetually saturated
with conditional GETs.

### 6.2 Solution

Add a per-route token-bucket rate limiter that caps refresh fetches per
second. When the rate limit is hit, the refresh is deferred: the entry is
re-scheduled with a short jittered delay rather than dropped.

### 6.3 Config

New field on `RouteCache`:

```go
// RefreshMaxRPS caps the number of background refresh fetches per second
// per route. When the cap is reached, pending refreshes are deferred with
// jittered backoff rather than dropped. Zero (default) means no rate limit
// (current behaviour). Set to a fraction of the upstream's capacity to
// prevent refresh traffic from saturating the origin.
RefreshMaxRPS int `yaml:"refresh_max_rps,omitempty" json:"refresh_max_rps,omitempty"`
```

Validation: `refresh_max_rps` requires `refresh_before_expiry: true`. Range
0 (unlimited, default) or 1–10000.

### 6.4 Implementation

New file `internal/cache/rate_limiter.go`:

```go
package cache

import (
    "math/rand/v2"
    "sync"
    "time"
)

// refreshRateLimiter is a token-bucket limiter with per-second refill.
// It is checked before spawning a refresh goroutine. When no token is
// available, the caller defers the refresh by re-scheduling with jitter.
//
// Uses a mutex rather than atomics because this is a background-path
// limiter (at most refresh_max_rps calls/s per route), not a hot-path
// function. The admin rate limiter (admin/server.go:436) uses a
// channel+goroutine pattern; we use a mutex here to avoid spawning a
// background goroutine per route.
type refreshRateLimiter struct {
    mu      sync.Mutex
    tokens  int64
    max     int64
    lastNs  int64 // unix nano of last refill
}

func newRefreshRateLimiter(rps int) *refreshRateLimiter {
    return &refreshRateLimiter{
        tokens: int64(rps),
        max:    int64(rps),
        lastNs: time.Now().UnixNano(),
    }
}

// Allow returns true if a token is available. It refills the bucket
// based on elapsed time since the last call.
func (r *refreshRateLimiter) Allow(now time.Time) bool {
    r.mu.Lock()
    defer r.mu.Unlock()

    elapsed := now.UnixNano() - r.lastNs
    if elapsed > 0 {
        refill := r.max * elapsed / int64(time.Second)
        if refill > 0 {
            r.tokens = min(r.tokens+refill, r.max)
            r.lastNs = now.UnixNano()
        }
    }
    if r.tokens <= 0 {
        return false
    }
    r.tokens--
    return true
}
```

Uses `math/rand/v2` (Go 1.22+, safe for concurrent use, per-P random) for
jitter (identified in Linus review).

**`triggerBgRefresh`** (`handler.go:480`):

After the freshness check and before acquiring `refreshSem`:

```go
if h.refreshLimiter != nil && !h.refreshLimiter.Allow(time.Now()) {
    delay := time.Duration(100+rand.IntN(400)) * time.Millisecond
    h.scheduler.Schedule(key, time.Now().Add(delay))
    h.refreshMetrics.IncSkips("rate_limited")
    return
}
```

### 6.5 Interaction with the scheduler

Deferred entries are re-inserted into the min-heap with a near-future
`refreshAt`. The drainer goroutine pops them again after the jitter delay.
If the rate limiter still has no tokens, they are deferred again. This creates
a natural backpressure loop: the refresh rate converges to the configured cap.

**Backlog bound:** With rate-limited deferral, the heap can grow with deferred
entries. In the worst case (1M objects, 30s TTL, 100 RPS cap), the backlog is
1M entries. The heap is ~32 MB and the registry is ~200–450 MB. The periodic
compaction (every 60s) removes entries whose objects have been evicted or
expired, keeping the backlog bounded by the live object count.

### 6.6 Metrics

New skip reason `rate_limited` for `bouine_refresh_skips_total`.

## 7. Change 5: Reactive-first mode for high-cardinality routes

### 7.1 Problem

Every newly cached object gets one unconditional refresh cycle
(`isRefresh=false` bypasses the gate). For a route with 1M objects, that's 1M
conditional GETs in the first TTL window. Most of these objects will never be
accessed again.

### 7.2 Solution

Add a `refresh_reactive_first` config option. When enabled, new objects are
**not** scheduled for proactive refresh. Instead, they rely on the SWR
mechanism: if the object is accessed while stale (within the SWR window), a
background revalidation is triggered. The SWR-triggered refresh stores with
`isRefresh=true` (see Change 2 §4.4), so the popularity gate applies: only
objects with `staleHits >= refreshMinHits` are promoted to proactive refresh
for the next TTL window.

### 7.3 Config

New field on `RouteCache`:

```go
// RefreshReactiveFirst changes the refresh strategy from proactive to
// reactive for the initial TTL window. New objects are not scheduled
// for proactive refresh. Instead, they rely on stale-while-revalidate
// (SWR): if accessed while stale, a background revalidation refreshes
// the object, and the popularity gate decides whether to promote it
// to proactive refresh for subsequent windows.
//
// This eliminates the unconditional first refresh cycle, reducing
// origin traffic by up to 100% for one-shot objects on high-cardinality
// routes. Requires refresh_before_expiry: true and
// stale_while_revalidate > 0.
RefreshReactiveFirst bool `yaml:"refresh_reactive_first,omitempty" json:"refresh_reactive_first,omitempty"`
```

Validation: `refresh_reactive_first` requires `refresh_before_expiry: true`,
`stale_while_revalidate > 0`, and `refresh_min_hits > 0`. The popularity
gate is required because reactive-first mode is useless without it —
without the gate, every SWR-triggered refresh unconditionally schedules
proactive refresh, making the flag a no-op. The gate provides the
filtering that makes reactive-first meaningful.

### 7.4 Implementation

**`storeObject`** — the check goes **inside** the `refreshBeforeExpiry`
block, after the owner check and before the popularity gate. It must NOT
go before `store.Put` (which is the first line of the function):

```go
func (h *Handler) storeObject(ctx context.Context, key api.Key, obj *api.Object, r *http.Request, isRefresh bool, staleHits int64) {
    _ = h.store.Put(ctx, key, obj)           // always store first
    if h.refreshBeforeExpiry && obj.TTL >= minRefreshTTL {
        if IsNegativeCacheable(obj.StatusCode) { return }
        if h.ownerFn != nil {
            if _, isLocal := h.ownerFn(key); !isLocal { return }
        }
        // Reactive-first: skip proactive refresh for new objects.
        // The object is already stored (Put above). It will rely on
        // SWR if accessed while stale. The SWR-triggered refresh
        // (isRefresh=true) applies the popularity gate and promotes.
        if !isRefresh && h.refreshReactiveFirst { return }
        // Popularity gate ...
    }
}
```

The SWR path (`doBackgroundRevalidate`) now calls `storeObject`
with `isRefresh=true` (Change 2 §4.4), so the reactive-first check is
bypassed and the popularity gate applies.

### 7.4.1 Foreground revalidation interaction

The foreground `revalidate` function (`handler.go:992`) also calls
`storeObject` with `isRefresh=false`. With `refresh_reactive_first=true`,
the early return blocks proactive refresh scheduling for foreground
revalidation stores too. This is **correct and intended**:

- The client already paid the latency cost of a synchronous origin fetch.
- The object gets a fresh TTL. If it's accessed again within the new TTL,
  it'll be a hit.
- If it's accessed during SWR, the SWR path evaluates the popularity gate
  and promotes if warranted.
- Bootstrapping proactive refresh from a foreground revalidation would
  schedule a background fetch for an object that may never be accessed
  again — the same problem reactive-first is solving.

### 7.5 SWR dependency

Reactive-first mode depends on SWR being functional. If
`stale_while_revalidate` is not set, stale objects are not served and the SWR
path is never triggered. The validation enforces this dependency.

When SWR fires, the client receives the stale object immediately (with
`Warning: 110`) and a background revalidation refreshes the object. The
`staleHits` passed to `storeObject` is read from `store.WindowHits(key)` in
`triggerBgRevalidate` / `doBackgroundRevalidate` before the store. If
`staleHits >= refreshMinHits`, proactive refresh is scheduled for the next
TTL window. If not, the object is left to expire.

### 7.6 Traffic reduction estimate

For a route with 1M objects, 30s TTL, and 90% one-shot ratio:
- **Current:** 1M unconditional refreshes in first 30s window = 33k RPS
- **Reactive-first:** 0 proactive refreshes for one-shot objects. Only the
  100k objects that are re-accessed during SWR trigger a background
  revalidation = ~3.3k RPS (90% reduction).
- Combined with per-window hits (Change 1+2): only objects with
  `staleHits >= refreshMinHits` in the SWR-triggered window get proactive
  refresh for the next cycle.

### 7.7 `doBackgroundRevalidate` must read `windowHits` before fetch

Currently `doBackgroundRevalidate` (`handler.go:1056`) does not read
`windowHits`. It must read `staleHits := h.store.WindowHits(key)` **before**
the `collapsedFetch` call (line 1060), not after. Hits may accumulate during
the origin round-trip, and we want the pre-fetch count from the expiring
window. The `staleHits` value is passed to `storeObject` for the popularity
gate. The `stale` object passed to `doBackgroundRevalidate` is the pre-refresh
object; its `windowHits` is the current window's count.

## 8. Implementation Phases

### Phase A: Per-window hit counter + Hits reset (Changes 1+2)

These must ship together — the gate switches from `Object.Hits` to
`windowHits`, and `Object.Hits` resets on refresh.

**Files:**
- `internal/storage/hot.go` — add `windowHits` to `hotEntry`, increment on
  both Get paths, add `WindowHits` method
- `internal/storage/store.go` — add `WindowHits` to
  the `Store` interface
- `internal/storage/tiered.go` — delegate `WindowHits` to hot store; warm
  store returns 0
- `internal/cache/handler.go` — `storeObject` signature change (add
  `staleHits int64`), `refreshFrom304` reset, `doBackgroundRefresh` reset,
  `triggerBgRefresh` reads `windowHits`, `doBackgroundRevalidate` reads
  `windowHits` and changes to `isRefresh=true`, all call sites updated
- `internal/cache/handler_test.go` — update tests
- `internal/cache/handler_bench_test.go` — compile-time `Store` assertion
  exercises `WindowHits`
- `pkg/api/storage.go` — no changes (Object.Hits field stays, semantics change)

**Handler struct:** No new fields in Phase A (the `staleHits` parameter is
passed through call chains, not stored on the Handler).

**Tests:**
- Verify `windowHits` increments on every Get (fast and slow path)
- Verify `windowHits` resets to 0 on refresh store
- Verify `Object.Hits` resets to 0 on 304 and 200 refresh
- Verify popularity gate uses `staleHits` (from `windowHits`), not `obj.Hits`
- Verify object that was popular then stopped being accessed is gated out
- Verify persist cycles decrement correctly with per-window hits
- Verify SWR-triggered refresh now goes through the popularity gate
- Verify SWR with gate disabled (default) still unconditionally schedules
- Benchmark: `BenchmarkHotGet_WithWindowHits` — verify < 1 ns/op added

### Phase B: Cost-weighted refresh score (Change 3)

**Files:**
- `internal/config/config.go` — add `RefreshMinScore` field
- `internal/config/loader.go` — validation
- `internal/cache/handler.go` — `shouldRefresh` function, gate in `storeObject`
- `internal/cache/handler_test.go` — score gate tests

**Handler struct:** Add `refreshMinScore int64` field, set during handler
construction from `RouteCache.RefreshMinScore`.

**Tests:**
- Verify large object with 1 hit passes score gate
- Verify small object with many hits fails score gate
- Verify both gates (hit-count + score) must pass
- Verify score gate is disabled when `refresh_min_score=0`
- Verify persist cycles work with the unified gate function

### Phase C: Per-route refresh rate limit (Change 4)

**Files:**
- `internal/config/config.go` — add `RefreshMaxRPS` field
- `internal/config/loader.go` — validation
- `internal/cache/rate_limiter.go` — new file
- `internal/cache/rate_limiter_test.go` — new file
- `internal/cache/handler.go` — rate limiter check in `triggerBgRefresh`
- `internal/cache/handler_test.go` — rate limiting tests
- `internal/observability/dataplane.go` — new skip reason

**Handler struct:** Add `refreshLimiter *refreshRateLimiter` field,
initialized during handler construction when `RefreshMaxRPS > 0`.

**Tests:**
- Verify rate limiter allows up to N tokens/s
- Verify excess refreshes are deferred with jitter
- Verify deferred refreshes are re-scheduled
- Verify no token leak across refill boundaries
- Verify concurrency safety (`-race` detector)
- Verify metrics: `rate_limited` skip counter

### Phase D: Reactive-first mode (Change 5)

**Files:**
- `internal/config/config.go` — add `RefreshReactiveFirst` field
- `internal/config/loader.go` — validation
- `internal/cache/handler.go` — early return in `storeObject`
- `internal/cache/handler_test.go` — reactive-first tests

**Handler struct:** Add `refreshReactiveFirst bool` field, set during
handler construction from `RouteCache.RefreshReactiveFirst`.

**Tests:**
- Verify new objects are not scheduled for proactive refresh
- Verify SWR-triggered refresh promotes popular objects to proactive
- Verify one-shot objects expire naturally without refresh
- Verify interaction with per-window hits and popularity gate
- Verify validation rejects `reactive_first` without SWR

### Phase E: Documentation and metrics

**Files:**
- `docs/decisions/0024-predictive-refresh-prioritization.md` — new ADR
- `docs/plans/refresh-prioritization.md` — this document (finalize)
- `internal/observability/dataplane.go` — new metrics if needed
- `internal/dashboard/` — dashboard tiles for refresh score distribution

## 9. Config summary

New fields on `RouteCache`:

| Field | Type | Default | Validation |
|-------|------|---------|------------|
| `refresh_min_score` | `int64` | 0 (disabled) | requires `refresh_before_expiry` + `refresh_min_hits > 0` |
| `refresh_max_rps` | `int` | 0 (unlimited) | requires `refresh_before_expiry`, 0 or 1–10000 |
| `refresh_reactive_first` | `bool` | false | requires `refresh_before_expiry` + `stale_while_revalidate > 0` + `refresh_min_hits > 0` |

No changes to existing fields. No breaking config changes.

## 10. Architecture compliance

### Layer dependencies

- Change 1 touches `internal/storage` (L2) — adds `windowHits` to `hotEntry`
  and one method (`WindowHits`) to the `Store` interface. L2 is a lower layer;
  L3 (`cache`) consuming its interface is the correct dependency direction.
- Changes 2–5 are in `internal/cache` (L3) and `internal/config` (L7). L3 → L7
  is allowed.
- No new dependencies on `internal/origin` (L4) or any other layer.

### Hit-path impact

- Change 1 adds one `atomic.Int64.Add` to the hot-tier Get fast path. This is
  a single locked instruction, < 1 ns on modern CPUs. Zero allocations.
  Benchmarks must prove this stays within the < 5 us p50 budget with zero
  allocs/op.
- The rate limiter is only checked in `triggerBgRefresh` (scheduler callback),
  not on the hit path.
- The score gate and reactive-first check are in `storeObject` (miss path),
  not on the hit path.

### Memory budget

- `windowHits`: 8 bytes per `hotEntry`. With 1M hot entries, that's 8 MB.
  `hotEntry` grows from ~40 bytes to ~48 bytes — a 20% increase per entry,
  but the entries are pointer-sized in the shard map, so the map overhead
  dominates. Net memory increase is negligible.
- Rate limiter: 32 bytes per route (one `refreshRateLimiter` struct:
  `sync.Mutex` + 3 int64 fields).
- No per-`Object` memory increase.
- Scheduler heap and registry size are bounded by existing compaction.

### Concurrency

- `windowHits` uses `atomic.Int64` — no mutex contention on the hit path.
  Increment on the fast path is done under RLock (same as today's `e.obj`
  pointer read). The atomic add is safe without the write lock because
  `atomic.Int64` is word-aligned within `hotEntry`.
- `refreshRateLimiter` uses `sync.Mutex` for refill+acquire atomicity.
  Not on the hot path — called at most `refresh_max_rps` times/s per route
  from the scheduler drainer.
- All other changes use existing synchronization (registry mutex, scheduler
  mutex).

### Store interface change

Adding `WindowHits` to the `Store` interface is a
breaking change for any out-of-tree implementations. Since `Store` is in
`internal/storage`, this is acceptable — there are no out-of-tree
implementations. The `TieredStore` and mock stores in tests must be updated.

## 11. Risks and mitigations

### 11.1 Per-window reset changes operator-visible behavior

**Risk:** Operators who set `refresh_min_hits: 5` and see objects with
`Hits=1000` will notice that after the change, the gate uses `windowHits`
(per-window) and `Object.Hits` shows small numbers (SIEVE signal). Dashboard
tiles that show "total hits" per object will show lower numbers.

**Mitigation:** Document the change in release notes. Surface `windowHits` in
the admin API. Cumulative hit counts remain in Prometheus. The per-window
number is more actionable for tuning `refresh_min_hits`.

### 11.2 `refresh_min_hits > 1` now works correctly

**Risk:** Before this change, `refresh_min_hits > 1` was silently impossible
to satisfy (Hits could only reach 1 per SIEVE cycle). After the change, it
works as documented. Operators who set `refresh_min_hits: 5` thinking it
worked will see a behavioral change: objects now actually need 5 hits per
window to be re-scheduled.

**Mitigation:** This is a bug fix. The previous behavior was that the gate
was effectively `min_hits: 1` for all values > 0. The fix makes the
configured value meaningful. Document this in the release notes.

### 11.3 Score gate threshold is hard to tune

**Risk:** `refresh_min_score` is a product of hits × bytes. Operators need to
understand the interaction to set a meaningful threshold.

**Mitigation:** Document example configurations in the runbook. The dashboard
should show the score distribution per route. The score gate is optional
(default 0 = disabled) and additive to the hit-count gate.

### 11.4 Rate limiter backlog

**Risk:** With a low RPS cap and many objects due for refresh, the scheduler
heap grows with deferred entries.

**Mitigation:** The existing compaction (every 60s) removes dead entries. The
backlog is bounded by the live object count, which is bounded by the hot-tier
size limit. With 1M live objects, the heap is ~32 MB and the registry is
~200–450 MB — within the existing budget.

### 11.5 SWR behavioral change for all routes

**Risk:** Changing SWR stores from `isRefresh=false` to `isRefresh=true`
affects all routes, not just reactive-first ones. Routes with
`refresh_min_hits > 0` will see SWR-triggered refreshes subject to the gate.

**Mitigation:** Routes without `refresh_min_hits` (default, gate disabled) are
unaffected — `isRefresh=true` has no effect when the gate is disabled. Routes
with the gate get stricter behavior, which is the intended design. Document
this in release notes.

### 11.6 Interaction with cluster mode

**Risk:** In strong cluster mode, only the key owner schedules refreshes. The
rate limiter and reactive-first mode apply per-node.

**Mitigation:** This is correct — each node limits its own origin traffic. The
total refresh rate is the sum across nodes, bounded by
`refresh_max_rps × node_count`. Operators set per-node RPS to
`total_budget / node_count`.

## 12. Test plan

### Unit tests

- `TestHotGet_WindowHitsIncrements` — verify `windowHits` increments on every
  Get (both fast and slow path)
- `TestHotGet_WindowHitsReset` — verify `windowHits` resets to 0 after
  `Put` (new `hotEntry` created)
- `TestStoreObject_ResetsHitsOn304` — verify `Object.Hits = 0` after 304
- `TestStoreObject_ResetsHitsOn200` — verify `Object.Hits = 0` after 200
- `TestStoreObject_StaleHitsGate` — verify gate uses `staleHits` from
  `windowHits`, not `obj.Hits`
- `TestStoreObject_MinHitsGTPossible` — verify `refresh_min_hits: 5` is
  satisfiable (5 hits in a window passes the gate)
- `TestStoreObject_ScoreGate` — verify score gate with various sizes/hits
- `TestStoreObject_ScoreGateDisabled` — verify score=0 skips the gate
- `TestStoreObject_ReactiveFirst` — verify new objects not scheduled
- `TestStoreObject_ReactiveFirstPromotion` — verify SWR promotes to proactive
- `TestStoreObject_SWRWithGateDisabled` — verify SWR still schedules
  unconditionally when gate is disabled (backward compat)
- `TestRateLimiter_Allows` — verify N tokens per second
- `TestRateLimiter_Defer` — verify excess calls are denied
- `TestRateLimiter_Refill` — verify token refill over time
- `TestRateLimiter_NoLeak` — verify no token leak across refill boundaries
- `TestRateLimiter_Concurrent` — verify no data race under `-race`

### Integration tests

- `TestRefresh_PerWindowHitsEndToEnd` — fill cache, access some objects N
  times, let TTL expire, verify only objects with N+ hits are refreshed
- `TestRefresh_RateLimitEndToEnd` — fill cache with many objects, verify
  refresh rate does not exceed configured RPS
- `TestRefresh_ReactiveFirstEndToEnd` — fill cache, don't re-access, verify
  no proactive refresh; re-access during SWR, verify promotion to proactive
- `TestRefresh_ScoreGateEndToEnd` — fill cache with mixed object sizes,
  verify only high-score objects are refreshed

### Benchmark

- `BenchmarkHotGet_WithWindowHits` — verify < 1 ns/op added vs baseline
- `BenchmarkStoreObject_WithScoreGate` — verify no hit-path regression
- `BenchmarkRateLimiter_Allow` — verify < 10 ns/op
- `BenchmarkTriggerBgRefresh_WithRateLimit` — verify rate check adds < 100 ns

### Conformance

- Run `make conformance` — verify no cache-tests regression. The refresh
  changes are handler-level and do not affect RFC 9111 freshness semantics.

## 13. Files changed

| File | Change |
|------|--------|
| `internal/storage/hot.go` | Add `windowHits` to `hotEntry`, increment on both Get paths, add `WindowHits` method |
| `internal/storage/store.go` | Add `WindowHits` to `Store` interface |
| `internal/storage/tiered.go` | Delegate `WindowHits` to hot store |
| `internal/cache/handler.go` | `storeObject` signature + `shouldRefresh` + score gate + reactive-first + rate limit check + `refreshFrom304` reset + `doBackgroundRefresh` reset + SWR `isRefresh` change + `triggerBgRevalidate` reads `windowHits` |
| `internal/cache/rate_limiter.go` | New file: token-bucket rate limiter |
| `internal/cache/rate_limiter_test.go` | New file: rate limiter tests |
| `internal/config/config.go` | 3 new fields on `RouteCache` |
| `internal/config/loader.go` | Validation for new fields |
| `internal/observability/dataplane.go` | New skip reasons |
| `internal/cache/handler_test.go` | Updated and new tests |
| `internal/storage/hot_test.go` | `windowHits` tests |
| `docs/decisions/0024-predictive-refresh-prioritization.md` | New ADR |
| `docs/plans/refresh-prioritization.md` | This document (finalize) |

## 14. Relationship to existing ADRs

- **ADR-0016** (refresh-before-expiry): This plan does not change the
  scheduler, registry, or background refresh mechanism. It changes the
  *decision* of whether to schedule and the *rate* at which refreshes fire.
- **ADR-0021** (popularity gate): This plan fixes two bugs in the gate:
  (1) `Object.Hits` is cumulative, not per-window; (2) `Object.Hits` only
  increments once per SIEVE cycle, making `min_hits > 1` impossible. The fix
  adds a true per-window hit counter (`windowHits`) and switches the gate to
  use it. The gate's semantics change from "was this ever popular" to "is
  this popular in the current window, with a real hit count."
- **ADR-0022** (persist cycles): This plan makes persist cycles behave as
  documented. With per-window `windowHits`, the persist counter reaches zero
  in a bounded number of TTL windows for dead objects, instead of being
  indefinitely reset by stale cumulative hits.

## 15. Revision history

- **Revision 1 (2026-07-11):** Initial draft. Four changes: per-window hit
  reset, cost-weighted score, rate limit, reactive-first mode.
- **Revision 2 (2026-07-11):** Amended after Linus review. Key changes:
  - **BLOCKER fix:** `Object.Hits` only increments once per SIEVE cycle
    (slow path only). Added Change 1: new `windowHits` counter on `hotEntry`
    that increments on every Get. This is a storage-layer change (L2), not
    handler-only as originally claimed.
  - **BLOCKER fix:** SWR path calls `storeObject` with `isRefresh=false`,
    not `true` as claimed. Documented the SWR behavioral change explicitly
    (§4.4) and wired it into the plan.
  - **bug fix:** Factored the hit-count and score gates into a single
    `shouldRefresh` function to eliminate duplicated persist/unregister
    blocks.
  - **bug fix:** Switched to `math/rand/v2` for jitter (concurrency-safe).
  - **taste fix:** Documented SIEVE eviction interaction with `windowHits`
    (they are now independent).
  - **nit fix:** ADR number changed from 0023 to 0024 (0023 is taken by
    warm-tier-eviction).
  - **nit fix:** Renamed `prevHits` to `staleHits` for clarity.
  - Removed false claim that all changes use "already stored data" — Change
    1 adds new per-entry state (`windowHits`).
- **Revision 3 (2026-07-11):** Amended after second Linus review:
  - **bug fix:** Removed `ResetWindowHits` — `Put` already creates a new
    `hotEntry` with `windowHits = 0` (zero value), making explicit reset
    dead code. Removed from `Store` interface, `HotStore`, `TieredStore`,
    and the `storeObject` call.
  - Clarified that `staleHits` must be read in `triggerBgRefresh`
    immediately after `Get` to minimize the race window.
  - Clarified that `doBackgroundRevalidate` reads `windowHits` before the
    `collapsedFetch` call (pre-fetch count from the expiring window).
- **Revision 4 (2026-07-11):** Amended after third Linus review:
  - **bug fix:** Added missing 8th call site of `storeObject` (POST/PUT
    invalidation re-cache at `handler.go:1298`).
  - **bug fix:** Removed unused `key` parameter from `shouldRefresh`
    (would fail `unparam` linter).
  - **bug fix:** Documented foreground revalidation interaction with
    reactive-first mode (§7.4.1) — the early return correctly blocks
    proactive refresh for foreground revalidation stores too.
  - **nit fix:** Corrected "Two new config fields" to "One" for Change 4.
  - **nit fix:** Updated status header to revision 3.
- **Revision 5 (2026-07-11):** Amended after fourth Linus review:
  - **bug fix:** Rate limiter switched from atomic CAS to `sync.Mutex` —
    the CAS loop had refill race conditions (dropped refills + token leaks
    under concurrent access). Mutex is correct for a background-path
    limiter (at most `refresh_max_rps` calls/s per route).
  - **bug fix:** Fixed `shouldRefresh` doc comment — it described the
    caller's persist-cycle logic, not the function's behavior.
  - **bug fix:** Added `handler_bench_test.go` to Phase A files (compile-time
    `Store` assertion exercises `WindowHits`).
  - **taste fix:** Added note explaining why a mutex-based limiter is chosen
    over the existing channel-based pattern in `admin/server.go`.
- **Revision 6 (2026-07-11):** Amended after fifth Linus review:
  - **bug fix:** Updated §10 Concurrency section — was still describing the
    old atomic rate limiter ("no mutex") after the switch to `sync.Mutex`.
  - **bug fix:** Updated §10 Memory budget — rate limiter is 32 bytes
    (Mutex + 3 int64), not 24 bytes (3 atomic.Int64).
  - **nit fix:** Updated §10 Layer dependencies — "two methods" → "one
    method (`WindowHits`)" after `ResetWindowHits` was removed.
- **Revision 7 (2026-07-11):** Amended after sixth Linus review:
  - **bug fix:** Clarified reactive-first check placement — must go
    inside the `refreshBeforeExpiry` block, after `store.Put` and the
    owner check, not at the top of the function (would skip the Put).
  - **bug fix:** Added `refresh_min_hits > 0` to `refresh_reactive_first`
    validation — reactive-first is useless without the popularity gate.
  - **bug fix:** Added Handler struct field notes to each phase
    (`refreshMinScore`, `refreshLimiter`, `refreshReactiveFirst`).
  - Acknowledged `windowHits` fast-path race with concurrent `Put` and
    explained why the lost update is semantically correct.
- **Revision 8 (2026-07-11):** Amended after seventh Linus review:
  - **bug fix:** Fixed cross-reference — SWR call-site rationale pointed
    to "Change 5 §6.3" but should point to "§4.4".
  - **bug fix:** Added note that SWR call sites read `staleHits` via
    `h.store.WindowHits(key)` in `doBackgroundRevalidate` before
    `collapsedFetch` — the value doesn't appear by magic.
  - **nit fix:** Corrected `refresh_max_rps` config summary range to
    "0 (unlimited) or 1–10000" to match the validation semantics.
- **Revision 9 (2026-07-11):** Amended after eighth Linus review:
  - **nit fix:** Fixed §6.3 validation text for `refresh_max_rps` — was
    still "Range 1–10000", now "0 (unlimited, default) or 1–10000" to
    match §9 config summary.
  - Round 8 verdict: **Solid**. Plan is ready for implementation.
