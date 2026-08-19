# Plan: Refresh-Before-Expiry (Proactive Background Refresh)

**Status:** Draft
**Scope:** Per-route opt-in background conditional revalidation that fires
*before* an object's TTL expires, keeping the hot cache perpetually fresh
and reducing origin traffic to lightweight 304s.
**Roadmap ref:** `docs/architecture.md §1.2` backlog — new entry. Supersedes the always-warm
concept (backlog #18). Always-warm handled eviction; refresh-before-expiry
handles the common case (TTL expiry) and is the primary tool for
"almost zero origin traffic."

---

## 1. Problem Statement

When a cached object's TTL expires, the next client request is a miss or
a revalidation round-trip to origin. For latency-critical routes with
high traffic, this creates periodic origin fetches and client-visible
latency spikes at the moment of expiry.

SWR (`stale-while-revalidate`) mitigates this by serving stale content
immediately and background-revalidating. But SWR still sends a full
client request through the cache pipeline, and the object transitions
through a stale state before being refreshed.

**Refresh-before-expiry** eliminates the expiry event entirely: a
background timer fires at `TTL − margin`, performs a conditional
revalidation (304-capable), and updates the object's TTL in place. The
object never expires, never enters the stale window, and clients always
see cache hits. Origin traffic is reduced to lightweight 304 responses
(no body transfer).

**Comparison to always-warm:** Always-warm fires *after* eviction (the
object is already gone). Refresh-before-expiry fires *before* expiry
(the object is still fresh). Always-warm is a safety net for memory
pressure; refresh-before-expiry is the primary freshness mechanism.

Varnish equivalent: `beresp.ttl` + scheduled bereq via VCL. nginx
equivalent: `proxy_cache_background_update on` +
`proxy_cache_background_update` (TTL-based). Squid: `background-refresh`.

---

## 2. Design Overview

```
 Put (cache fill)
     │
     ▼
 storeAndReplicate ──► refreshScheduler.Schedule(key, obj)
     │                        │
     │                        ▼
     │                 min-heap insert
     │                 (refreshAt = StoredAt + TTL − margin)
     │                        │
     ▼                        ▼
 (object stored)     scheduler goroutine
                            │
                     sleep until heap.top.refreshAt
                            │
                            ▼
                     pop key from heap
                            │
                     ┌──────┴───────┐
                     │ store.Get    │
                     │ key still    │
                     │ there?       │
                     │              │
                    YES            NO
                     │              │
                     │         (skip — evicted/
                     │          banned/deleted)
                     ▼
                     obj.Fresh(now)?
                     │
                    YES            NO
                     │              │
                     │         (skip — already
                     │          stale, SWR/miss
                     │          path handles it)
                     ▼
                     triggerBgRefresh(key, obj)
                     │
                     ▼
                     refreshRegistry.Lookup(key)
                     → reconstruct request
                     → ConditionalHeaders (ETag/LM)
                     → collapsedFetch (singleflight)
                     │
                     ├── 304 → refreshFrom304 → storeAndReplicate
                     │        → re-schedule
                     ├── 200 → buildObject → storeAndReplicate
                     │        → re-schedule
                     └── err → re-schedule with backoff
                              (object still fresh, retry later)
```

### 2.1 Key design decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | Min-heap scheduler, not per-object timers | `time.AfterFunc` creates a goroutine per timer — unacceptable for 1M objects. A min-heap with a single drainer goroutine uses O(log n) insert on Put (not the hit path) and O(1) peek. One goroutine total, regardless of object count. `container/heap` is stdlib (pre-approved) but not used elsewhere in bouine — this introduces a new pattern. A benchmark in Phase A must prove the O(log n) insert matters vs a sorted slice for the expected entry count. |
| D2 | Refresh registry for URL/headers (not on `api.Object`) | The cache key is an irreversible xxhash64. To reconstruct the origin request we need the URL, method, and Vary-relevant headers. Storing these on `api.Object` changes the public API, increases per-object memory for all objects, and serializes to warm tier. A handler-side registry is consistent with the existing `variantSets` pattern and only costs memory for routes where refresh is enabled. |
| D3 | Store only Vary-relevant request headers in registry | A request with 10 headers costs ~1.3 KB to store. The Vary header lists 0–3 headers that affect the response. Storing only those (plus `Accept-Encoding` for content negotiation) reduces per-entry cost to ~200–400 B. |
| D4 | Lazy cancellation + periodic compaction (no cancel map) | On pop, the scheduler calls `store.Get(key)`. If the object is gone (evicted, banned, deleted) or already stale (refreshed by a client request, so `StoredAt` advanced), the heap entry is silently dropped. No cancel map, no generation counters. The store lookup is O(1) (sharded map). **However**, entries that are evicted long before their `refreshAt` sit in the heap until the drainer reaches them. To bound dead entry memory, the scheduler runs a periodic compaction pass every 60s: pops all entries with `refreshAt <= now + compactionWindow` (default 5s), re-inserts only those whose `store.Get` returns a live, fresh object. This bounds dead entries to O(refreshRate × compactionInterval). |
| D5 | Re-use `collapsedFetch` (singleflight) | If a client request arrives for the same key while the background refresh is in flight, they share the same fetch via singleflight. No duplicate origin requests. |
| D6 | Separate per-Handler `refreshSem` (not package-level `bgRevalSem`) | Background refresh should not compete with SWR revalidation for semaphore slots. `refreshSem` (default 8) is a per-Handler field, distinct from `bgRevalSem` (package-level, 256, shared across all handlers) and `fetchSem` (per-Handler, 32 foreground). Total worst-case concurrent origin connections: 32 + 256 + 8 = 296. In practice refresh fetches are 304s (no body), so memory pressure is minimal. **Do not consolidate `refreshSem` into a package-level var** — a high-traffic refresh route would starve a low-traffic refresh route. Unlike `bgRevalSem` (shared, best-effort drop), `refreshSem` is per-route to enforce per-route bounds. |
| D7 | Minimum TTL threshold: 5s | Objects with TTL < 5s are not scheduled. The refresh window (10% of 5s = 0.5s) is too tight for a network round-trip. These objects fall through to normal SWR/miss handling. |
| D8 | No refresh on Ban / Delete / Put-replace | The scheduler lazily detects these via `store.Get` returning nil. No explicit notification needed. |
| D9 | No refresh of negative-cached objects (404s) | Re-fetching 404s proactively is surprising and wastes origin capacity. Negative-cached objects are never scheduled. |
| D10 | Cluster-aware: only key owner refreshes (strong mode) | Same as the always-warm design. In strong mode, only the consistent-hash owner schedules refreshes. In eventual mode, each node refreshes independently. In full mode, only the original-storing node refreshes. |
| D11 | `refreshAt` computed as `StoredAt + TTL − margin` | Uses the same `StoredAt` and `TTL` fields that `Fresh()` uses. After a 304 refresh, `refreshFrom304` updates `StoredAt = now` and recomputes `TTL`, so re-scheduling with the new `StoredAt + TTL − margin` is correct. |

### 2.2 Refresh scheduler

New file: `internal/cache/scheduler.go`.

```go
type heapEntry struct {
    key       api.Key
    refreshAt int64 // unix nano
    index     int   // heap index (container/heap bookkeeping)
}

type refreshHeap []*heapEntry

type RefreshScheduler struct {
    mu    sync.Mutex
    heap  refreshHeap
    done  chan struct{}
    wg    sync.WaitGroup
    ready chan api.Key // signals the drainer to wake early
}
```

**Schedule(key, obj):** Called from `storeAndReplicate` when
refresh-before-expiry is enabled. Computes `refreshAt = obj.StoredAt +
obj.TTL − margin`. Inserts into the min-heap under `mu`. If the new entry
is the new heap top (earliest refresh), signals `ready` to wake the
drainer.

**Drainer goroutine:** Single goroutine, started in `NewHandler`,
terminated by `Close`:

```go
func (s *RefreshScheduler) run(handler *Handler) {
    defer s.wg.Done()
    for {
        s.mu.Lock()
        if s.heap.Len() == 0 {
            s.mu.Unlock()
            select {
            case <-s.done:
                return
            case <-s.ready:
                continue
            }
        }
        top := s.heap[0]
        delay := time.Until(time.Unix(0, top.refreshAt))
        s.mu.Unlock()

        if delay > 0 {
            timer := time.NewTimer(delay)
            select {
            case <-s.done:
                timer.Stop()
                return
            case <-s.ready:
                timer.Stop()
                continue // new top may be earlier
            case <-timer.C:
            }
        }

        s.mu.Lock()
        if s.heap.Len() == 0 {
            s.mu.Unlock()
            continue
        }
        entry := heap.Pop(&s.heap).(*heapEntry)
        s.mu.Unlock()

        handler.triggerBgRefresh(entry.key)
    }
}
```

**Memory per heap entry:** `api.Key` (8 B) + `refreshAt` (8 B) + `index`
(8 B) + pointer (8 B) = 32 B. For 10 000 objects on a refresh-enabled
route: 320 KB. For 100 000: 3.2 MB. For 1 000 000: 32 MB.

**Dead entry bounding:** Objects evicted long before their `refreshAt`
leave dead entries in the heap. Without compaction, 100k objects stored
with 24h TTL then evicted within 1h would leave 3.2 MB of dead entries
for 21+ hours. The periodic compaction pass (every 60s) pops entries
in the near-future window and re-inserts only live ones, bounding dead
entries to O(refreshRate × 60s). For a route refreshing 100 objects/s,
that's at most ~6000 dead entries (~192 KB) between compaction passes.

**CPU:** O(log n) insert on Put (miss path, not hit path). For 100k
entries, ~17 comparisons of int64. ~50–80 ns. The drainer goroutine
sleeps until the next refresh — CPU is proportional to refresh rate,
not entry count. Compaction pass runs every 60s, touches only entries
in the near-future window — O(refreshRate × compactionWindow), not
O(heap size).

**Hit path impact:** Zero. The heap is not touched on `Get`. The
registry is not touched on `Get`. No new code on the hot path.

### 2.3 Refresh registry

New file: `internal/cache/refresh_registry.go`.

```go
type refreshEntry struct {
    url    string       // compact: "https://host/path?query"
    method string       // GET or HEAD
    header http.Header  // snapshot of Vary-relevant request headers only
}

type refreshRegistry struct {
    mu      sync.Mutex
    entries map[api.Key]*refreshEntry
}
```

**Registration:** `storeAndReplicate` is the chokepoint for all cacheable
stores, but its signature is `(ctx, key, obj)` — it does not have the
`*http.Request`. There are 6 call sites, all of which have `r` in scope:
`revalidate` (line 662), `doBackgroundRevalidate` (lines 734, 746),
`writeAndMaybeStore` (lines 796, 799), `maybeStorePostResponse` (line 963).

**Approach:** Change `storeAndReplicate` signature to
`(ctx, key, obj, r *http.Request)`. This forces every store path to
consider registration. When `refreshBeforeExpiry` is enabled,
`storeAndReplicate` calls `h.refreshRegistry.Register(key, r,
varyHeaders)` and `h.scheduler.Schedule(key, obj, h.refreshMargin)`
after `store.Put`. When disabled, these are no-ops (nil checks).

`Register` extracts the Vary header names from the response Object's
`Header` field, clones only those request headers (plus
`Accept-Encoding`), and stores a compact URL string. Storing a reference
to `r.Header` would race with HTTP server request pooling — `Register`
must clone.

**Unregistration:** On explicit `Delete` (purge) and
`invalidateAndProxy` success, the handler calls
`h.refreshRegistry.Unregister(key)`. The scheduler also calls
`Unregister` when `store.Get` returns nil on pop (evicted/banned).

**Eviction cleanup:** When the scheduler pops a key and `store.Get`
returns nil (evicted/banned/deleted), the scheduler calls
`h.refreshRegistry.Unregister(key)` to clean up the stale entry.

**Memory per entry:** URL string ~50–100 B + method ~4 B + Vary-relevant
headers (0–3 headers, ~100–300 B) + map overhead ~50 B = ~200–450 B.
For 10 000 entries: ~2–4.5 MB. For 100 000: ~20–45 MB.

**Combined memory (heap + registry):** ~232–482 B per entry. For 10 000
objects: ~2.3–4.8 MB. For 100 000: ~23–48 MB. Only for refresh-enabled
routes. Acceptable for the "almost zero origin traffic" use case.

### 2.4 Background refresh

```go
func (h *Handler) triggerBgRefresh(key api.Key) {
    // Reject if handler is shutting down.
    select {
    case <-h.done:
        return
    default:
    }

    // Check if object is still in store and still fresh.
    // Use a short timeout — if the warm tier disk read is slow,
    // skip the refresh (object will expire and client path handles it).
    getCtx, getCancel := context.WithTimeout(context.Background(), 5*time.Second)
    obj, _, err := h.store.Get(getCtx, key)
    getCancel()
    if err != nil || obj == nil {
        h.refreshRegistry.Unregister(key)
        return
    }
    if !obj.Fresh(time.Now()) {
        return // already stale — SWR/miss path handles it
    }

    select {
    case h.refreshSem <- struct{}{}:
    default:
        return // semaphore full — object will expire and client will refresh
    }

    h.refreshWg.Add(1)
    go func() {
        defer func() {
            h.refreshWg.Done()
            <-h.refreshSem
        }()

        ctx, cancel := context.WithTimeout(
            context.WithoutCancel(context.Background()),
            h.refreshTimeout,
        )
        defer cancel()

        h.doBackgroundRefresh(ctx, key, obj)
    }()
}

func (h *Handler) doBackgroundRefresh(ctx context.Context, key api.Key, stale *api.Object) {
    entry := h.refreshRegistry.Lookup(key)
    if entry == nil {
        return
    }

    u, err := url.Parse(entry.url)
    if err != nil {
        h.refreshRegistry.Unregister(key)
        return
    }

    req := &http.Request{
        Method: entry.method,
        URL:    u,
        Header: entry.header.Clone(),
        Host:   u.Host,
    }
    req = req.WithContext(ctx)
    ConditionalHeaders(req, stale)

    res := h.collapsedFetch(req, key)
    if res.Err != nil {
        // Object is still fresh. Re-schedule with backoff:
        // refreshAt = now + min(refreshMargin, remainingTTL / 2)
        remaining := time.Until(stale.StoredAt.Add(stale.TTL))
        if remaining <= 0 {
            return // already expired — SWR/miss path handles it
        }
        delay := min(h.refreshMargin, remaining/2)
        if delay < time.Second {
            delay = time.Second
        }
        h.scheduler.ScheduleAt(key, time.Now().Add(delay))
        return
    }

    if res.StatusCode == http.StatusNotModified {
        refreshed := h.refreshFrom304(stale, res)
        h.storeAndReplicate(ctx, key, refreshed)
        // storeAndReplicate re-schedules via Schedule (new StoredAt + TTL).
        return
    }

    if IsCacheableWithDefault(res.StatusCode, req.Header, res.Header, h.negativeTTL, h.defaultTTL) {
        if !h.allowSetCookie && res.Header.Get(header.SetCookie) != "" {
            return
        }
        if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
            return
        }
        obj := buildObject(key, req, res, h.negativeTTL, h.defaultTTL, h.overrideTTL,
            h.defaultSWR, h.defaultSIE, h.jitterPercent, h.excludeHeaders)
        h.storeAndReplicate(ctx, key, obj)
        // storeAndReplicate re-schedules via Schedule.
    }
}
```

**Key detail:** `storeAndReplicate` (now with signature
`(ctx, key, obj, r *http.Request)`) is the chokepoint. When
refresh-before-expiry is enabled, it calls `h.refreshRegistry.Register`
and `h.scheduler.Schedule(key, obj, h.refreshMargin)` after `store.Put`.
This means both the initial cache fill AND the refresh-304/200 re-store
automatically re-schedule the next refresh. No manual re-scheduling
needed in `doBackgroundRefresh` for the success path.

**No capacity check needed:** Unlike always-warm (which checked if the
hot tier was near full before re-fetching), refresh-before-expiry does
not need a capacity check. The object is still in the store when the
refresh fires. The refresh updates the object in place via `store.Put`,
which may trigger eviction of *other* entries, but not the refreshed
object itself (it was just accessed, so SIEVE marks it visited).

**Error path re-scheduling:** On fetch error, the object is still fresh
(origin was unreachable but TTL hasn't expired). The scheduler re-inserts
the key with a backoff delay: `min(refreshMargin, remainingTTL / 2)`,
minimum 1s. This retries the refresh before the object expires. If the
origin remains unreachable, the object eventually expires and falls
through to SWR/SIE/StayinAlive.

### 2.5 Shutdown lifecycle

`Handler` gains:
- `done chan struct{}` — closed by `Close`, signals all goroutines to stop.
- `refreshWg sync.WaitGroup` — tracks in-flight refresh goroutines.
- `scheduler *RefreshScheduler` — owns its own `done` and `wg`.

```go
func (h *Handler) Close(ctx context.Context) error {
    close(h.done)
    h.scheduler.Stop() // closes scheduler.done, waits for drainer

    done := make(chan struct{})
    go func() {
        h.refreshWg.Wait()
        close(done)
    }()

    select {
    case <-done:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

**Builder shutdown ordering:** The engine calls `handler.Close(ctx)`
**before** `store.Close(ctx)`. Otherwise, in-flight refresh goroutines
would call `store.Put` / `store.Get` on a closed store → panic.

### 2.6 Hot reload safety

Each `Handler` instance gets a unique `handlerID` (monotonic atomic
counter). The `RefreshScheduler` is **per-handler**, not shared across
routes. On hot reload:

1. The builder calls `oldHandler.Close(ctx)` — stops the drainer, drains
   in-flight refreshes.
2. The old scheduler's heap and the old registry are GC'd.
3. The new handler gets a fresh scheduler and registry.

This is simpler than the always-warm design (which needed a shared
`EvictionDispatcher` with generation tracking) because the scheduler is
not store-level — it's handler-level. No shared state to clean up.

---

## 3. Config Schema Changes

`internal/config/config.go` — `RouteCache` struct:

```go
// RefreshBeforeExpiry enables proactive background conditional
// revalidation. A background timer fires at TTL − margin, performing
// a conditional fetch (If-None-Match / If-Modified-Since). On 304,
// the TTL is refreshed in place — the object never expires and
// clients always see cache hits. On 200, the object is replaced.
//
// Requires caching to be enabled. Objects with TTL < 5s are not
// scheduled (too tight for a network round-trip). Negative-cached
// objects (404s) are not refreshed.
//
// Composes with SWR: if a refresh fails and the object expires, SWR
// serves stale content while the next client request triggers
// revalidation.
RefreshBeforeExpiry bool `yaml:"refresh_before_expiry,omitempty" json:"refresh_before_expiry,omitempty"`

// RefreshMarginPercent controls when the background refresh fires,
// as a percentage of TTL. Default 10 (fire at 90% of TTL). Range 1–50.
// For a 60s TTL at 10%, the refresh fires at 54s.
// For a 1h TTL at 10%, the refresh fires at 54m.
RefreshMarginPercent int `yaml:"refresh_margin_percent,omitempty" json:"refresh_margin_percent,omitempty"`

// RefreshConcurrency bounds the number of concurrent background
// refresh fetches per route. Default 8. Zero means use the default.
// Range 1–64.
RefreshConcurrency int `yaml:"refresh_concurrency,omitempty" json:"refresh_concurrency,omitempty"`

// RefreshTimeout is the maximum duration for a single background
// refresh fetch. Since there is no client request to inherit a
// context from, this timeout is the only protection against a hung
// origin. Default 30s. Range 5s–120s.
RefreshTimeout time.Duration `yaml:"refresh_timeout,omitempty" json:"refresh_timeout,omitempty"`
```

**Validation** (`internal/config/loader.go`):
- `RefreshBeforeExpiry` requires `Cache.Enabled != false`.
- `RefreshMarginPercent` must be 1–50.
- `RefreshConcurrency` must be 1–64.
- `RefreshTimeout` must be 5s–120s.
- `RefreshBeforeExpiry` requires `TTLDefault > 0` or `TTLOverride > 0`
  (otherwise objects with no origin freshness are not cacheable and
  there's nothing to refresh).

---

## 4. Storage Layer Changes

**None.** Unlike always-warm (which needed an eviction hook on
`HotStore`), refresh-before-expiry is entirely handler-level. The
scheduler uses `store.Get` to check if an object is still present. No
changes to `HotStore`, `TieredStore`, `Store` interface, or `api.Stats`.

This is a significant advantage: zero storage-layer risk, zero hit-path
risk, zero eviction-path risk.

---

## 5. Cache Layer Changes

### 5.1 New file: `internal/cache/scheduler.go`

- `RefreshScheduler` type (§2.2)
- `heapEntry` type, `refreshHeap` type (implements `container/heap.Interface`)
- `Schedule(key, obj, margin)` — computes `refreshAt`, inserts
- `ScheduleAt(key, time)` — inserts with explicit time (error backoff)
- `Stop()` — closes `done`, waits for drainer
- Implements `heap.Interface`: `Len`, `Less`, `Swap`, `Push`, `Pop`

### 5.2 New file: `internal/cache/refresh_registry.go`

- `refreshRegistry` type (§2.3)
- `refreshEntry` type
- Methods: `Register`, `Unregister`, `Lookup`, `Len`
- `Register` extracts Vary header names from the response, clones only
  those request headers + `Accept-Encoding`, stores compact URL string

### 5.3 `internal/cache/handler.go`

- `Handler` struct: add `refreshBeforeExpiry bool`,
  `refreshRegistry *refreshRegistry`, `scheduler *RefreshScheduler`,
  `refreshSem chan struct{}`, `refreshMargin time.Duration`,
  `refreshTimeout time.Duration`,
  `done chan struct{}`, `refreshWg sync.WaitGroup`,
  `handlerID uint64`, `routeName string`
- `storeAndReplicate`: change signature to `(ctx, key, obj, r *http.Request)`,
  add registration + scheduling when `refreshBeforeExpiry` is enabled.
  Update all 6 call sites: `revalidate` (line 662),
  `doBackgroundRevalidate` (lines 734, 746), `writeAndMaybeStore`
  (lines 796, 799), `maybeStorePostResponse` (line 963).
- `HandlerConfig`: add corresponding fields (including `RouteName string`)
- `NewHandler`: wire new fields, create `refreshSem` channel, initialise
  `done` channel, start scheduler drainer goroutine, store `routeName`
- `storeAndReplicate`: if `refreshBeforeExpiry`, call
  `h.refreshRegistry.Register(key, r, varyHeaders)` and
  `h.scheduler.Schedule(key, obj, h.refreshMargin)` after `store.Put`
- `invalidateAndProxy`: unregister from `refreshRegistry` for deleted keys
- `invalidateAndProxy`: unregister from `refreshRegistry` for deleted keys
- New methods: `triggerBgRefresh`, `doBackgroundRefresh`, `Close`
  (§2.4, §2.5)

### 5.4 `internal/cache/handler_test.go`

See §9 (Test Plan).

---

## 6. Builder Wiring

`cmd/bouine/cmd/builder.go`:

1. `buildRouter`: for each route with `Cache.RefreshBeforeExpiry=true`:
   - Set `HandlerConfig.RefreshBeforeExpiry = true`
   - Set `HandlerConfig.RouteName = rc.Name` (for metrics `route` label)
   - Compute `refreshMargin = TTL * marginPercent / 100` (using
     `TTLDefault` or `TTLOverride` as the TTL basis)
   - Set `HandlerConfig.RefreshMargin`, `RefreshConcurrency`,
     `RefreshTimeout`
2. **Shutdown:** The engine's shutdown sequence must call
   `handler.Close(ctx)` for every handler **before** `store.Close(ctx)`.
   Add a `handlers []io.Closer` slice to `runState` or iterate the
   router's routes.
3. **Hot reload:** Before creating a new handler for a route, call
   `oldHandler.Close(ctx)` to stop its scheduler and drain refreshes.
   The old scheduler's heap and registry are GC'd.

---

## 7. Cluster Interactions

| Mode | Behaviour |
|------|-----------|
| Single-node | Scheduler runs, refreshes all scheduled objects |
| Strong | Only the key owner schedules refreshes. `storeAndReplicate` checks `ownerFn(key).isLocal` before calling `scheduler.Schedule`. Non-owner nodes that store a peer-fetched object do not schedule. |
| Eventual | Each node schedules independently. May cause duplicate 304s to origin, but `collapsedFetch` deduplicates locally. Cross-node dedup is not attempted (eventual mode accepts redundant work). |
| Full | Only the node that originally stored the object schedules. `storeAndReplicate` checks a `localStore` flag (set when the store is from a local fetch, not a replication broadcast). |

**Cluster resize:** In strong mode, when the ring rebalances, keys
migrate to new owners. The old owner's scheduler still has entries for
keys it no longer owns. On pop, `store.Get` returns the object (it's
still in the local cache), but the object will be evicted naturally
(peer fetch promotes on the new owner). The old owner's refresh fetches
are wasted 304s. Mitigation: the scheduler can check `ownerFn(key).isLocal`
on pop and skip if the key is no longer local. Low priority — wasted
304s are cheap.

---

## 8. Metrics

| Name | Type | Labels | Description |
|------|------|--------|-------------|
| `bouine_refresh_total` | counter | `route`, `result` | Background refresh fetches (result: 304, 200, error) |
| `bouine_refresh_errors_total` | counter | `route`, `error_type` | Failed refresh fetches (timeout, 5xx, connection) |
| `bouine_refresh_skips_total` | counter | `route`, `reason` | Skipped refreshes (evicted, stale, not_owner, negative) |
| `bouine_refresh_in_flight` | gauge | `route` | Current in-flight background refreshes |
| `bouine_refresh_scheduled` | gauge | `route` | Entries in the refresh scheduler heap |
| `bouine_refresh_registry_size` | gauge | `route` | Entries in the refresh registry |

**Cardinality:** `route` is bounded by config. `result`, `error_type`,
`reason` are fixed enums. All within the 10k label combination budget.

**Route label:** Unlike `VaryCapHits` (a single counter with no labels),
the refresh metrics need a `route` label. The existing `DataPlaneMetrics`
gets the route from the `X-Bouine-Route` request header in middleware —
but refresh fetches have no client request and don't go through
middleware. The handler needs its own `routeName string` field (set
from `rc.Name` in the builder). Metrics are registered in
`internal/observability/dataplane.go` and passed to the handler via
labelled counter/gauge interfaces: `refreshCounters[route][result].Inc()`
or a `RefreshMetrics` struct holding pre-labelled `prometheus.Counter`
values per route.

---

## 9. Observability

- **Logs:** `slog` debug for refresh triggers, info for successful 304
  refreshes, warn for errors and retries. No request bodies or auth
  headers logged.
- **Traces:** A span `bouine.refresh` is created for each
  `doBackgroundRefresh`. Attribute `cache.refresh.key` (uint64, not the
  URL — avoids leaking path info).
- **Access log:** Background refresh fetches do NOT generate access log
  entries (not client requests). Tracked via metrics above.
- **Dashboard:** New tile on the Routes page: "Proactive Refresh Rate"
  showing 304/200/error counts per route. New column "Refresh" in the
  route table with a ✓/✗ indicator.

---

## 10. Security Considerations

| Threat | Mitigation |
|--------|------------|
| Origin load amplification: many objects expiring simultaneously | `refreshSem` (8 per route), min-heap spaces out refreshes naturally (jitter on TTL spreads `refreshAt`), `refreshTimeout` (30s) bounds each fetch |
| Registry memory growth | Bounded by stored objects. Cleaned up on eviction (lazy via `store.Get`), Delete, Ban. Per-route opt-in. ~200–450 B/entry. |
| Refresh after operator purge | Scheduler lazily detects via `store.Get` returning nil → skips + unregisters. No explicit notification needed. |
| Refresh after config reload disables feature | `handler.Close(ctx)` stops the scheduler. Old heap/registry GC'd. New handler has `refreshBeforeExpiry=false` → no new schedules. |
| Refresh fetches sensitive URLs after route change | If route match criteria change, registry entries may point to URLs that no longer match. The refresh fetch goes to origin, but the response may not be stored (cacheability check). Worst case: one wasted 304. |
| Refresh interferes with origin shutdown | `Handler.Close(ctx)` closes `done`, waits for `refreshWg` (or context deadline). `refreshTimeout` bounds each fetch. Engine calls `handler.Close` before `store.Close`. |

---

## 11. Threat Model Update

Add to `docs/security/threat-model.md`:

| ID | Threat | Category | Mitigation | Status |
|----|--------|----------|------------|--------|
| T-RBE-01 | Origin load amplification via refresh | Resource exhaustion | Concurrency cap (8), TTL jitter spreads refresh times, timeout (30s), min TTL threshold (5s) | Active |
| T-RBE-02 | refreshRegistry memory growth | Resource exhaustion | Bounded by stored objects, cleaned on eviction/Delete/Ban, per-route opt-in, ~200–450 B/entry | Active |
| T-RBE-03 | Refresh after operator purge | Integrity | Lazy detection via `store.Get` → skip + unregister. No hook needed. | Active |
| T-RBE-04 | Refresh goroutine outlives shutdown | Use-after-close | `Handler.Close` drains `refreshWg` before `store.Close`. `done` channel signals early termination. | Active |

---

## 12. ADR-0016: Refresh-Before-Expiry Per Route

**Status:** Proposed
**Date:** 2026-07-01
**Context:** Operators need a way to keep high-value routes perpetually
fresh with minimal origin traffic. SWR handles TTL expiry reactively
(client request triggers revalidation). Always-warm handles eviction
reactively. Neither prevents the expiry event. The cache key is an
irreversible hash, so the scheduler needs a handler-side registry to
reconstruct origin requests.

**Decision:** Per-route `refresh_before_expiry` opt-in. Handler-side
min-heap scheduler fires at `TTL − margin`. Conditional revalidation
via existing `collapsedFetch` path. Registry stores Vary-relevant
request headers only. Lazy cancellation via `store.Get` on pop.

**Alternatives considered:**
1. **Per-object `time.AfterFunc`** — rejected: one goroutine per object,
   unacceptable for 1M objects.
2. **Scanner (like reaper)** — rejected: O(n) scan per interval, coarse
   granularity, wastes CPU scanning non-refresh entries.
3. **Store the URL in `api.Object`** — rejected: changes public API,
   increases per-object memory for all objects, serializes to warm tier.
4. **Integrate with reaper** — rejected: 30s granularity too coarse for
   short TTLs. Mixing reaper (delete expired) and refresh (refresh
   before expiry) in one scan complicates the lock discipline.
5. **Always-warm (eviction-triggered)** — complementary, not
   alternative. Always-warm handles memory pressure eviction;
   refresh-before-expiry handles TTL expiry. Both can be enabled on the
   same route.

**Consequences:**
- New per-route memory cost: ~232–482 B per scheduled object (heap +
  registry).
- One drainer goroutine per handler with refresh enabled.
- Up to `refreshConcurrency` (8) refresh goroutines per route.
- `Handler` gains `Close(ctx) error` and a `done` channel.
- New config fields (5) with validation.
- New metrics (6).
- Zero storage-layer changes.
- Zero hit-path impact.
- ADR + threat model update + runbook entry.

---

## 13. Implementation Phases

### Phase A — Refresh scheduler (S)

- `internal/cache/scheduler.go` — `RefreshScheduler`, `heapEntry`,
  `refreshHeap` (implements `container/heap.Interface`)
- `Schedule`, `ScheduleAt`, `Stop`, drainer goroutine, periodic
  compaction pass (every 60s)
- Benchmark: heap insert vs sorted slice for 10k/100k/1M entries to
  justify `container/heap` (new pattern in this codebase)
- Tests: heap ordering, schedule/stop, lazy wake on new top, drain on
  empty heap, compaction removes dead entries, compaction preserves
  live entries

### Phase B — Refresh registry (S)

- `internal/cache/refresh_registry.go` — `refreshRegistry`,
  `refreshEntry`
- `Register` (clones Vary-relevant headers only), `Unregister`,
  `Lookup`, `Len`
- Tests: register/lookup/unregister, Vary-only header storage, header
  snapshot is a copy (no data race)

### Phase C — Handler integration (M)

- Add refresh fields to `Handler` and `HandlerConfig` (incl. `RouteName`)
- Change `storeAndReplicate` signature to `(ctx, key, obj, r)`, update
  all 6 call sites
- `triggerBgRefresh`, `doBackgroundRefresh`, `Close`
- Wire into `storeAndReplicate` (schedule + register) and
  `invalidateAndProxy` (unregister)
- `NewHandler`: create scheduler, start drainer, create `refreshSem`
- Error-path re-scheduling with backoff (clamp `remaining` to ≥ 0)
- `triggerBgRefresh`: use 5s timeout on `store.Get` (warm tier may be
  slow)
- Tests: refresh fires before expiry, 304 refreshes TTL, 200 replaces
  object, error retries with backoff, Ban/Delete no-refresh, Close
  drains goroutines, no new triggers after Close, refreshTimeout
  context, negative objects not refreshed (by default), `store.Get`
  timeout skips on slow warm tier, `storeAndReplicate` signature
  change doesn't break existing call sites

### Phase D — Config + builder wiring (S)

- Add config fields to `RouteCache` (5 fields)
- Validation in `loader.go`
- Wire in `builder.go` (`buildRouter`, shutdown ordering, hot reload)
- Tests: config validation, builder wiring, shutdown ordering, hot
  reload cleanup

### Phase E — Cluster integration (S)

- Strong mode: `isLocal` check before `scheduler.Schedule`
- Full mode: `localStore` flag to skip replication-sourced stores
- Eventual mode: no changes
- Tests: non-owner skip, owner refreshes

### Phase F — Metrics + dashboard + docs (S)

- 6 new Prometheus metrics (registered in `dataplane.go`, passed via
  interfaces)
- Dashboard tile + route table column
- ADR-0016
- Threat model update (T-RBE-01 through T-RBE-04)
- Runbook entry: "Refresh-Before-Expiry: when to use, when not to"
- Update `docs/architecture.md` §2.2 (prefetch now implemented)
- Move `docs/architecture.md` backlog item to active phase

---

## 14. Test Plan

### Unit tests

| Test | File | What it verifies |
|------|------|------------------|
| Heap ordering by refreshAt | `scheduler_test.go` | Earliest refreshAt at top |
| Schedule inserts into heap | `scheduler_test.go` | Entry appears, heap property maintained |
| ScheduleAt inserts with explicit time | `scheduler_test.go` | Custom time respected |
| Stop terminates drainer | `scheduler_test.go` | Drainer exits, no leak |
| Drainer wakes on new earlier top | `scheduler_test.go` | `ready` channel interrupts sleep |
| Drainer sleeps on empty heap | `scheduler_test.go` | Blocks on `ready`, no busy-loop |
| Registry register/lookup | `refresh_registry_test.go` | Entry stored and retrieved |
| Registry unregister | `refresh_registry_test.go` | Entry removed |
| Registry stores only Vary headers | `refresh_registry_test.go` | Non-Vary headers not stored |
| Registry header is a snapshot | `refresh_registry_test.go` | Mutating original header doesn't affect registry |
| Refresh fires before TTL expiry | `handler_test.go` | `doBackgroundRefresh` called before `StoredAt + TTL` |
| 304 refreshes TTL in place | `handler_test.go` | `refreshFrom304` called, `store.Put` called with new StoredAt |
| 200 replaces object | `handler_test.go` | `buildObject` called, `store.Put` called |
| Error re-schedules with backoff | `handler_test.go` | `ScheduleAt` called with delay < refreshMargin |
| Ban does not trigger refresh | `handler_test.go` | After Ban, scheduler pop finds no object |
| Delete does not trigger refresh | `handler_test.go` | After Delete, scheduler pop finds no object |
| Close drains refresh goroutines | `handler_test.go` | `refreshWg.Wait` completes, no panic on store.Close |
| No new triggers after Close | `handler_test.go` | `triggerBgRefresh` returns early after `done` closed |
| RefreshTimeout context cancels fetch | `handler_test.go` | Origin hangs → context deadline → error re-schedule |
| Negative objects not refreshed | `handler_test.go` | 404 with NegativeTTL → not scheduled |
| Min TTL threshold (5s) not scheduled | `handler_test.go` | Object with TTL=3s → not scheduled |
| Store.Get nil on pop → unregister | `handler_test.go` | Evicted key → registry entry removed |
| Config validation | `loader_test.go` | refresh_without_caching → error; margin 0 → error; margin 51 → error |

### Integration tests

| Test | File | What it verifies |
|------|------|------------------|
| Single-node perpetual freshness | `test/integration/` | Object never expires over 5 min; origin sees only 304s |
| Strong mode: only owner refreshes | `test/integration/` | Non-owner does not schedule; owner refreshes |
| Hot reload disables refresh | `test/integration/` | After reload, old scheduler stopped, no new refreshes |
| Shutdown drains refreshes | `test/integration/` | `handler.Close` → `store.Close` → no panic |
| Eviction storm + refresh coexist | `test/integration/` | Refresh and eviction don't interfere; no double-fetch |

### Conformance

No `cache-tests` regression expected — refresh-before-expiry is a
background feature that does not change the RFC 9111 freshness model.
The conformance score must remain ≥ 93.2%.

### Benchmarks

| Benchmark | Gate |
|-----------|------|
| Hit path with refresh disabled | allocs/op = 0, ns/op = baseline (no change) |
| Hit path with refresh enabled | allocs/op = 0, ns/op = baseline (no hit-path code) |
| Put path with refresh (heap insert) | ≤ 100 ns/op overhead (O(log n) mutex + compare) |
| Heap insert vs sorted slice (10k/100k/1M) | Justify `container/heap` choice with data |
| Scheduler drainer idle | 0 CPU when heap empty (sleeps on `ready`) |
| Scheduler drainer active | ≤ 500 ns/op per pop (store.Get + time check) |
| Scheduler compaction pass | ≤ 1 ms per pass (touches only near-future window) |
| refreshRegistry Register | ≤ 150 ns/op (mutex + map write + header clone) |

---

## 15. Files Changed

| File | Change |
|------|--------|
| `internal/config/config.go` | Add 5 fields to `RouteCache` |
| `internal/config/loader.go` | Validation for refresh fields |
| `internal/cache/scheduler.go` | **New** — `RefreshScheduler`, min-heap |
| `internal/cache/refresh_registry.go` | **New** — `refreshRegistry` |
| `internal/cache/handler.go` | Refresh fields (incl. `routeName`), `storeAndReplicate` signature change, `triggerBgRefresh`, `doBackgroundRefresh`, `Close`, scheduler wiring |
| `internal/cache/handler_test.go` | Tests for refresh-before-expiry |
| `internal/cache/scheduler_test.go` | **New** — scheduler tests |
| `internal/cache/refresh_registry_test.go` | **New** — registry tests |
| `cmd/bouine/cmd/builder.go` | Wire refresh config + shutdown ordering + hot reload |
| `internal/observability/dataplane.go` | 6 new metrics |
| `docs/decisions/0016-refresh-before-expiry.md` | **New** ADR |
| `docs/security/threat-model.md` | T-RBE-01 through T-RBE-04 |
| `docs/runbook/refresh-before-expiry.md` | **New** operator guide |
| `docs/architecture.md` | Update §2.2 (proactive refresh now implemented) |
| `docs/architecture.md` | Add refresh-before-expiry to roadmap |

---

## 16. Risks and Open Questions

1. **Heap size for very large caches.** For 1M objects on a
   refresh-enabled route, the heap is 32 MB (live) and the registry is
   ~200–450 MB. Dead entries are bounded by the compaction pass to
   O(refreshRate × 60s). Mitigation: document the cost.
   Consider a `max_refresh_keys` config field in v1.2 that caps the
   scheduler+registry size (LRU eviction of scheduled entries).

2. **Thundering herd at startup.** When bouine starts, the cache is
   empty. The first wave of client requests fills the cache and
   schedules refreshes. If all objects have the same TTL (no jitter),
   they all refresh at the same time. Mitigation: `JitterPercent`
   (existing config) spreads `refreshAt` values. Document that jitter
   is strongly recommended with refresh-before-expiry.

3. **Interaction with `max_fetch_concurrency`.** The refresh semaphore
   (8) is separate from the foreground fetch semaphore (32). If the
   origin is slow, 8 refresh + 32 foreground = 40 concurrent
   connections. Refresh fetches are 304s (no body), so memory pressure
   is low, but connection count may exceed origin capacity.
   Mitigation: consider a shared origin connection budget in v1.2.

4. **Refresh of Vary variants.** Each variant has its own cache key and
   its own heap/registry entry. If `Vary: Accept-Encoding` produces 2
   variants (gzip, identity), both are scheduled and refreshed
   independently. This is correct but doubles the origin 304 traffic
   for that URL. Acceptable — 304s are cheap.

5. **Clock skew.** `refreshAt` is computed using `time.Now()` (or the
   injected clock in tests). If the system clock jumps backward, the
   scheduler may fire late. If it jumps forward, the scheduler may fire
   early (refreshing objects that are still fresh). Mitigation: use
   monotonic clock (Go's `time.Now()` includes monotonic since 1.9, so
   `time.Until` is monotonic-safe). Document that `StoredAt` uses
   wall-clock (for persistence) but `time.Until` uses monotonic — this
   is already the case in the existing code.

6. **Should refresh-before-expiry replace SWR?** No. They compose:
   refresh-before-expiry is the primary freshness mechanism (fires
   before expiry). SWR is the fallback (serves stale if refresh fails
   and the object expires). StayinAlive is the last resort (serves
   stale indefinitely on origin down). The three form a defence-in-depth
   freshness strategy.
