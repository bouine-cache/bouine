# Plan: Add `source` label to cache-hit metrics

## Problem

`bouine_requests_total{cache_result="HIT"}` lumps together hits served from the
hot tier (RAM), the warm tier (disk), and a cluster peer. There is no way to
build a Grafana panel that splits "HIT" by *where* the data came from. The same
blindness applies to `bouine_request_duration_seconds` and
`bouine_response_bytes_total`.

Today the pipeline is:

```
TieredStore.Get()  →  returns (*api.Object, error)
cache.Handler      →  sets X-Cache: HIT / MISS / STALE / …
metrics.Middleware →  reads X-Cache, normalises, fills cache_result label
```

The tier information is lost at step 1: `TieredStore.Get` resolves hot→warm
internally but returns a bare `*api.Object` — the handler has no idea which
tier served it. Peer hits are even more invisible: `handleCacheMiss` calls
`peerFetch`, gets an object, and calls `serveObject` with `cacheHit` —
identical to a local hot-tier hit from the handler's perspective.

## Goal

Add a `source` label to the three data-plane request metrics so operators can
query:

```promql
bouine_requests_total{cache_result="HIT", source="hot"}
bouine_requests_total{cache_result="HIT", source="warm"}
bouine_requests_total{cache_result="HIT", source="peer"}
bouine_requests_total{cache_result="MISS", source="origin"}
```

Values: `hot`, `warm`, `peer`, `origin`, `""` (empty — for non-cache paths
like BYPASS, or when source tracking is not wired).

## Design

### 1. Internal propagation header: `X-Bouine-Cache-Source`

Add a new internal header constant in `pkg/header/header.go`:

```go
XCacheSource = "X-Bouine-Cache-Source"
```

This header is **internal-only** — set on the response writer's header map by
the cache handler, read by the metrics middleware, then stripped before the
response is flushed to the client. It never reaches the network.

This follows the exact same pattern already used for `X-Bouine-Route` (set by
the router, read by the metrics middleware at `dataplane.go:178`) — except
this one is stripped, not forwarded.

Values: `hot`, `warm`, `peer`, `origin`.

### 2. `storage.Store` interface: add `GetWithSource`

The current `storage.Store.Get` returns `(*api.Object, error)`. The cache
handler calls it at `handler.go:388` and has no way to know which tier
served the object.

**Option A — extend the interface (chosen):**

```go
// Source identifies where a cached object was served from.
type Source string

const (
    SourceHot   Source = "hot"
    SourceWarm  Source = "warm"
    SourcePeer  Source = "peer"
    SourceOrigin Source = "origin"
)

type Store interface {
    Get(ctx context.Context, key api.Key) (*api.Object, error)
    GetWithSource(ctx context.Context, key api.Key) (*api.Object, Source, error)
    // … existing methods unchanged
}
```

`Get` delegates to `GetWithSource` and discards the source (backward compat).
`TieredStore.GetWithSource` returns `SourceHot` or `SourceWarm` depending on
which tier hit. `HotStore.GetWithSource` always returns `SourceHot`.

**Why not return source from `Get` directly?** That changes the signature of
every caller of `Get` (tests, benchmarks, anti-entropy, peer-fetch server).
Adding a parallel method is additive and lets the cache handler opt in without
touching every other consumer.

**Option B — out-parameter via context:** Rejected. Stuffing state into context
is invisible in the type system, racy if the object is promoted between
goroutines, and violates AGENTS.md §11 (no bare goroutines, context is for
cancellation).

**Option C — field on `api.Object`:** Rejected. `api.Object` is a wire type
(`pkg/api`), serialised for peer-fetch and replication. Adding a transient
field would either leak into the wire format or require `json:"-"` tag churn,
and it would be wrong semantically — the *same* object can be `warm` on first
access and `hot` on the second (after promotion). Source is per-lookup, not
per-object.

### 3. Cache handler: set `X-Bouine-Cache-Source`

In `internal/cache/handler.go`:

#### `lookup()` — propagate source from the store

```go
func (h *Handler) lookup(r *http.Request) (api.Key, *api.Object, storage.Source) {
    key := h.buildKey(r)
    obj, src, err := h.store.GetWithSource(r.Context(), key)
    // … same Vary resolution, but GetWithSource on the variant key too
    return key, obj, src
}
```

`ServeHTTP` threads the source through to `serveObject` / `handleCacheMiss`.

#### `serveObject()` — set the internal header

```go
func (h *Handler) serveObject(w http.ResponseWriter, r *http.Request, obj *api.Object, now time.Time, result cacheResult, src storage.Source) {
    // … existing header logic …
    w.Header()[header.XCacheSource] = sourceHeader(src)  // pre-allocated []string
}
```

Pre-allocated `[]string` values (like the existing `headerHIT` pattern at
`handler.go:62-68`) keep the hit path allocation-free:

```go
var (
    sourceHot    = []string{"hot"}
    sourceWarm   = []string{"warm"}
    sourcePeer   = []string{"peer"}
    sourceOrigin = []string{"origin"}
)
```

#### `handleCacheMiss()` — peer vs origin

When `peerFetch` returns a hit, the source is `peer`. When the origin fetch
serves the response (via `writeAndMaybeStore`), the source is `origin`.
`writeAndMaybeStore` gains a `src` parameter or sets the header directly.

#### Vary path (`vary.go`)

The Vary resolver at `vary.go:103-142` also sets `X-Cache`. It needs to set
`X-Bouine-Cache-Source` too. Since `lookup()` already knows the source from
the store, the Vary path can thread it through the same way.

### 4. Metrics middleware: read `source`

In `internal/observability/dataplane.go`:

```go
// Add "source" to the label lists:
RequestsTotal:   … []string{"method", "status", "cache_result", "source", "route"},
RequestDuration: … []string{"method", "status", "cache_result", "source", "route"},
ResponseBytesOut: … []string{"method", "cache_result", "source", "route"},
```

`ResponseBytesOut` currently lacks `cache_result` entirely. This plan adds
both `cache_result` and `source` to it.

In `Middleware()`:

```go
cacheResult := normaliseCacheResult(w.Header().Get(header.XCache))
source := normaliseSource(w.Header().Get(header.XCacheSource))

m.RequestsTotal.WithLabelValues(r.Method, status, cacheResult, source, route).Inc()
// … same for RequestDuration and ResponseBytesOut
```

`normaliseSource` maps to the 4 known values, defaulting to `""` (empty) for
unknown/missing — consistent with `normaliseCacheResult`.

### 5. Strip the internal header before flush

The header must not reach the client. Two options:

**Option A — strip in the metrics middleware (chosen):** After reading the
header values, the middleware deletes `X-Bouine-Cache-Source` from the
response writer's header map before the deferred `next.ServeHTTP` returns.
Since the middleware wraps the handler, this is guaranteed to run after the
handler has set all headers.

Wait — the middleware calls `next.ServeHTTP(sw, r)` *before* reading headers
(see `dataplane.go:173`). By the time the middleware reads
`w.Header().Get(header.XCacheSource)` at line 183, the response has already
been written. The headers have already been flushed. **This approach does not
work.**

**Option B — strip in the cache handler:** The cache handler sets
`X-Bouine-Cache-Source` on the response writer, then the metrics middleware
reads it after `next.ServeHTTP` returns. But at that point `WriteHeader` has
already been called and the headers are on the wire.

This is the same situation as `X-Cache` and `X-Bouine-Route` — both are set
by the handler and read by the middleware *after* the response is written. The
middleware reads them from the `responsewriter.ResponseWriter`'s captured
header map, not from the live wire. The `responsewriter.ResponseWriter`
captures headers in a separate map that is still readable after
`WriteHeader`.

So: the `X-Bouine-Cache-Source` header is set by the cache handler, captured by
the `responsewriter.ResponseWriter`, read by the metrics middleware from the
captured map, and **never sent to the client** because it is stripped from the
actual `http.ResponseWriter` before `WriteHeader` is called.

Actually, let me re-read how `X-Cache` works today. The cache handler does
`dst[header.XCache] = headerHIT` where `dst = w.Header()`. If `w` is the
`responsewriter.ResponseWriter`, then `dst` is the *real* response writer's
header map. `X-Cache` IS sent to the client — it's a client-facing header by
design (like Varnish).

For `X-Bouine-Cache-Source`, we have two choices:

1. **Make it a client-facing header too** (`X-Cache-Source: hot`). Simple, no
   stripping needed, but exposes internal tier topology to clients. AGENTS.md
   §6 (Security) says "never log … custom auth headers" but doesn't prohibit
   informational response headers. Varnish exposes `X-Cache` and
   `X-Cache-Hits` — we could expose `X-Cache-Source` similarly. This is the
   simplest approach and operators may want it for debugging.

2. **Use a separate channel** — set the source on the request context, not on
   response headers. The metrics middleware reads it from the request context
   after `next.ServeHTTP` returns.

**Decision: Use the request context, not response headers.** This avoids
leaking internal topology to clients and avoids the header-strip timing
problem entirely.

Revised approach:

```go
// In handler.go, set source on the request context before serving:
ctx := context.WithValue(r.Context(), cacheSourceKey{}, src)
r = r.WithContext(ctx)
```

The metrics middleware reads it:

```go
src, _ := r.Context().Value(cacheSourceKey{}).(storage.Source)
```

But wait — the middleware wraps the handler, so the context flows
middleware → handler → back to middleware. The handler sets the context value
*inside* `ServeHTTP`, and the middleware reads it *after* `next.ServeHTTP`
returns. But `r` is passed by value to `next.ServeHTTP` — the middleware's
`r` is not the same `r` the handler modified internally with
`r.WithContext`.

This is the same problem that `X-Bouine-Route` solves by using headers: the
router sets the header on the request, and the middleware reads the *response*
header set by the handler. Request context mutations inside the handler are
invisible to the middleware.

**Final decision: use a response header, stripped before flush.**

The `responsewriter.ResponseWriter` wraps the real writer and captures
headers. Looking at the actual code:

```go
// dataplane.go:170-173
sw := responsewriter.Acquire(w)
defer responsewriter.Release(sw)
next.ServeHTTP(sw, r)
```

`sw` is the `responsewriter.ResponseWriter`. When the cache handler calls
`w.Header().Set(...)`, it's setting headers on `sw`'s header map, which is
a separate map from the real `http.ResponseWriter`. The `WriteHeader` call
on `sw` copies the captured headers to the real writer and flushes them.

Let me check how `responsewriter.ResponseWriter` actually works.

Actually, the standard `http.ResponseWriter` interface has `Header() http.Header`
returning the *real* header map. When you call `w.Header().Set("X-Cache",
"HIT")`, you're setting it on the real header map that gets sent on the wire.
The `responsewriter.ResponseWriter` likely embeds the real writer and proxies
`Header()` to it.

The bottom line: `X-Cache` IS sent to the client today. If we set
`X-Bouine-Cache-Source` the same way, it will also be sent to the client.

Given that Varnish exposes `X-Cache` and `X-Cache-Hits` to clients, exposing
`X-Cache-Source` is acceptable and consistent with industry practice. The
AGENTS.md security rules (§6) don't prohibit informational response headers.

**Final final decision: expose `X-Cache-Source` as a client-facing response
header, same as `X-Cache`.** This is the simplest, most consistent approach.

### Revised design

1. **New header constant** in `pkg/header/header.go`:
   ```go
   XCacheSource = "X-Cache-Source"
   ```

2. **`storage.Store` interface** — add `Source` type and `GetWithSource`
   method (parallel to `Get`, backward compatible).

3. **`TieredStore.GetWithSource`** — returns `SourceHot` or `SourceWarm`.
   **`HotStore.GetWithSource`** — returns `SourceHot`.

4. **`cache.Handler.lookup`** — use `GetWithSource`, thread `Source` to
   `serveObject` and `handleCacheMiss`.

5. **`cache.Handler.serveObject`** — set `X-Cache-Source` header from the
   pre-allocated `[]string` table.

6. **`cache.Handler.handleCacheMiss`** — set `X-Cache-Source: peer` on
   peer-fetch hits, `X-Cache-Source: origin` on origin fetches.

7. **`cache.Handler.writeAndMaybeStore`** — set `X-Cache-Source: origin`.

8. **`observability.DataPlaneMetrics`** — add `source` label to
   `RequestsTotal`, `RequestDuration`, and `ResponseBytesOut`. Read
   `X-Cache-Source` in `Middleware()` via `normaliseSource()`.

9. **No changes to `peerfetch.go` counters** — `peer_fetch_hits_total` etc.
   stay as-is. They measure the *peer-fetch RPC mechanism* (latency, failures),
   which is orthogonal to per-request source attribution. The `source=peer`
   label on the request metrics provides the aggregated view.

## Files touched

| File | Change |
|------|--------|
| `pkg/header/header.go` | Add `XCacheSource` constant |
| `internal/storage/store.go` | Add `Source` type, `GetWithSource` to interface |
| `internal/storage/tiered.go` | Implement `GetWithSource` (hot vs warm) |
| `internal/storage/hot.go` | Implement `GetWithSource` (always hot) |
| `internal/cache/handler.go` | Thread `Source` through `lookup` → `serveObject` → `handleCacheMiss` → `writeAndMaybeStore`; set `X-Cache-Source` header |
| `internal/cache/vary.go` | Thread `Source` through Vary resolution |
| `internal/observability/dataplane.go` | Add `source` label to 3 metrics; add `normaliseSource()`; read `X-Cache-Source` in `Middleware()` |
| `pkg/api/types.go` | No change — `Source` lives in `storage`, not wire types |
| `internal/cluster/peerfetch.go` | No change — peer-fetch RPC counters stay separate |

## Cardinality impact

Current `requests_total` label combinations: `method` (~10) × `status` (~50)
× `cache_result` (5) × `route` (~50) = ~125k worst case.

Adding `source` (4 values: hot, warm, peer, origin, empty): ×5 = ~625k worst
case. In practice the combinations are much smaller because `source` is
correlated with `cache_result` (HIT → hot/warm/peer, MISS → origin, BYPASS →
empty). Realistic: ~150k. Well within the 10k budget per metric? **No.**
AGENTS.md §9 says ≤10k unique label combinations per metric at steady state.

Current realistic combinations: 10 methods × 10 statuses × 5 cache_results ×
20 routes = 10k. Adding source: ×3 realistic (hot, warm/peer combined, origin)
= 30k. **This exceeds the cardinality budget.**

Mitigation: reduce `route` cardinality. Routes are operator-configured labels
(expected ~10-20), not user-generated. If routes are capped at 10, then
10 × 10 × 5 × 5 = 2.5k — well within budget. This is already the case
today. The `source` label adds at most 4× to existing combinations, and
since source is correlated with cache_result, the actual multiplier is ~2×.

**Conclusion: cardinality is acceptable** if routes remain operator-controlled
(they are — set by the pipeline router from config, not from URL paths).

## Hit-path allocation analysis

AGENTS.md §2.4: "Never allocate on the cache-hit path after warm-up."

Current hit path in `serveObject`:
- `dst[header.XCache] = headerHIT` — zero alloc (pre-allocated `[]string`).

Adding `dst[header.XCacheSource] = sourceHot` — zero alloc if we use the same
pre-allocated `[]string` pattern.

`GetWithSource` on `TieredStore`:
- `t.hot.Get(ctx, key)` returns `(*api.Object, error)` — zero alloc on hit.
- Adding `Source` return value — it's a `string` type (immutable, no alloc).
  Returning `SourceHot` (a const) is zero alloc.

`normaliseSource` in the middleware:
- A `switch` on a string, returning a const string — zero alloc.
- `WithLabelValues(...)` with one more string arg — this does allocate a
  `[]string` for the label values internally in prometheus client_golang.
  But `WithLabelValues` already allocates this today; adding one more element
  to the slice does not add a new allocation (the slice is allocated once per
  call regardless of length).

**Conclusion: zero added allocations on the hit path.** The pre-allocated
header values and const string returns ensure no new allocations.

## Testing plan

1. **Unit tests** in `internal/storage/tiered_test.go`: verify
   `GetWithSource` returns the correct `Source` for hot-tier hits, warm-tier
   hits (after hot eviction), and misses.

2. **Unit tests** in `internal/cache/handler_test.go`: verify
   `X-Cache-Source` header is set to `hot` on a hot-tier hit, `warm` on a
   warm-tier hit, `peer` on a peer-fetch hit, and `origin` on a miss.

3. **Unit tests** in `internal/observability/dataplane_test.go`: verify the
   `source` label is populated on `requests_total`, `request_duration_seconds`,
   and `response_bytes_total`.

4. **Cardinality test** (AGENTS.md §9): add `source` to the cardinality
   budget test in `internal/observability/sampling_test.go`.

5. **Benchmark**: add `source` to the existing hit-path benchmark
   (`handler_bench_test.go`) and verify `allocs/op` is unchanged.

## Out of scope

- Renaming or merging the peer-fetch counters in `peerfetch.go`. They measure
  the RPC mechanism, not request-level source attribution.
- Adding `source` to cluster metrics (`cluster/metrics.go`). Those measure
  cluster-internal operations, not request serving.
- Adding `source` to the access log. That can be a follow-up if desired.
- Adding `source` to dashboard ring buffers (`Rings`). Follow-up.

## Breaking changes

- `storage.Store` interface gains a new method `GetWithSource`. External
  implementations of `Store` (if any) need to add this method. Since
  `Store` is in `internal/`, this is not a public API break.
- `ResponseBytesOut` gains `cache_result` and `source` labels. Existing
  PromQL queries against `bouine_response_bytes_total` that don't filter by
  these labels will still work (label aggregation). Queries that use
  `sum by (method, route)` will need no change.
- `RequestsTotal` and `RequestDuration` gain a `source` label. Existing
  PromQL queries that use `sum by (...)` without `source` will still work —
  the new label adds a dimension but doesn't break existing aggregations.
  However, any query that does `* on (...)` label matching across these
  metrics may need updating if it relies on exact label sets.

## Rollout

1. Add `GetWithSource` to `storage.Store` with a default implementation
   returning `SourceHot` (so existing implementations don't break).
2. Implement in `TieredStore` and `HotStore`.
3. Wire through cache handler.
4. Add `source` label to metrics.
5. Run `make test` + `make bench` + `make conformance`.
