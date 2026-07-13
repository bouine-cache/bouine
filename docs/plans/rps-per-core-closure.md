# RPS-Per-Core Closure Plan — bouine vs Varnish

> **Goal:** Close the RPS-per-core gap with Varnish by addressing
> structural differences in the HTTP stack and leveraging Go features
> that are currently unexploited. Every phase must improve or maintain
> CPU usage, latency, and memory — no regressions allowed.

## Current State

- Hit-path micro-benchmark: ~298 ns/op, 0 allocs/op (reusable writer)
- k6 hit-only: bouine 0.166 ms avg vs Varnish 0.177 ms — faster on
  average, but Varnish wins on p90/p95 and on RPS-per-core at saturation
- The gap is structural: `net/http` per-request overhead (header
  canonicalization, Request struct allocation, response buffering), no
  `writev` on the response path, single accept loop, tracing overhead
  on every request, GC pressure from `net/http` allocations

## Benchmark Gates (every phase)

Each phase must pass **all** of these before merging:

### Micro-benchmark gates (go test -bench)

| Benchmark | Current | Gate |
|-----------|---------|------|
| `BenchmarkHandler_CacheHit_ReusableWriter` | 298 ns, 0 allocs | ns/op ≤ previous, allocs/op = 0 |
| `BenchmarkEvaluate_Hit` | 34 ns, 0 allocs | ns/op ≤ previous, allocs/op = 0 |
| `BenchmarkHotStore_Get_Hit` | 5.2 ns, 0 allocs | ns/op ≤ previous, allocs/op = 0 |
| `BenchmarkHotStore_Get_Miss` | 3.6 ns, 0 allocs | ns/op ≤ previous, allocs/op = 0 |
| `BenchmarkHotGet_NoBans_Parallel` | 65 ns, 0 allocs | ns/op ≤ previous, allocs/op = 0 |
| `BenchmarkSIEVE_Access` | 5.3 ns, 0 allocs | ns/op ≤ previous, allocs/op = 0 |
| `BenchmarkBuildKey` | 48 ns, 0 allocs | ns/op ≤ previous, allocs/op = 0 |

### New benchmarks (added by this plan)

| Benchmark | Purpose | Gate |
|-----------|---------|------|
| `BenchmarkServeObject` | Isolate serveObject cost (header WriteTo + Write) | allocs/op = 0 |
| `BenchmarkServeObject_Prepared` | Pre-prepared headers + Write | allocs/op = 0, ns/op < BenchmarkServeObject |
| `BenchmarkMetricsMiddleware_Hit` | Full middleware overhead on a hit | allocs/op ≤ 1 |
| `BenchmarkTracingMiddleware_Noop` | No-op tracer overhead per request | allocs/op = 0 |
| `BenchmarkRouter_Match` | Route lookup cost | allocs/op = 0 |
| `BenchmarkResponseWriterPool_AcquireRelease` | Pool contention under parallelism | allocs/op = 0 |
| `BenchmarkHotStore_Get_Parallel_64Shards` | 64-shard concurrent hit at GOMAXPROCS | allocs/op = 0 |

### k6 load-test gates (docker compose)

| Scenario | Metric | Gate |
|----------|--------|------|
| 3.2 hit-only (50k RPS, 120s) | p50 server latency | ≤ previous − 2% or ≤ Varnish + 5% |
| 3.2 hit-only | p99 server latency | ≤ previous + 2% |
| 0.1 uncapped throughput | max RPS | ≥ previous − 0% (never regress) |
| 3.6 mixed realistic (25k RPS) | p95 server latency | ≤ previous + 2% |
| Memory RSS at steady state | pod RSS | ≤ previous + 5% |

### Regression check command

```bash
make bench           # micro-benchmarks + gate checks
# Then run k6 scenarios:
cd bench/loadtest && ./scenarios/3.2_hit_only/run.sh
cd bench/loadtest && ./scenarios/0.1_uncapped_throughput/run.sh
```

---

## Phase 1 — Eliminate per-request overhead on the hit path

**Problem:** The tracing middleware runs on every request, even when
tracing is not configured. It allocates 3 `attribute.KeyValue` structs
and creates a context + span per request. The no-op tracer is cheap
but not free — at 100k RPS, 100ns of overhead per request is 10ms of
CPU time per second.

**Fix:** Use an `atomic.Bool` set by `InitTracer` to detect whether
tracing is active. When the tracer is a no-op (no OTLP endpoint
configured), skip the middleware entirely. The middleware is only
wired into the handler chain when tracing is active. `atomic.Bool`
is used instead of `sync.OnceValue` because `InitTracer` and
`buildHandler` are both called at startup but in different
initialization phases — `atomic.Bool` has no ordering dependency.

### Tasks

1.1. Add `tracing.Enabled()` function backed by an `atomic.Bool` —
returns true only when `InitTracer` has configured a non-no-op tracer.
`InitTracer` sets the flag to true when an OTLP endpoint is configured.

1.2. Update `builder.go:buildHandler` to conditionally wrap with
`tracing.HTTPMiddleware` only when `tracing.Enabled()` returns true.

1.3. Add `BenchmarkTracingMiddleware_Noop` — measures the no-op tracer
overhead per request. Gate: 0 allocs/op, ns/op < 50.

1.4. Add `BenchmarkMetricsMiddleware_Hit` — measures the full
DataPlaneMetrics middleware overhead on a cache hit (including
Prometheus counter increment + histogram observe). Gate: allocs/op ≤ 1
(the 1 is from the histogram bucket lookup, unavoidable).

### Risk

- Removing the tracing middleware changes the context passed to
  downstream handlers. The `r.Context()` will no longer carry a span.
  Any code that calls `trace.SpanFromContext(r.Context())` will get a
  no-op span — this is the correct behavior when tracing is disabled.
- The `DataPlaneMetrics.Middleware` checks for a valid span to attach
  exemplars (`dataplane.go:393`). With no span, it falls back to
  plain `obs.Observe(dur)` — this path already exists and is correct.

### Files touched

| File | Changes |
|------|---------|
| `internal/observability/tracing/tracing.go` | `Enabled()` with `atomic.Bool` |
| `cmd/bouine/cmd/builder.go` | Conditional `tracing.HTTPMiddleware` |
| `internal/observability/tracing/tracing_test.go` | Test for `Enabled()` |
| `internal/cache/handler_bench_test.go` | `BenchmarkTracingMiddleware_Noop`, `BenchmarkMetricsMiddleware_Hit` |

---

## Phase 2 — Pre-populated response headers + `Content-Length` fast path

**Problem:** `serveObject` populates `http.Header` (a
`map[string][]string`) via `obj.Header.WriteTo(dst)`, then calls
`w.WriteHeader(code)` and `w.Write(obj.Body)`. `net/http` serializes
the header map into wire format, buffers everything through a
`bufio.Writer`, and flushes. That's 2+ syscalls and a body memcpy.
Varnish composes the response head + body into a single `writev` call
— zero copies, one syscall.

**Fix:** Pre-populate the `http.Header` map at `Put` time into a
compact, copyable representation. At serve time, copy it into
`w.Header()` in bulk (bypassing the `WriteTo` loop), set
`Content-Length` from `obj.BodySize` (avoiding chunked encoding),
call `w.WriteHeader(code)`, then `w.Write(body)`. `net/http`
internally uses `writev` when the body is written after `WriteHeader`
and the connection is HTTP/1.1 — the status line + headers are in
the `bufio.Writer` buffer, and the body write triggers a single
`writev` syscall. The real win is eliminating the `header.Map.WriteTo`
loop + the `w.Header().Del()` + `w.Header()[key] = val` calls that
populate the response header map per request.

### Design

At `Put` time, `PrepareHeaders` builds a complete `http.Header` map
with all origin headers + `Content-Length` from `obj.BodySize` +
`X-Cache: HIT` + `X-Cache-Source: hot`. Internal headers
(`XBouinePath`, `XBouineHost`) are excluded. `Transfer-Encoding` is
stripped. `stripNoCacheFields` is applied. This map is stored on
`obj.PreparedHeaders`.

At serve time, `serveObjectFast` does:
1. Copy `obj.PreparedHeaders` into `w.Header()` — this is a map
   copy, not the current `header.Map.WriteTo` loop. Since
   `PreparedHeaders` is already an `http.Header` (same type), we
   can iterate its keys and assign directly. This skips the
   `headerEntry` → `[]string` conversion that `WriteTo` does.
2. Set `w.Header()["Age"] = ageHeader(ComputeAge(obj, now))` —
   one map assignment, same as current.
3. Call `w.WriteHeader(obj.StatusCode)`.
4. Call `w.Write(obj.Body)`.

`net/http` internally buffers the status line + headers in a
`bufio.Writer`, then writes the body. When the body fits in the
`bufio.Writer`'s remaining buffer space, everything goes out in
one `write` syscall. When it doesn't, `net/http` flushes the header
buffer first, then writes the body directly — effectively two
syscalls, same as today but without the `WriteTo` overhead.

The `Age` header is not in `PreparedHeaders` — it's set at serve
time because it depends on `time.Since(obj.StoredAt)`.

Only the HIT variant is pre-prepared. STALE and REVALIDATED
responses use the slow `serveObject` path — they are the minority
and need different `X-Cache` / `Warning` headers.

**Why not `net.Buffers` / `writev`:** `net.Buffers.WriteTo` requires
the writer to be `*net.TCPConn` to trigger `writev`. Through
`http.ResponseWriter` (`*http.response`), `net.Buffers` falls back
to sequential `Write` calls — no `writev`. Hijacking the connection
to write directly to `*net.TCPConn` breaks keep-alive. The
pre-populated `http.Header` approach gets the header-serialization
savings without hijacking.

### Tasks

2.1. Add `PreparedHeaders http.Header` field to `api.Object` (tagged
`json:"-"` — transient, not serialized to warm tier). Pre-populated
at `Put` time with all origin headers + `Content-Length` from
`obj.BodySize` + `X-Cache: HIT` + `X-Cache-Source`. Internal headers
(`XBouinePath`, `XBouineHost`) are excluded. `Transfer-Encoding` is
stripped. `stripNoCacheFields` is applied. Only the HIT variant is
pre-populated — STALE and REVALIDATED use the slow `serveObject` path.

2.2. Add `PrepareHeaders(obj *api.Object, src api.Source)` function
in `internal/cache/handler.go` — builds the `http.Header` map once
at `Put` time. Called before `store.Put`.

2.3. Add `serveObjectFast` method — the fast-path serve function.
Copies `obj.PreparedHeaders` into `w.Header()` in bulk, sets the
dynamic `Age` header, calls `w.WriteHeader(code)`, then `w.Write(body)`.
Called only when `obj.PreparedHeaders != nil`. Falls back to
`serveObject` for legacy objects (warm-tier loads, cluster
replication).

2.4. Update `serveObject` to check for `PreparedHeaders` and dispatch
to `serveObjectFast`. The dispatch is a nil check — zero overhead
when pre-prepared headers are absent.

2.5. Update `Put` path (`handler.go:fillCache`) to call
`PrepareHeaders(obj, src)` before storing the object. Only
pre-prepare when `obj.BodySize <= 65536` (64 KiB) — larger objects
stream from warm tier and don't benefit. This is a named constant
(`maxPreSerializeBodySize`) and tested.

2.6. Update `codec.go` (warm-tier encode/decode) — the
`PreparedHeaders` field is `json:"-"` so it's automatically skipped.
On warm load, `PreparedHeaders` is nil → `serveObject` falls back to
the slow path → the next `Put` re-prepares. This is correct: warm
objects are cold and won't be served until promoted to hot.

2.7. Add `BenchmarkServeObject` — current path (header WriteTo +
Write). Add `BenchmarkServeObject_Prepared` — fast path
(pre-prepared headers + Write). Gate: both 0 allocs/op,
fast path ns/op < slow path ns/op.

### Memory impact

- `http.Header` is a `map[string][]string` — ~400 bytes for a 15-header
  response (map overhead + 15 keys + 15 single-element slices). At 1M
  entries with the 64 KiB body-size threshold, ~80% of objects qualify
  → ~320MB.
- The `http.Header` map is allocated once at `Put` time and lives with
  the `api.Object`. It's reclaimed when the object is evicted (the
  `hotEntry` pool reuses the `*api.Object`, so the map is reused too —
  `PrepareHeaders` clears and re-populates it).
- The k6 RSS gate (≤ previous + 5%) catches any regression. If 320MB
  exceeds the budget, reduce `maxPreSerializeBodySize` to 32 KiB.

### Risk

- **HTTP/2 compatibility:** `PreparedHeaders` is an `http.Header`
  map — it works identically for HTTP/1.1 and HTTP/2. The
  `serveObjectFast` path is safe for both protocols. No fallback
  needed.
- **Chunked encoding:** if the origin used `Transfer-Encoding: chunked`,
  `PrepareHeaders` strips it and sets `Content-Length` from
  `obj.BodySize` (matching PERF_PLAN Tier 1.5). This prevents
  `net/http` from falling back to chunked encoding on the hit path.
- **`no-cache` field stripping:** `stripNoCacheFields` removes
  fields listed in `Cache-Control: no-cache=field1, field2` from the
  response headers. This is done at `PrepareHeaders` time (in
  `PrepareHeaders`), not at serve time — the pre-prepared map
  already has the stripped headers.
- **Stale `PreparedHeaders` on object mutation:** if the object's
  headers change after `Put` (e.g., via cluster replication update),
  `PreparedHeaders` must be re-built. The `Put` path always calls
  `PrepareHeaders` before storing, so this is handled. On warm-tier
  load, `PreparedHeaders` is nil (not serialized), so the slow path
  runs until the object is re-put into hot.
- **Map copy cost:** copying `PreparedHeaders` into `w.Header()` is
  N map assignments (one per header). This is the same cost as the
  current `WriteTo` loop, but without the `headerEntry` → `[]string`
  sub-slice conversion. Net: slightly faster, same allocations (0).

### Files touched

| File | Changes |
|------|---------|
| `pkg/api/storage.go` | `PreparedHeaders http.Header` field |
| `internal/cache/handler.go` | `PrepareHeaders`, `serveObjectFast`, dispatch in `serveObject` |
| `internal/cache/handler.go:fillCache` | Call `PrepareHeaders` before `store.Put` |
| `internal/storage/codec.go` | Verify `json:"-"` skips the field (no change needed) |
| `internal/cache/handler_bench_test.go` | `BenchmarkServeObject`, `BenchmarkServeObject_Prepared` |

---

## Phase 3 — `SO_REUSEPORT` with N parallel accept loops

**Problem:** Bouine runs a single `http.Server.Serve()` per protocol.
At high connection rates, the single accept loop and the Go
scheduler's goroutine handoff become a bottleneck. Varnish in
multi-worker mode uses `SO_REUSEPORT` to distribute connections
across N worker processes, each with its own accept loop.

**Fix:** Wire `SO_REUSEPORT` (already implemented in
`platform_linux.go` but never called) and start N listener goroutines
(one per `GOMAXPROCS`), each calling `Serve()` on its own
`net.Listener` bound to the same address. The kernel hashes incoming
connections across sockets, distributing load.

### Tasks

3.1. Update `internal/server/listener.go:setSocketOptions` to call
`platform.SetReusePort(fd)` when `cfg.ReusePort` is true (new config
field, default true on Linux).

3.2. Add `ReusePort bool` to `ListenerConfig` and `config.Listen`
struct. Default: true on Linux, false on other platforms (macOS
doesn't support `SO_REUSEPORT` for TCP).

3.3. Update `Listener.Serve` to create N `net.Listener` instances
(N = `runtime.GOMAXPROCS(0)`) when `ReusePort` is true, each with
`SO_REUSEPORT` set. Run `http.Server.Serve()` on each in a separate
goroutine via `supervised.Group`. This distributes the accept loop
across N goroutines — the kernel hashes incoming connections across
the N sockets, so no single accept call is a bottleneck. Connection
goroutine count is unchanged (one per connection, as always). When
`ReusePort` is false, fall back to the current single-listener
behavior.

3.4. The `connLimitListener` semaphore is shared across all N
listeners — the connection cap is global, not per-listener.

3.5. Add `BenchmarkResponseWriterPool_AcquireRelease` — measures
`sync.Pool` contention under parallelism with N goroutines. This
establishes a baseline for pool contention before and after
`SO_REUSEPORT`.

3.6. Add `BenchmarkHotStore_Get_Parallel_64Shards` — 64-shard
concurrent hit at `GOMAXPROCS` parallelism. Establishes a baseline
for shard contention under realistic parallelism.

### Memory impact

- Each `net.Listener` + its accept goroutine costs ~8KB of stack.
  At `GOMAXPROCS=8`, that's ~64KB — negligible.
- Each `http.Server.Serve()` creates its own `connLimitListener`
  wrapper if `maxConns > 0`. The semaphore channel is shared, not
  duplicated. No additional memory.

### Risk

- **macOS doesn't support `SO_REUSEPORT` for TCP.** The config
  default is `false` on non-Linux. The `platform.SetReusePort`
  function is a no-op on non-Linux (returns an error, which is
  logged and ignored).
- **Connection distribution is hash-based, not round-robin.** The
  kernel hashes the 4-tuple (src_ip, src_port, dst_ip, dst_port).
  Under load testing with a single client IP, all connections may
  hash to the same listener. This is a known Linux kernel behavior
  — the 5.19+ kernel improved distribution, but it's still not
  perfect. For benchmarks, use multiple client IPs or disable
  `ReusePort` to compare single vs multi-listener.
- **Graceful shutdown** must drain all N listeners. The
  `supervised.Group` already handles this — `ctx.Done()` triggers
  `Shutdown()` on each `http.Server` in parallel.

### Files touched

| File | Changes |
|------|---------|
| `internal/server/listener.go` | Multi-listener support, `SO_REUSEPORT` wiring |
| `internal/config/config.go` | `ReusePort bool` field on `Listen` |
| `internal/config/loader.go` | Default: true on Linux, false otherwise |
| `cmd/bouine/cmd/engine.go` | Pass `ReusePort` to `ListenerConfig` |
| `internal/storage/hot_bench_test.go` | `BenchmarkHotStore_Get_Parallel_64Shards` |
| `internal/observability/responsewriter/responsewriter_test.go` | `BenchmarkResponseWriterPool_AcquireRelease` |
| `docs/decisions/0028-so-reuseport.md` | ADR |

---

## Phase 4 — `GOGC=50` + programmatic `SetMemoryLimit`

**Problem:** `GOGC=100` (default) means the GC triggers when the heap
doubles. At high RPS, `net/http` allocations generate ~200MB/s of
garbage. The GC runs every few seconds with multi-ms pauses. Varnish
has no GC — it's C with a fixed workspace.

**Fix:** Lower `GOGC` to 50 (with `GOMEMLIMIT` as the backstop) for
shorter, more frequent GC cycles with lower pause times. Set
`GOMEMLIMIT` programmatically from the cgroup memory limit instead
of relying on the Helm chart's 75% heuristic.

### Tasks

4.1. Add `debug.SetGCPercent(50)` in `engine.go` at startup, but
only when `GOGC` env var is not explicitly set (operator override).

4.2. Add `debug.SetMemoryLimit(bytes)` in `engine.go` at startup,
reading the cgroup v2 memory limit from
`/sys/fs/cgroup/memory.max` (or cgroup v1
`/sys/fs/cgroup/memory/memory.limit_in_bytes`). Falls back to the
`GOMEMLIMIT` env var, then to no limit. **Startup ordering:**
`SetMemoryLimit` and `os.Setenv("GOMEMLIMIT", resolvedValue)` MUST be
called before `config.Parse` / `config.Load` / `ResolveHotMaxBytes` /
`ResolveWarmMaxEntries` so that the cache budgets and the GC agree on
the memory ceiling. The `engine.go` startup sequence must be:
  1. Read cgroup limit → `SetMemoryLimit` + `os.Setenv("GOMEMLIMIT")`
  2. Parse config (which calls `ResolveHotMaxBytes` /
     `ResolveWarmMaxEntries` reading the env var)
  3. Build store with derived budgets
If the `GOMEMLIMIT` env var is already set (operator override), skip
step 1 and use the env var value for both the GC limit and the cache
budgets.

4.3. Add `EffectiveMemoryLimit()` to `internal/platform/` — reads
cgroup memory limit on Linux, returns 0 on other platforms.

4.4. Update `bench/loadtest/docker-compose.yaml` — set `GOGC=50`
for bouine (Varnish and NGINX are unaffected — no GC).

4.5. Add k6 gate: memory RSS at steady state ≤ previous + 5%. This
is measured by the Prometheus scraper on the `bouine` container.

### Memory impact

- `GOGC=50` allows the heap to grow 50% between GC cycles (vs 100%
  with default). Steady-state heap is slightly higher, but
  `GOMEMLIMIT` caps it. Net: same RSS, shorter GC pauses.
- `SetMemoryLimit` to the actual cgroup limit (vs 75% heuristic)
  gives the cache more headroom. The hot store budget (75% of
  GOMEMLIMIT) and warm index budget (15% of GOMEMLIMIT) are already
  derived from this value — setting the limit accurately means the
  GC doesn't trigger prematurely.

### Risk

- `GOGC=50` increases GC CPU usage by ~2-3% (more frequent cycles).
  This is offset by shorter pauses and better p99 stability. The
  micro-benchmark gate (ns/op ≤ previous) catches any regression.
- `SetMemoryLimit` to the full cgroup limit (vs 75%) means less GC
  headroom. The cache budgets must be accurate — if they over-allocate,
  the pod OOMKills. The existing `ResolveHotMaxBytes` (75% of
  GOMEMLIMIT) and `ResolveWarmMaxEntries` (15% of GOMEMLIMIT) leave
  10% for the Go runtime + stack + other allocations. This is tight
  but workable — monitor RSS in soak tests.

### Files touched

| File | Changes |
|------|---------|
| `cmd/bouine/cmd/engine.go` | `SetGCPercent(50)`, `SetMemoryLimit` |
| `internal/platform/platform_linux.go` | `EffectiveMemoryLimit()` |
| `internal/platform/platform_other.go` | `EffectiveMemoryLimit()` (returns 0) |
| `bench/loadtest/docker-compose.yaml` | `GOGC=50` for bouine |
| `deploy/helm/bouine/values.yaml` | Default `goGC: 50` |

---

## Phase 5 — `unique.Make` for header value interning

**Problem:** Every `Put` allocates new `string` values for common
headers like `"text/html"`, `"gzip"`, `"no-cache"`. At 1M entries
with 10 headers each, ~5 common values are duplicated across all
entries — ~50MB of duplicated strings that the GC must scan.

**Fix:** Use `unique.Make[string]` (Go 1.23+) to intern common header
values at `Put` time. Identical strings share one allocation.
Comparing two `unique.Handle[string]` values is a pointer comparison.

### Tasks

5.1. Add an interned header value cache in `pkg/header/headermap.go`:
a `sync.Map` mapping `string → unique.Handle[string]`. The `FromHTTP`
function (which builds `header.Map` from `http.Header`) interns each
value via `unique.Make`.

5.2. The `Map.values` slice stores `string` values, not
`unique.Handle[string]` — the interning is transparent. The
`unique.Handle` is only used during `FromHTTP` to deduplicate. The
stored `string` is `handle.Value()` which returns the canonical
string.

5.3. Add `InternValue(s string) string` function — wraps
`unique.Make(s).Value()`. For values already interned, this returns
the existing string with no allocation. For new values, it allocates
once and caches.

5.4. Update `header.FromHTTP` to call `InternValue` on each header
value before storing it in the `values` slice.

5.5. Add `BenchmarkStoreFootprint_Interned` — measures the heap
footprint of storing 1000 objects with interned vs non-interned
header values. Gate: `B/op` must be ≤ `BenchmarkStoreFootprint` − 15%.

### Memory impact

- Strongly positive — deduplicates string allocations. At 1M entries
  with 5 interned values per entry, saves ~50MB of heap.
- Reduces GC scan set — fewer unique string pointers to trace.
  Shorter GC pauses, lower p99.

### Risk

- `unique.Make` uses a global `sync.Map` internally — there is a
  small lock contention risk under high `Put` concurrency. This is
  on the miss path, not the hit path, so it doesn't affect RPS.
- The interned strings are never collected — they live for the
  process lifetime. This is acceptable for header values (small,
  bounded cardinality). A cache with 1000 unique Content-Type values
  interns ~20KB — negligible.

### Files touched

| File | Changes |
|------|---------|
| `pkg/header/headermap.go` | `InternValue`, update `FromHTTP` |
| `pkg/header/headermap_test.go` | Test interning deduplicates values |
| `internal/cache/handler_footprint_test.go` | `BenchmarkStoreFootprint_Interned` |

---

## Phase 6 — `http.ResponseController` + pre-resolved Prometheus labels

**Problem:** The `ResponseWriter` wrapper calls `w.Header()`,
`w.WriteHeader()`, `w.Write()` through the `http.ResponseWriter`
interface — dynamic dispatch on every call. The
`DataPlaneMetrics.Middleware` calls `WithLabelValues` (map hash +
lookup) on every request — even though the label tuple is one of a
small fixed set determined by route + cache result + status.

**Fix:** Use `http.ResponseController` (Go 1.20+) to cache type
assertions. Pre-resolve Prometheus label tuples at route init time
and call `.Inc()` on the pre-resolved counter directly.

### Tasks

6.1. Replace the `ResponseWriter` wrapper's delegation pattern with
`http.NewResponseController(w)`. Store the `*http.ResponseController`
on the `ResponseWriter` struct (set once in `Acquire`). The hit path
calls `rc.Header()`, `rc.WriteHeader()`, `rc.Write()` — no dynamic
dispatch after warmup.

6.2. Pre-resolve `prometheus.Counter` and `prometheus.Observer`
instances for the common label tuples at route init. Store them
on the route struct (not a global map) — each route has a fixed
set of expected (method, status, cacheResult, source) tuples.
The middleware indexes into the route's pre-resolved array by
status + cacheResult (both are small enums). No hash computation
per request — just an array index.

```go
type routeMetrics struct {
    // [status][cacheResult] → pre-resolved counter
    requestsTotal   [3][3]prometheus.Counter
    requestDuration [3][3]prometheus.Observer
    responseBytes   [3][3]prometheus.Counter
}
```
Status is indexed as `status/100 - 1` (2xx=1, 3xx=2, 4xx=3,
clamped). CacheResult is indexed as `cacheResultHit=0,
cacheResultMiss=1, cacheResultStale=2`. This is a direct array
index — no map lookup, no hash.

6.3. The `WithLabelValues` path is kept as a fallback for label
tuples outside the pre-resolved range (e.g., 5xx status codes).

6.4. Add `BenchmarkRouter_Match` — measures the route lookup cost.
Gate: allocs/op = 0, ns/op < 50.

### Memory impact

- `http.ResponseController` is a small struct (3 pointers) stored on
  the pooled `ResponseWriter` — no per-request allocation.
- The pre-resolved `routeMetrics` map is small (N routes × M status
  codes × K cache results). At 10 routes × 3 statuses × 3 cache
  results × 3 sources = 270 entries — negligible.

### Risk

- `http.ResponseController` wraps an `http.ResponseWriter` and caches
  type assertions. If the underlying writer changes between requests
  (which it doesn't — the pool resets the wrapper but the underlying
  `http.ResponseWriter` is per-connection), the cached assertions
  are invalid. Solution: `Acquire` must reset the
  `ResponseController` with the new underlying writer. This is a
  one-line change in the pool's `Acquire` function.

### Files touched

| File | Changes |
|------|---------|
| `internal/observability/responsewriter/responsewriter.go` | `http.ResponseController` integration |
| `internal/observability/dataplane.go` | Pre-resolved label tuples, `routeMetrics` map |
| `cmd/bouine/cmd/builder.go` | Pre-resolve labels at route init |
| `internal/server/router_test.go` | `BenchmarkRouter_Match` |

---

## ~~Phase 7 — `ConnState` per-connection buffer pooling~~ (DROPPED)

**Dropped in review cycle 1.** The design was broken: `ConnState`
fires per-connection, but `r.Context()` is the request context, not
the connection context. Getting `net.Conn` from the handler requires
hijacking, which breaks keep-alive. A `sync.Map[net.Conn, *ResponseWriter]`
adds a map lookup per request — potentially slower than the
`sync.Pool` it replaces. The ~5% gain doesn't justify the complexity.
`sync.Pool` in Go 1.26 is already per-P (per-processor) and has low
contention — the `ResponseWriter` pool is not the bottleneck.

---

## Implementation Order & Dependencies

```
Phase 1 (tracing skip)         — no dependencies, trivial, ship first
Phase 4 (GOGC + GOMEMLIMIT)    — no dependencies, trivial, ship early
Phase 5 (header interning)     — no dependencies, small, ship early
Phase 3 (SO_REUSEPORT)         — no dependencies, medium, ship after 1+4
Phase 2 (pre-prepared headers)  — depends on Phase 1 (tracing skip),
                                  large, highest RPS impact
Phase 6 (ResponseController)   — depends on Phase 2 (serveObject changes),
                                  small
```

### Expected cumulative impact

| Phase | RPS gain (cumulative) | p99 latency | Memory |
|-------|----------------------|-------------|--------|
| Baseline | — | — | — |
| + Phase 1 | +5-10% | -5% | 0 |
| + Phase 4 | +0% | -10-20% | neutral |
| + Phase 5 | +5-10% | -5% (less GC) | -50MB at 1M |
| + Phase 3 | +15-25% on multi-core | -5% | +64KB |
| + Phase 2 | +15-25% (cumulative +35-50%) | -10% | +320MB (mitigated, bounded) |
| + Phase 6 | +2-5% | -2% | neutral |

**Target:** within 10% of Varnish RPS-per-core, p99 ≤ Varnish + 5%,
RSS ≤ current + 5%.
