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

> **`writev` note:** The lack of `writev` on the response path is
> listed as a structural gap but is **not addressed by any phase** in
> this plan. `net/http` doesn't expose `writev` without hijacking the
> connection, which breaks keep-alive and HTTP/2. The `WriteHeader` +
> `Write` path is 2 syscalls, but both are fast-path kernel writes for
> small responses. A custom `io.Writer` using `syscall.Writev` is not
> justified by the ~1 µs saving. Dropped from scope.

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
| `BenchmarkServeObject_FastPath` | Fast-path serve (pre-stripped headers + Write) | allocs/op = 0, ns/op < BenchmarkServeObject |
| `BenchmarkMetricsMiddleware_Hit` | Full middleware overhead on a hit | allocs/op ≤ 1 |
| `BenchmarkTracingMiddleware_Noop` | No-op tracer overhead per request | allocs/op = 0 |
| `BenchmarkRouter_Match` | Route lookup cost | allocs/op = 0 |
| `BenchmarkResponseWriterPool_AcquireRelease` | Pool contention under parallelism | allocs/op = 0 |
| `BenchmarkHotStore_Get_Parallel_64Shards` | 64-shard concurrent hit at GOMAXPROCS | allocs/op = 0 |
| `BenchmarkH1Parse_Get` | Custom H1 parser: parse a realistic GET request | allocs/op = 0 |
| `BenchmarkFastPath_Hit` | End-to-end fast path: parse + lookup + evaluate + writev | allocs/op = 0 |
| `BenchmarkFastPath_Fallthrough` | Parse + construct `*http.Request` + call handler chain | allocs/op ≤ 2 |

### k6 load-test gates (docker compose)

| Scenario | Metric | Gate |
|----------|--------|------|
| 3.2 hit-only (50k RPS, 120s) | p50 server latency | ≤ previous − 2% or ≤ Varnish + 5% |
| 3.2 hit-only | p99 server latency | ≤ previous + 2% |
| 0.1 uncapped throughput | max RPS | ≥ previous − 0% (never regress) |
| 3.6 mixed realistic (25k RPS) | p95 server latency | ≤ previous + 2% |
| Memory RSS at steady state | pod RSS | ≤ previous + 5% |
| 0.1 uncapped throughput (h1_fast_path=true) | max RPS | ≥ previous + 20% |
| 0.1 uncapped throughput (h1_fast_path=true) | p99 server latency | ≤ Varnish + 5% |

### Regression check command

```bash
make bench           # micro-benchmarks + gate checks
# Then run k6 scenarios:
cd bench/loadtest && ./scenarios/3.2_hit_only/run.sh
cd bench/loadtest && ./scenarios/0.1_uncapped_throughput/run.sh
```

---

## Phase 1 — Eliminate per-request tracing overhead on the hit path

**Problem:** The tracing middleware runs on every request, even when
tracing is not configured. It allocates 3 `attribute.KeyValue` structs
and creates a context + span per request. The no-op tracer is cheap
but not free — at 100k RPS, 100ns of overhead per request is 10ms of
CPU time per second.

Even when tracing **is** active, the middleware is more expensive
than it needs to be. `r.URL.String()`
([tracing.go:45](internal/observability/tracing/tracing.go#L45))
allocates a formatted URL string on every request — including hits
where the value is only attached as a span attribute and never read
again. At 100k RPS with tracing on, that's 100k allocations/s for
no functional benefit: the trace backend can reconstruct the URL from
`http.method` + `http.host` + `r.URL.Path` (which is already a string
field on the `*url.URL` struct, no allocation to read).

**Fix (two changes):**

1. **Skip the middleware when tracing is off.** Use an `atomic.Bool`
set by `InitTracer` to detect whether tracing is active. When the
tracer is a no-op (no OTLP endpoint configured), skip the middleware
entirely. The middleware is only wired into the handler chain when
tracing is active. `atomic.Bool` is used instead of `sync.OnceValue`
because `InitTracer` and `buildHandler` are both called at startup
but in different initialization phases — `atomic.Bool` has no ordering
dependency.

2. **Eliminate `r.URL.String()` even when tracing is on.** Replace
`attribute.String("http.url", r.URL.String())` with
`attribute.String("http.target", r.URL.Path)` — `r.URL.Path` is a
pre-existing string field on `*url.URL` with no formatting or
allocation. This matches the [OpenTelemetry semantic conventions for
HTTP servers](https://opentelemetry.io/docs/specs/semconv/http/http-spans/),
which use `http.request.method` + `url.path` + `url.query` (or
`http.target` for the combined form) rather than the full
`url.scheme://host/path?query` form.

Note: `r.URL.RequestURI()` was considered but rejected because it
allocates when a query string is present (`result += "?" + u.RawQuery`
is a string concatenation). `r.URL.Path` is always a direct field access
— zero allocation in all cases. The query string is available from
`r.URL.RawQuery` (also a direct field) and can be set as a separate
`url.query` attribute if needed, or omitted — the path is sufficient
for trace attribution.

### Tasks

1.1. Add `tracing.Enabled()` function backed by an `atomic.Bool` —
returns true only when `InitTracer` has configured a non-no-op tracer.
`InitTracer` sets the flag to true when an OTLP endpoint is configured.

1.2. Update `builder.go:buildHandler` to conditionally wrap with
`tracing.HTTPMiddleware` only when `tracing.Enabled()` returns true.

1.3. Replace `r.URL.String()` in `tracing.HTTPMiddleware` with
`r.URL.Path`. Change the attribute key from `"http.url"` to
`"url.path"` to match OpenTelemetry semantic conventions.
`r.URL.Path` is a direct field access on `*url.URL` — zero
allocation in all cases (unlike `RequestURI()` which allocates when
a query string is present). Optionally add `url.query` from
`r.URL.RawQuery` as a separate attribute if query visibility is
needed in traces.

1.4. Add `BenchmarkTracingMiddleware_Active` — measures the tracing
middleware overhead with a real (non-no-op) tracer after the
`r.URL.String()` fix. Gate: 0 allocs/op, ns/op < 80. The benchmark
must use a request with a query string to verify the `r.URL.Path`
path is truly zero-alloc.

1.5. Add `BenchmarkTracingMiddleware_Noop` — measures the no-op tracer
overhead per request (should never be wired, but benchmarks the
middleware itself for regression detection). Gate: 0 allocs/op,
ns/op < 50.

1.6. Add `BenchmarkMetricsMiddleware_Hit` — measures the full
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
- **`r.URL.Path` vs `r.URL.String()`:** `Path` is a direct field access
  on `*url.URL` — zero allocation. `String()` formats
  `scheme://host/path?query`, always allocating. `RequestURI()` was
  rejected because it allocates when a query string is present (string
  concatenation: `result += "?" + u.RawQuery`). The trace backend
  loses the scheme (http vs https) and host, but `http.host` is
  already set as a separate attribute (`r.Host`). If the full URL is
  needed, set `url.scheme` from the listener config (pre-computed, not
  per-request). Do not call `r.URL.String()` on the hot path.
- **Semantic conventions:** The switch from `http.url` to `url.path` +
  `http.host` follows the [OpenTelemetry HTTP server semantic
  conventions](https://opentelemetry.io/docs/specs/semconv/http/http-spans/).
  Verify that Grafana / Tempo dashboards that filter by `http.url`
  are updated to use `url.path` or `http.target`.

### Files touched

| File | Changes |
|------|---------|
| `internal/observability/tracing/tracing.go` | `Enabled()` with `atomic.Bool`, replace `r.URL.String()` with `r.URL.Path` |
| `cmd/bouine/cmd/builder.go` | Conditional `tracing.HTTPMiddleware` |
| `internal/observability/tracing/tracing_test.go` | Test for `Enabled()`, test `RequestURI()` attribute |
| `internal/cache/handler_bench_test.go` | `BenchmarkTracingMiddleware_Active`, `BenchmarkTracingMiddleware_Noop`, `BenchmarkMetricsMiddleware_Hit` |

---

## Phase 2 — Move serve-time header work to Put time

**Problem:** `serveObject` ([handler.go:930](internal/cache/handler.go#L930))
does several things per request that could be done once at `Put` time:

1. `obj.Header.WriteTo(dst)` — copies all headers into `w.Header()`.
   This is already zero-alloc (`headermap.go:268`), but it copies
   internal headers (`X-Bouine-Path`, `X-Bouine-Host`) that are
   immediately deleted.
2. `dst.Del(header.XBouinePath)` + `dst.Del(header.XBouineHost)` —
   2 map deletes per request for headers that should never be in the
   output map in the first place.
3. `stripNoCacheFields(dst, obj.CacheControl)` — parses the
   `Cache-Control` string via `ParseCacheControl` and calls
   `strings.FieldsFunc` which **allocates a `[]string` slice** on
   every hit when `no-cache=field` directives are present. This is
   a per-request heap allocation that affects objects with
   `no-cache=field` directives (uncommon, but still a correctness
   violation of the zero-alloc hit-path rule for those objects). The
   function returns early when `cc.NoCacheFields == ""` (the common
   path), but the allocation is still present for affected objects.
   The result is deterministic for a given object — it never changes
   after
   `Put`.
4. `X-Cache` and `X-Cache-Source` — 2 map assignments per request.

The `WriteTo` loop itself is not the bottleneck — it's already a
zero-alloc direct map assignment from a flat slice. The bottleneck is
the *per-request post-processing* (deletes, stripping, conditional
headers) that could be pre-computed.

**Fix:** Do all serve-time header work at `Put` time on the existing
`header.Map`. No new `http.Header` map is added to `api.Object` — the
existing `header.Map` is prepared once and serves directly.

### Design

**Step A: Move `X-Bouine-*` internal headers out of `header.Map`.**

`X-Bouine-Path` and `X-Bouine-Host` are stored in `obj.Header` at
`buildObject` time ([handler.go:1583](internal/cache/handler.go#L1583))
for ban predicate matching, then stripped in `serveObject` before
serving. This is the only reason `serveObject` calls `dst.Del()`.

Add two dedicated fields to `api.Object`:
```go
BouinePath string `json:"-"` // transient, for ban matching
BouineHost string `json:"-"` // transient, for ban matching
```
Set them in `buildObject` instead of `obj.Header.Set`. Update ban
matching to read from these fields. `serveObject` no longer needs
`dst.Del(XBouinePath)` or `dst.Del(XBouineHost)` — the headers were
never in `header.Map` to begin with.

For warm-tier persistence: the binary codec already skips `json:"-"`
fields. On warm load, these fields are empty. If ban matching needs
them, re-derive from the key (the key already encodes the path). If
not, skip — bans on warm objects are rare.

**Step B: Move `stripNoCacheFields` to `Put` time.**

`stripNoCacheFields` removes headers listed in
`Cache-Control: no-cache=field1, field2` from the response. The
result is deterministic for a given object — it never changes after
`Put`. Apply it in `buildObject` after `header.FromHTTP`, operating
on `header.Map` directly (add a `StripNoCacheFields` method to
`header.Map`). At serve time, `serveObject` skips the call entirely.

**Step C: Pre-set `Content-Length` at `Put` time (already done).**

`buildObject` already sets `Content-Length` from `obj.BodySize`
([handler.go:1596](internal/cache/handler.go#L1596)). No change needed.
This ensures `net/http` doesn't fall back to chunked encoding on the
hit path.

**Step D: Set `X-Cache` and `X-Cache-Source` at serve time only.**

These depend on the cache result (HIT vs STALE vs REVALIDATED) and the
source tier (hot/warm/peer), so they must remain at serve time. They're
already zero-alloc direct map assignments using pre-allocated `[]string`
values. No change needed.

### What this does NOT do

- **No `PreparedHeaders http.Header` field.** The original plan proposed
  storing a second copy of all headers as `http.Header` on every object
  (~400 B/object, ~320 MB at 1M entries). This is a bad trade: it
  duplicates data already stored in `header.Map` (144 B/object) to save
  2 map deletes and 1 function call. The `header.Map.WriteTo` loop is
  already zero-alloc and ~50 ns for 15 headers — it's not the bottleneck.
- **No `serveObjectFast` / dual-path dispatch.** The improvements are
  applied to `serveObject` itself — no nil check, no fallback path,
  no complexity. Every object benefits, not just HIT.

### Tasks

2.1. Add `BouinePath` and `BouineHost` string fields to `api.Object`
(`json:"-"`). Update `buildObject` to set these instead of
`obj.Header.Set(XBouinePath, ...)` and `obj.Header.Set(XBouineHost, ...)`.
Update all ban-matching code that reads `obj.Header.Get(XBouinePath)`
to read `obj.BouinePath` instead.

2.2. Add `StripNoCacheFields(cc string)` method to `header.Map` —
parses the `no-cache=field1,field2` directive and `Del`s matching
entries from the Map. Call it in `buildObject` after `header.FromHTTP`
and after `Set-Cookie` stripping. Remove the `stripNoCacheFields`
call from `serveObject`.

2.3. Update `serveObject` to remove the `dst.Del(XBouinePath)`,
`dst.Del(XBouineHost)`, and `stripNoCacheFields` calls. The remaining
serve path is: `WriteTo` → set `Age` → set `X-Cache-Source` → set
`X-Cache` (conditional) → `WriteHeader` → `Write`.

2.4. Update the warm-tier codec to handle the new fields:
The warm-tier binary codec ([codec.go](internal/storage/codec.go))
is a custom encoder that explicitly names every field it serializes
in `encodeObject` — it does not use reflection or `json` tags.
`BouinePath`/`BouineHost` are skipped because `encodeObject` does
not reference them, not because of the `json:"-"` tag. The `json:"-"`
tag is kept for documentation consistency with `CacheControl`/`OriginAge`.
On warm load, these fields are empty — ban matching on warm objects
falls back to key-based matching (the key already encodes the path).
Document this in the codec.

2.5. Add `BenchmarkServeObject` — current path (before changes) and
`BenchmarkServeObject_FastPath` — after changes. Gate: both 0 allocs/op,
fast path ns/op < slow path ns/op.

### Memory impact

- **Net negative (memory savings).** Removing `X-Bouine-Path` and
  `X-Bouine-Host` from `header.Map` saves 2 entries per object (~48 B:
  2 × 24 B `headerEntry`). At 1M entries, saves ~48 MB.
- No new fields added to `api.Object` except 2 transient strings
  (`BouinePath`, `BouineHost`) that replace 2 `header.Map` entries —
  net zero or slightly negative (strings are smaller than map entries).

### Risk

- **Ban matching regression:** Ban predicates that match on
  `X-Bouine-Path` or `X-Bouine-Host` must be updated to read from the
  new fields. Audit all ban predicate evaluators before merging.
  The `grep` for `XBouinePath` / `XBouineHost` in ban code is the
  verification step.
- **Warm-tier ban matching:** Warm-loaded objects have empty
  `BouinePath`/`BouineHost`. If a ban predicate needs these, the
  object must be promoted to hot first (where the fields are
  re-populated). This is acceptable — bans on cold objects are rare,
  and the current behavior already re-derives `CacheControl`/`OriginAge`
  on warm load.
- **HTTP/2 compatibility:** No change — `header.Map.WriteTo` populates
  `http.Header` identically for HTTP/1.1 and HTTP/2.

### Files touched

| File | Changes |
|------|---------|
| `pkg/api/storage.go` | `BouinePath`, `BouineHost` fields on `Object` |
| `pkg/header/headermap.go` | `StripNoCacheFields` method on `Map` |
| `internal/cache/handler.go` | `buildObject`: set new fields, call `StripNoCacheFields`; `serveObject`: remove deletes + strip |
| `internal/cache/ban.go` (or wherever bans are evaluated) | Read `obj.BouinePath` / `obj.BouineHost` instead of `obj.Header.Get` |
| `internal/storage/codec.go` | Verify `json:"-"` skips new fields (no change needed) |
| `internal/cache/handler_bench_test.go` | `BenchmarkServeObject`, `BenchmarkServeObject_FastPath` |

---

## Phase 3 — `SO_REUSEPORT` with N parallel accept loops

**Problem:** Bouine runs a single `http.Server.Serve()` per protocol.
At high connection rates, the single accept loop and the Go
scheduler's goroutine handoff become a bottleneck. Varnish in
multi-worker mode uses `SO_REUSEPORT` to distribute connections
across N worker processes, each with its own accept loop.

**Fix:** Wire `SO_REUSEPORT` (already implemented in
`platform_linux.go:50` but never called) and start N listener goroutines
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

3.4. The `connLimitListener` semaphore must be shared across all N
listeners — the connection cap is global, not per-listener. This
requires extracting the semaphore channel and passing it to each
listener's `connLimitListener` wrapper explicitly. Currently each
`Serve` call creates its own `connLimitListener` — verify the
semaphore is shared, or refactor to share it.

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
  wrapper if `maxConns > 0`. The semaphore channel must be shared,
  not duplicated. No additional memory if shared correctly.

### Risk

- **macOS doesn't support `SO_REUSEPORT` for TCP.** The config
  default is `false` on non-Linux. The `platform.SetReusePort`
  function is a no-op on non-Linux (returns `nil`, not an error).
  This is a problem: if an operator sets `ReusePort=true` on macOS,
  `SetReusePort` silently succeeds, and the listener creates N sockets
  bound to the same address *without* `SO_REUSEPORT` — resulting in
  "address already in use" on the second listener. **Fix:** the
  non-Linux stub must return `errors.New("SO_REUSEPORT not supported")`
  so the listener can log the error and fall back to single-listener
  mode. Additionally, `config.Validate` should reject `ReusePort=true`
  on non-Linux platforms.
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
- **`connLimitListener` semaphore sharing:** If each `Serve` call
  creates its own semaphore, the connection cap becomes N × maxConns
  instead of maxConns. The semaphore must be created once and shared
  across all N listeners. This requires a small refactor to
  `Listener.Serve` — extract the semaphore creation and pass it to
  each listener instance.

### Files touched

| File | Changes |
|------|---------|
| `internal/server/listener.go` | Multi-listener support, `SO_REUSEPORT` wiring, shared semaphore |
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

### Startup ordering (corrected)

The cgroup limit reading and `os.Setenv("GOMEMLIMIT")` must happen
**before** `config.Parse` / `config.Load` is called, because
`ResolveHotMaxBytes` and `ResolveWarmMaxEntries` read `GOMEMLIMIT`
from the environment during config loading. Currently config is
parsed in `serve.go:loadConfig` **before** `engine.go:run` is called.

The correct startup sequence is:

1. In `serve.go` (or a new `init` step before `loadConfig`):
   a. Read the cgroup memory limit via `platform.EffectiveMemoryLimit()`.
   b. If `GOMEMLIMIT` env var is not already set, set it to the cgroup
      limit via `os.Setenv("GOMEMLIMIT", resolvedValue)`.
   c. Call `debug.SetMemoryLimit(resolvedValue)`.
2. Call `loadConfig` → `config.Parse` → `ResolveHotMaxBytes` /
   `ResolveWarmMaxEntries` (which read `GOMEMLIMIT` from the env).
3. `engine.go:run` receives the already-parsed config and builds the
   store with derived budgets.
4. Call `debug.SetGCPercent(50)` in `engine.go:run` at startup, but
   only when `GOGC` env var is not explicitly set (operator override).

If the `GOMEMLIMIT` env var is already set (operator override), skip
step 1 and use the env var value for both the GC limit and the cache
budgets.

### Tasks

4.1. Add `debug.SetGCPercent(50)` in `engine.go:run` at startup, but
only when `GOGC` env var is not explicitly set (operator override).

> **Re-evaluation note:** `GOGC=50` is tuned for the pre-optimization
> allocation rate. After Phases 1, 2, and 5 land (which reduce
> allocations on the hot and miss paths), the heap grows slower between
> GC cycles. `GOGC=50` may then be too aggressive — the GC runs more
> often than needed, wasting 2-3% CPU. Re-evaluate `GOGC` after those
> phases merge: `GOGC=75` or `GOGC=100` may be sufficient and reclaim
> the GC CPU overhead.

4.2. Add cgroup limit reading + `debug.SetMemoryLimit` + `os.Setenv`
in `serve.go` before `loadConfig` is called. This is the earliest
point where the process can read cgroup limits and set the env var
before any config resolution happens.

4.3. Add `EffectiveMemoryLimit()` to `internal/platform/` — reads
cgroup v2 memory limit from `/sys/fs/cgroup/memory.max` (or cgroup v1
`/sys/fs/cgroup/memory/memory.limit_in_bytes`). Falls back to the
`GOMEMLIMIT` env var, then to 0 (no limit). Returns 0 on non-Linux.

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
| `cmd/bouine/cmd/serve.go` | cgroup limit reading + `SetMemoryLimit` + `os.Setenv` before `loadConfig` |
| `cmd/bouine/cmd/engine.go` | `SetGCPercent(50)` |
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

**Consistency:** The existing `InternKey` function
([headermap.go:59](pkg/header/headermap.go#L59)) uses a hand-rolled
`sync.Map`. Migrate `InternKey` to `unique.Make[string]` as well, so
both key and value interning use the same stdlib mechanism. This
removes the `keyIntern` global `sync.Map` and its manual store/load
pattern. `unique.Make` handles the deduplication internally with
the same semantics (process-lifetime deduplication, never collected).

### Tasks

5.1. Add `InternValue(s string) string` function in
`pkg/header/headermap.go` — wraps `unique.Make(s).Value()`. For
values already interned, this returns the existing string with no
allocation. For new values, it allocates once and caches.

5.2. Update `header.FromHTTP` to call `InternValue` on each header
value (after joining multi-value headers with `", "`) before storing
it in the `values` slice.

5.3. Migrate `InternKey` to use `unique.Make[string]` instead of the
hand-rolled `keyIntern sync.Map`. Remove the `keyIntern` variable.
`InternKey` becomes `return unique.Make(http.CanonicalHeaderKey(key)).Value()`.

5.4. Add `BenchmarkStoreFootprint_Interned` — measures the heap
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
| `pkg/header/headermap.go` | `InternValue`, migrate `InternKey` to `unique.Make`, update `FromHTTP` |
| `pkg/header/headermap_test.go` | Test interning deduplicates values |
| `internal/cache/handler_footprint_test.go` | `BenchmarkStoreFootprint_Interned` |

---

## Phase 6 — Pre-resolved Prometheus labels + `CoarseNow` on the hit path

**Problem:** The `DataPlaneMetrics.Middleware`
([dataplane.go:364](internal/observability/dataplane.go#L364)) calls
`WithLabelValues` (map hash + lookup) on every request — even though
the label tuple is one of a small fixed set determined by route +
cache result + status. It also calls `time.Now()` (25–40 ns) and
`time.Since` twice (25–40 ns each), plus `statusString`,
`normaliseCacheResult`, `normaliseSource`, and `HeaderVal` × 2 —
all on every hit.

**Fix:** Pre-resolve Prometheus label tuples at route init time
and call `.Inc()` on the pre-resolved counter directly. Replace
`time.Now()` with `platform.CoarseNow()` (2–4 ns) in the metrics
middleware on the hit path. Compute `time.Since` once and derive
both the duration and the millisecond value from it.

### Tasks

6.1. Pre-resolve `prometheus.Counter` and `prometheus.Observer`
instances for the common label tuples at route init. Store them
on the route struct (not a global map) — each route has a fixed
set of expected (method, status, cacheResult, source) tuples.
The middleware indexes into the route's pre-resolved array by
status + cacheResult (both are small enums). No hash computation
per request — just an array index.

The current `WithLabelValues` call takes five labels:
`r.Method`, `status`, `cacheResult`, `source`, `route`. The
pre-resolved array must cover all per-request-varying dimensions:
`method`, `status`, `cacheResult`, `source`. The `route` dimension is
implicit — the `routeMetrics` struct is stored per-route (see 6.2
for lookup mechanism).

```go
type routeMetrics struct {
    // [method][statusClass][cacheResult][source] → pre-resolved counter
    // method:      GET=0, HEAD=1, other=2 (3 slots — GET/HEAD cover >99% of hits)
    // statusClass: 2xx=0, 3xx=1, 4xx=2, 5xx=3, other=4 (5 slots)
    // cacheResult: HIT=0, MISS=1, STALE=2, REVALIDATED=3, BYPASS=4 (5 slots)
    // source:      HOT=0, WARM=1, PEER=2, ORIGIN=3, NONE=4 (5 slots)
    requestsTotal   [3][5][5][5]prometheus.Counter
    requestDuration [3][5][5][5]prometheus.Observer
    responseBytes   [3][5][5][5]prometheus.Counter
}
```

At 10 routes: 3×5×5×5×3 = 11,250 entries — still negligible. Each
request indexes the array directly from its `method`, `status`,
`cacheResult`, and `source` values — no hash, no `WithLabelValues`
call. The `method` dimension collapses to GET/HEAD/other (3 slots,
covering >99% of hit-path traffic). The `source` dimension covers
all `normaliseSource` return values (HOT/WARM/PEER/ORIGIN/NONE).

6.2. The middleware determines the route from `r.Header[header.XBouineRoute]`
after the handler returns. To avoid a per-request `map[string]*routeMetrics`
lookup, the `DataPlaneMetrics` struct holds a small fixed array of
`*routeMetrics` indexed by a route ID assigned at init time. Route IDs
are stored in a `map[string]int` built during `buildHandler` — this map
is read once per request (single hash lookup), but the subsequent array
index is O(1). The `WithLabelValues` path is kept as a fallback for
label tuples outside the pre-resolved range (e.g., unexpected status
codes like 1xx, or new cache results added in the future). The fallback
path increments a `metrics.fallbackLookups` counter for observability —
if it fires frequently, the pre-resolved set needs updating.

6.3. Replace `time.Now()` at [dataplane.go:366](internal/observability/dataplane.go#L366)
with `platform.CoarseNow()` on the hit path. `CoarseNow` uses
`CLOCK_REALTIME_COARSE` (~2–4 ns vs ~25–40 ns for `time.Now()`).
Compute `time.Since(start)` once and derive both the duration
(seconds, for the histogram) and the millisecond value (for rings)
from it — currently `time.Since(start)` is called twice (lines 391
and 413).

6.4. Add `BenchmarkRouter_Match` — measures the route lookup cost.
Gate: allocs/op = 0, ns/op < 50.

### What was dropped (and why)

**`http.ResponseController` (original task 6.1) is dropped.**
`ResponseController` caches type assertions for *optional* interfaces
(`Flusher`, `Hijacker`, `ReaderFrom`). The base `ResponseWriter`
methods (`Header()`, `WriteHeader()`, `Write()`) still go through
the interface — `ResponseController` just delegates to the wrapped
`http.ResponseWriter`. There is no dispatch savings on the hit path.
The only methods that benefit are `Flush()`, `Hijack()`,
`ReadFrom()` — none of which are called on a cache hit. It would
be theater.

### Memory impact

- The pre-resolved `routeMetrics` map is small (N routes × 5 status
  classes × 5 cache results × 3 metric types = 375 entries per
  route). At 10 routes: 3,750 entries — negligible.
- `CoarseNow` has no memory impact.

### Risk

- Pre-resolved labels must be invalidated if routes change at
  runtime (config reload). The existing config watcher rebuilds the
  handler chain on reload — the pre-resolved metrics are rebuilt
  too. This is the same lifecycle as the current `WithLabelValues`
  path (the `DataPlaneMetrics` struct is recreated on reload).
- `CoarseNow` has ~1 ms resolution. For p99 latency measurement,
  this is fine — p99 is ≥ 1 ms at saturation. For the micro-benchmark
  gate (ns/op), the metrics middleware is not in the benchmarked
  path, so this doesn't affect the gate.

### Files touched

| File | Changes |
|------|---------|
| `internal/observability/dataplane.go` | Pre-resolved label tuples, `routeMetrics` struct, `CoarseNow`, single `time.Since` |
| `cmd/bouine/cmd/builder.go` | Pre-resolve labels at route init |
| `internal/server/router_test.go` | `BenchmarkRouter_Match` |

---

## Phase 7 — Off-hit-path cardinality scaling fixes

**Problem:** The hit path is already lean (~298 ns, 0 allocs), but
several off-hit-path features scale poorly with cardinality (number
of cached objects, unique URLs, cluster nodes, scheduled refreshes).
Under heavy load with high cardinality, these features cause periodic
CPU spikes, lock contention, and p99 regressions that are invisible
in hit-only micro-benchmarks but show up in production as periodic
stalls.

**Scope:** This phase targets four features that scale with
cardinality. It does not affect the hit path directly — the goal is
to eliminate background CPU spikes and lock contention that steal
CPU cores from request serving under sustained load.

### 7A — Warm-tier compaction: eliminate index copy + minimize global lock hold

**Problem:** Compaction (`warm.go:Compact`, every 30 min when
≥30% dead bytes) snapshots the entire index under `idxMu.RLock`:
`make(map[uint64]warmLoc, len(s.index))` + a manual `for k, v := range`
copy — O(N) allocation where N = warm entries. At 1M warm entries,
that's a ~50 MB map allocation + O(N) copy. The global `s.mu.Lock`
(write lock) is then held during the final swap phase (file rename +
`NewStore` reopen + index replacement + SIEVE rebuild), blocking
all warm Puts for the duration of the swap — typically 100-500 ms,
not seconds as originally claimed. Gets are not blocked (they use
`s.mu.RLock` which is compatible with the read path, but the swap
acquires `s.mu.Lock` which blocks both).

The real costs are:
1. **Index snapshot allocation** — O(N) memory + time, under
   `idxMu.RLock` (read lock, doesn't block Gets but blocks Puts).
2. **Global write lock during swap** — blocks all Gets and Puts
   for 100-500 ms during the file rename + `NewStore` reopen +
   SIEVE rebuild.

**Fix:** Eliminate the index copy. Minimize the global lock hold time.

#### Tasks

7A.1. Eliminate the full index copy. Instead of `make(map[...], len(s.index))`
+ range loop, iterate the live index under `idxMu.RLock` without
copying — collect the live keys + locations into a pre-allocated
slice (`make([]warmLocPair, 0, len(s.index))`) instead of a map.
The slice is used to scan live records during compaction. This
changes the compaction scan from O(N) map lookups to O(N) slice
iteration — same time complexity, but eliminates the ~50 MB map
allocation.

7A.2. Prepare the new segment set + SIEVE list in the background
(outside any lock). The current code builds the new store via
`NewStore(dir)` which re-reads the compacted files. This is the
slow part (100-500 ms). Keep this outside the lock. **The background
phase must build both the new index and the new SIEVE list with
cross-referenced `loc.sieve` pointers** — the current `Compact` code
at [warm.go:1714-1720](internal/storage/warm/warm.go#L1714-L1720)
iterates all keys to set `loc.sieve` on each index entry. This O(N)
linkage step must happen *before* the swap, not during it. Only the
final pointer swap (`s.segs = fresh.segs`, `s.segByID = fresh.segByID`,
`s.index = newIndex`, `s.evictList = newList`) should be under
`s.mu.Lock` + `idxMu.Lock` — a few pointer assignments, ~1 µs.

7A.3. During the swap, briefly acquire `s.mu.Lock` + `idxMu.Lock`,
do the pointer assignments, release. This bounds the global write
lock hold time to microseconds instead of 100-500 ms.

7A.4. Add `BenchmarkWarmCompaction_1M` — populates 1M warm
entries, triggers compaction, measures wall-clock time and max
Get/Put latency during compaction. Gate: max Get/Put latency
during swap phase ≤ 1 ms (currently 100-500 ms).

#### Memory impact

- The index slice (`[]warmLocPair`) at 1M entries is ~16 MB
  (1M × 16 B per pair) vs the current map copy at ~50 MB. Net
  savings: ~34 MB peak during compaction.
- No extra index map in memory — the slice replaces the map copy.

#### Risk

- The slice-based scan changes the compaction iteration from map
  lookups to linear scan. Same O(N) complexity, but the constant
  factor is smaller (slice iteration vs hash map iteration).
- The background `NewStore` reopen reads the compacted files. If a
  Put arrives during this window, it writes to the old segments.
  The compaction must handle late writes to old segments: either
  re-scan after the swap and migrate late writes, or accept that
  objects written during compaction are in the old (uncompacted)
  segments and will be compacted in the next cycle. The latter is
  simpler and correct — compaction is best-effort deduplication, not
  a consistency barrier.
- The `s.mu.Lock` hold time is reduced to the pointer swap. If a
  Put arrives during the swap, it blocks for ~1 µs. This is
  acceptable — the existing code blocks for 100-500 ms.

#### Files touched

| File | Changes |
|------|--------|
| `internal/storage/warm.go` | Slice-based index scan, background segment prep, atomic swap |
| `internal/storage/warm_bench_test.go` | `BenchmarkWarmCompaction_1M` |
| `docs/decisions/0029-incremental-compaction.md` | ADR |

### 7B — Refresh scheduler: don't hold lock during `alive()` checks

**Problem:** The refresh scheduler's `compact()`
([scheduler.go:232](internal/cache/scheduler.go#L232), every 60s)
pops entries near `now + 5s` and calls `alive(key)` → `store.Get`
**while holding `s.mu.Lock`** — the scheduler's own mutex, not the
store's. This blocks `Schedule` calls (new refresh registrations)
for the duration of the alive checks. At 10K entries in the
near-future window, each `alive()` is a hot-tier Get (~50-100 ns), so
the total lock hold is ~1 ms — not a p99 problem, but unnecessary.
The code comment explicitly acknowledges this: "Holding the lock
prevents a concurrent Schedule from re-inserting the same key and
causing a double-pop."

**Fix:** Collect keys to check under the lock, release the lock, do
`store.Get` calls without holding it, re-acquire the lock to remove
dead entries.

#### Tasks

7B.1. Refactor `compact()` to collect the keys to check into a local
slice under `s.mu.Lock`, release the lock, call `alive(key)` for each
key without holding the lock, then re-acquire `s.mu.Lock` to remove
dead entries in bulk. This bounds the lock hold time to the collection
and removal (O(N) but cheap — slice + map delete), not the `store.Get`
calls (O(N) with shard RLock per key).

7B.2. Handle the double-pop race: after releasing the lock, a
concurrent `Schedule` can re-insert a key that's being checked. On
re-acquire, use `s.index[key]` to verify the entry still exists before
deleting — if it was re-scheduled, skip deletion. This is correct
because the re-scheduled entry has a new `refreshAt` and will be
checked in the next cycle.

7B.3. Add `BenchmarkSchedulerCompact_10K` — schedules 10K refreshes,
triggers `compact()`, measures wall-clock time and lock hold time.
Gate: lock hold time ≤ 100 µs (currently ~1 ms).

#### Memory impact

- No change — the `*heapEntry` allocation is unchanged. The fix is
  purely about lock hold time.

#### Risk

- Releasing the lock between collection and removal means new `Schedule`
  calls can insert entries during the alive checks. This is correct —
  the new entries are not in the compact set and will be checked in the
  next cycle. On re-acquire, verify `s.index[key]` still points to the
  same entry before deleting — if re-scheduled, skip deletion.
- If `alive(key)` returns a different result than the eventual removal
  (e.g., object evicted between check and removal), the removal is a
  no-op (map delete on absent key). Correct.
- The scheduler mutex is NOT the store mutex. Blocking `Schedule`
  does not block cache hits or store operations — only refresh
  registration. The impact is a ~1 ms delay on refresh scheduling
  every 60s, not a p99 regression.

#### Files touched

| File | Changes |
|------|--------|
| `internal/cache/scheduler.go` | Lock-free `alive()` checks in `compact()` |
| `internal/cache/scheduler_test.go` | `BenchmarkSchedulerCompact_10K` |

### 7C — URLRing: add sampling to reduce miss-path overhead

**Problem:** `URLRing.entries` (`rings.go:752`) is a `sync.Map`
keyed by URL prefix (capped at 512 entries), called on every non-HIT
request via `RecordURL`. The cap is 512 — not "thousands" or
"millions" — so the `sync.Map` growth is bounded and `Range` in
`URLStats` iterates at most 512 entries (~5 µs). The real overhead is
the per-miss `sync.Map.Load` call (~15-25 ns) + the atomic adds on
the `urlCounters` struct. Under high miss rates (10k misses/s),
this is ~200 µs/s of CPU — small but avoidable.

**Fix:** Add sampling. Keep the existing `sync.Map` (512 entries is
not a problem — replacing it with a bounded LRU would be
over-engineering for a 512-entry ring).

#### Tasks

7C.1. Add sampling to `RecordURL` — record only every Nth non-HIT
request (configurable, default 1:100, matching the access log
sampling rate). Use an `atomic.Int64` counter + modulo, not a hash.
This reduces `RecordURL` calls by 100× under high miss rates,
eliminating ~99% of the `sync.Map.Load` overhead.

7C.2. Add `urlRingSampleRate` to `config.Dashboard` (default 100).
When set to 0, `RecordURL` is never called (the nil-pointer check
in `Middleware` already skips when `Rings == nil`).

7C.3. Add `BenchmarkURLRing_RecordURL_1K` — measures `RecordURL`
with 1000 unique URLs at 1:100 sampling. Gate: allocs/op = 0,
ns/op < 100 (with sampling, 99% of calls are a single atomic increment
+ modulo check — ~5 ns).

#### Memory impact

- No change — the `sync.Map` is already capped at 512 entries.
- Sampling reduces memory writes by 100×.

#### Risk

- The 512-entry cap means rarely-seen URLs are already evicted
  (silently dropped when cap is reached). Sampling at 1:100 means a
  URL must have ≥100 misses to appear in the ring. For debugging a
  specific URL, operators can temporarily set `urlRingSampleRate=1`.
  Document this in the runbook.
- The existing TOCTOU race on the cap check (`r.size.Load() >= urlRingCap`
  then `LoadOrStore`) can overshoot by a few entries under concurrency.
  This is acceptable for a best-effort observability ring. Not fixed.

#### Files touched

| File | Changes |
|------|--------|
| `internal/observability/rings.go` | Sampling in `RecordURL` |
| `internal/config/config.go` | `urlRingSampleRate` field on `Dashboard` |
| `internal/observability/rings_test.go` | `BenchmarkURLRing_RecordURL_1K` |
| `docs/runbook/url-ring.md` | Document sampling + debug override |

### 7D — Peer fetch server-side: pool encode buffer + binary request

**Problem:** The peer fetch server handler (`peerfetch.go:288`,
`/v1/peer/fetch`) allocates per request: `io.ReadAll` (up to 4 KB),
`json.Unmarshal` into `PeerFetchRequest`, and `storage.EncodeObject(obj)`
which allocates a fresh buffer proportional to object size. Under
cluster load with 4 peers and 10k misses/s, this is 40k allocations/s.

The client side already uses `peerFetchBufPool` for the response buffer.
The server side does not pool.

**Fix:** Pool the encode buffer on the server side. Switch the request
from JSON to binary codec (the response already uses binary).

#### Tasks

7D.1. Pool the `EncodeObject` buffer on the server side. `EncodeObject`
currently allocates `make([]byte, 0, len(obj.Body)+256)` — a buffer
sized to the object body. Use a `sync.Pool` of `*[]byte` with a
grow-on-demand strategy: `Acquire` gets a buffer (initial cap 4 KB),
`append` into it (grows if needed), `Release` returns it to the pool
if `cap <= 64 KiB`, discards if larger (prevents pool from retaining
large buffers). The buffer is written to `w.Write(buf)` before
return to pool — `http.ResponseWriter.Write` copies into the
response buffer, so the pool buffer is safe to reuse after `Write`
returns. Verify with `-race`.

7D.2. Switch the peer fetch request from JSON to binary codec. The
request is a `PeerFetchRequest` with two fields (`Key uint64`,
`VaryKey string`). Encode this as a 10-byte binary header (1 byte
version + 8 byte key + 1 byte vary-key length + vary-key string).
This is ~10× faster than `json.Unmarshal` for a 2-field struct and
eliminates the `io.ReadAll` allocation (read exactly 10 + N bytes
from the body instead of `io.ReadAll(io.LimitReader(r.Body, 4096))`).

7D.3. Maintain backward compatibility: check the first byte of the
request body. If it's `{` (0x7B), parse as JSON (legacy client). If
it's the binary version byte, parse as binary. This allows rolling
upgrades — new servers handle both formats, new clients send binary.

7D.4. Add `BenchmarkPeerFetchHandler` — measures the server-side
peer fetch handler with a binary request + pooled encode buffer.
Gate: allocs/op = 0 for objects ≤ 64 KiB.

#### Memory impact

- Pooled encode buffers are reused — eliminates per-request `[]byte`
  allocation for the encoded object. At 10k misses/s × 4 peers, saves
  ~40k allocations/s.
- Binary request eliminates `io.ReadAll` (4 KB) + `json.Unmarshal`
  allocations per peer fetch.

#### Risk

- **Backward compatibility:** The binary request format must coexist
  with the JSON format during rolling upgrades. The first-byte check
  (0x7B vs version byte) is reliable — JSON always starts with `{`,
  binary starts with a version byte (currently 2). This is the same
  pattern used by the warm-tier codec.
- **Pool buffer retention:** Large encoded objects (> 64 KiB) are not
  pooled — they're returned to the GC. This prevents the pool from
  retaining large buffers. The 64 KiB threshold matches the existing
  `bodyThreshold` and `maxPreSerializeBodySize`.
- **Buffer lifetime:** The encoded buffer is written to the response
  before being returned to the pool. `http.ResponseWriter.Write` copies
  the data into the response buffer, so returning the pool buffer
  after `Write` is safe. Verify with `-race`.

#### Files touched

| File | Changes |
|------|--------|
| `internal/cluster/peerfetch.go` | Pool encode buffer, binary request parsing, backward compat |
| `internal/cluster/peerfetch_test.go` | `BenchmarkPeerFetchHandler`, binary request test |
| `docs/decisions/0030-peer-fetch-binary.md` | ADR |

---

## ~~Phase 8 — `ConnState` per-connection buffer pooling~~ (DROPPED)

**Dropped in review cycle 1.** The design was broken: `ConnState`
fires per-connection, but `r.Context()` is the request context, not
the connection context. Getting `net.Conn` from the handler requires
hijacking, which breaks keep-alive. A `sync.Map[net.Conn, *ResponseWriter]`
adds a map lookup per request — potentially slower than the
`sync.Pool` it replaces. The ~5% gain doesn't justify the complexity.
`sync.Pool` in Go 1.26 is already per-P (per-processor) and has low
contention — the `ResponseWriter` pool is not the bottleneck.

---

## Phase 9 — Custom HTTP/1.1 hit-path parser (bypass `net/http` on hits)

**Problem:** `net/http` is the single largest contributor to per-request
overhead on the hit path. Every request — even a cache hit — pays for:

1. `*http.Request` allocation (~400 B + header map + `*url.URL`).
2. Header canonicalization (every key run through
   `http.CanonicalHeaderKey`).
3. `http.ResponseWriter` allocation (per connection, per request).
4. Two-syscall response path (`WriteHeader` + `Write`), no `writev`.

Varnish parses HTTP in C into a fixed workspace with zero allocation
and writes responses via `writev`. The gap between bouine's 298 ns
handler and Varnish's ~1-2 µs total per-request cost is dominated by
`net/http` overhead — not the cache lookup, not the header write, not
the metrics.

**Fix:** Build a custom HTTP/1.1 request parser on top of `net.Conn`
that handles cache hits without ever constructing a `*http.Request` or
`http.ResponseWriter`. Misses and all non-GET/HEAD requests fall through
to `net/http` unchanged. HTTP/2 connections are handled by `net/http`
exclusively — the custom parser only runs on the cleartext (h2c) and
TLS HTTP/1.1 paths.

This is the same architecture Varnish uses: a custom HTTP parser for
the fast path, a general-purpose stack for everything else.

### Layering (AGENTS.md §3.1 compliance)

L1 (`internal/server`) may only depend on L7 and `/pkg/api`. The fast
path calls cache logic (L3: `Evaluate`, `VariantKey`) and storage (L2:
`store.Get`) — a direct L1→L2/L3 import would violate the layer rules.

The solution: **L1 declares a `FastPathHandler` interface; L3 implements it.**

```go
// internal/server/fastpath.go (L1)
// FastPathHandler is implemented by the cache layer (L3). L1 calls it
// through this interface — no upward import from L1 to L3.
type FastPathHandler interface {
    // TryHit attempts to serve a cache hit from the parsed request.
    // If the request qualifies (GET/HEAD, no conditional headers, cache hit),
    // it returns a non-nil FastPathResponse containing the serialized
    // status line + headers + body ready for writev.
    // If the request does not qualify (miss, conditional, range, etc.),
    // it returns nil — the caller falls through to net/http.
    TryHit(req *RawRequest, now time.Time) (*FastPathResponse, bool)
}

// FastPathResponse is the pre-serialized response for a cache hit.
// L1 writes it directly to net.Conn via net.Buffers — no http.ResponseWriter.
type FastPathResponse struct {
    Buffers      net.Buffers // [status_line, headers, body] — writev in one syscall
    HeaderBuf    []byte      // pooled buffer for status_line + headers; returned to pool after WriteTo
    StatusCode   int
    CacheResult  string // "HIT" or "STALE"
    Source       string // "hot", "warm", "peer"
    Route        string
    BytesOut     int
}
```

**Buffer ownership:** `Buffers[0]` (status line) and `Buffers[1]`
(headers) are slices of `HeaderBuf` — a pooled 4 KB buffer allocated by
L3 in `TryHit`. After `buffers.WriteTo(conn)` returns, L1 returns
`HeaderBuf` to the pool. `Buffers[2]` is `obj.Body` — owned by the cache
(L2), not pooled, and must not be modified or returned by L1.

`RawRequest` and `FastPathResponse` live in `internal/server` (L1) or
`/pkg/api` (leaf, importable by all). L3 implements `TryHit` by calling
`store.Get` + `EvaluateFromRaw` + `VariantKeyFromRaw` internally — L3
depends on L2 and `/pkg/api`, which is allowed. L1 only depends on the
interface and the types, both in L1 or `/pkg/api`.

### Architecture

```
net.Conn (accepted)
  │
  ├─ HTTP/2? → net/http (ALPN or h2c upgrade) — full stack, unchanged
  │
  └─ HTTP/1.1 → h1parser (L1, internal/server/h1parser)
       │
       ├─ Parse request line + headers into RawRequest (stack-allocated)
       │
       ├─ fastPathHandler.TryHit(rawReq, now)  [interface call → L3]
       │   │
       │   ├─ HIT: returns *FastPathResponse (pre-serialized headers + body)
       │   │       → buffers.WriteTo(conn) — single writev syscall. Done.
       │   │
       │   └─ NOT QUALIFIED: returns nil, false
       │       → construct *http.Request from RawRequest
       │       → call existing http.Handler chain (net/http)
       │       → one allocation, miss path — acceptable.
```

The h1parser is pure L1: it parses bytes, calls `time.Now()` once per
request (threaded through `TryHit` for freshness, `Date` header
serialization, and metrics duration — one `time.Now()`, not three), calls
the `FastPathHandler` interface, and writes the response. It never imports
`internal/cache` or `internal/storage`. The cache layer (L3) implements
`FastPathHandler` and is wired in by `cmd/bouine/cmd/builder.go` at
startup — the same place where the existing `http.Handler` chain is
assembled.

### Key design decisions

1. **HTTP/1.1 only.** The custom parser runs only when the connection
   speaks HTTP/1.1. HTTP/2 connections (ALPN `h2` or h2c upgrade) go
   to `net/http` directly. The listener peeks the first bytes: if the
   connection preface is `PRI * HTTP/2.0` (h2c upgrade) or the ALPN
   negotiated `h2` (TLS), hand off to `net/http`. Otherwise, use the
   custom parser.

2. **Fall-through, not fall-back.** When `TryHit` returns false (miss,
   conditional, range, non-GET/HEAD, invalidating method), the h1parser
   constructs a `*http.Request` from its already-parsed `RawRequest` and
   calls the existing `http.Handler` chain. This is one allocation on the
   miss path — acceptable. The custom parser does not re-parse; it
   populates the `*http.Request` fields from its own struct.

3. **`net.Buffers` for responses (writev).** The `FastPathResponse`
   contains a `net.Buffers` (a `[][]byte`) with the status line, header
   block, and body. `buffers.WriteTo(conn)` uses `writev` on Linux/macOS
   when the connection is a `*net.TCPConn` — a single syscall. No
   platform-specific code needed. The header block is serialized by L3
   (in `TryHit`) from `header.Map` — if Phase 2 is landed, the headers
   are already clean (no `X-Bouine-*`, no `no-cache` fields). If Phase 2
   is not landed, `TryHit` performs the stripping at serve time.

4. **No `http.ResponseWriter` on the fast path.** The fast path writes
   directly to `net.Conn`. This means the metrics middleware cannot
   observe the response via the `ResponseWriter` wrapper. Instead, the
   h1parser increments metrics directly from the `FastPathResponse` fields
   (`StatusCode`, `CacheResult`, `Source`, `Route`, `BytesOut`). The
   access log is sampled at 1:100 as before.

5. **Connection reuse.** The h1parser handles keep-alive in a loop:
   parse request → TryHit or fall through → read next request. The
   `net.Conn` is returned to `net/http` only when a fall-through happens
   (miss path). On a hit, the parser loops back for the next request
   without yielding to `net/http`.

6. **Feature flag.** Gated by `experimental.h1_fast_path bool` in
   config, default off. Operators opt in. The existing `net/http` path
   remains the default and is fully supported.

### What the fast path handles

A request qualifies for the fast path if **all** of these are true:

- Method is GET or HEAD.
- No `Cache-Control: no-cache` or `no-store` in the request.
- No conditional request headers (`If-None-Match`, `If-Modified-Since`,
  `If-Range`, `Range`).
- `TryHit` (L3) calls `store.Get` + `EvaluateFromRaw` + `VariantKeyFromRaw`
  internally and returns a `*FastPathResponse` on hit/stale-hit (including
  Vary'd objects). If any of these checks fail, `TryHit` returns false
  and the h1parser falls through to `net/http`.
- `tracing.Enabled()` returns false (or the fast path attaches a span
  after the fact, see Risk below).

Anything else falls through to `net/http`. With `VariantKeyFromRaw` inside
`TryHit`, this covers >90% of production hit-path traffic (simple GET →
cache hit → 200 OK, including Vary'd objects).

### Tasks

9.1. Create `internal/server/h1parser/` package (L1). Implement an
HTTP/1.1 request parser that reads from `net.Conn` into a stack-allocated
`RawRequest` struct:
```go
// RawRequest is the parsed HTTP/1.1 request. Lives in internal/server (L1)
// so both the h1parser and the FastPathHandler interface can reference it
// without upward imports.
type RawRequest struct {
    Method   string // sliced from read buffer, no allocation
    Path     string
    Query    string
    Host     string
    Headers  [maxHeaders]RawHeader // fixed array, no map
    NHeaders int
    // ... parsed from the read buffer in place
}
```
The parser reads the request line + headers in a single `conn.Read`
into a pooled 16 KB buffer (covers 99.9%+ of real-world headers; the
`MaxHeaderBytes` 64 KiB cap is handled by falling through to `net/http`
on overflow). Parse is line-based, no `bufio.Scanner` (allocates). Gate:
`BenchmarkH1Parse_Get` — allocs/op = 0, ns/op < 200.

9.2. Create `internal/server/fastpath.go` (L1). Define the
`FastPathHandler` interface and `FastPathResponse` type (see Layering
section above). These types live in L1 so the h1parser can reference
them without importing L3.

9.3. Implement the `FastPathHandler` in `internal/cache/` (L3). The
implementation (`fastpath.go` in L3) holds a reference to `storage.Store`
(L2) and the cache config. Its `TryHit` method:
  a. Checks `req.Method` is GET/HEAD, no `no-cache`/`no-store`, no
     conditional headers — all read from `RawRequest` fields.
  b. Calls `BuildKeyFromRaw(req)` to compute the cache key (same xxhash64
     logic as `BuildKey`, reading from `RawRequest` instead of
     `*http.Request`). Gate: allocs/op = 0, ns/op ≤ `BenchmarkBuildKey`.
  c. Calls `store.Get(ctx, key)` → obj.
  d. Calls `EvaluateFromRaw(req, obj, now)` (same logic as `Evaluate`,
     reading from `RawRequest` fields). Gate: allocs/op = 0, ns/op ≤
     `BenchmarkEvaluate_Hit`.
  e. If the object has `Vary`, calls `VariantKeyFromRaw(key,
     obj.Header.Get(Vary), req, exclude)` and re-fetches the variant.
     Gate: allocs/op = 0, ns/op ≤ `BenchmarkBuildKey`.
  f. If hit/stale-hit: serializes the response into `FastPathResponse.Buffers`
     (a `net.Buffers` with [status_line, headers, body]). The status line +
     headers are serialized into a pooled 4 KB buffer; the body is
     `obj.Body` directly (zero-copy). Sets `StatusCode`, `CacheResult`,
     `Source`, `Route`, `BytesOut`. Returns `(*FastPathResponse, true)`.
  g. If not qualified: returns `(nil, false)`.

`EvaluateFromRaw`, `BuildKeyFromRaw`, and `VariantKeyFromRaw` are all
internal to L3 — L1 never sees them. L1 only calls `TryHit` through the
`FastPathHandler` interface.

9.4. Implement the fast-path serve (hit path) in the h1parser (L1).
After `TryHit` returns a hit, write `response.Buffers` to `conn` via
`buffers.WriteTo(conn)` — a single `writev` syscall. After writing,
return the pooled header buffer to the pool (see Buffer ownership below),
increment metrics (task 9.7), and loop back for the next keep-alive request.

Gate: `BenchmarkFastPathServe_200` — allocs/op = 0, ns/op < 500.

9.5. Implement the fall-through path (miss path) in the h1parser (L1).
When `TryHit` returns false, construct a `*http.Request` from `RawRequest`
and call the existing `http.Handler` chain. The `*http.Request` is built
from the already-parsed fields — no re-parsing. The connection is handed
to `net/http` for the remainder of its lifetime (the miss path may do
origin fetch, streaming, etc. — `net/http` handles all of that). The
h1parser does not loop back after a fall-through.

9.6. Implement the connection router in `internal/server/listener.go`.
After accepting a `net.Conn`, peek the first bytes:
- TLS connection with ALPN `h2`: hand to `net/http` (existing path).
- Cleartext connection starting with `PRI * HTTP/2.0`: h2c upgrade,
  hand to `net/http` (existing path).
- Otherwise: HTTP/1.1, use the custom parser. The parser handles
  keep-alive in a loop. On fall-through, the `*http.Request` +
  remaining connection is passed to `net/http`.

Gate behind `experimental.h1_fast_path` config flag. When false, all
connections go to `net/http` as today. The `FastPathHandler` is passed
from `builder.go` (which wires L3→L1) — when the flag is off, `nil` is
passed and the h1parser is not used.

9.7. Wire metrics into the fast path (L1). The fast path does not go
through the `DataPlaneMetrics.Middleware`, so the h1parser increments
metrics directly from `FastPathResponse` fields. After serving a hit:
- Increment `RequestsTotal` with `(method, status, "HIT", source, route)`.
- Observe `RequestDuration` with the elapsed time.
- Increment `ResponseBytesOut` with `len(obj.Body)`.

If Phase 6 is landed, use the pre-resolved `routeMetrics` array. If
not, use `WithLabelValues` — one hash, acceptable on the fast path
since the total saving dwarfs the hash cost. The access log is sampled
at 1:100 via `atomic.AddInt64` + modulo, same as the middleware.

9.8. Wire tracing into the fast path (when active). If
`tracing.Enabled()` returns true, the fast path creates a span *after*
the response is written (not before — the span is created from the
parsed request fields, not from `r.Context()`). The span records
`http.method`, `url.path`, `http.host`, `http.status_code`, and
`cache.result`. This is a single span, not a middleware chain — the
overhead is ~100 ns, acceptable when tracing is opted in.

9.9. Handle `X-Cache` and `X-Cache-Source` headers in `TryHit` (L3).
These are set directly in the serialized header buffer — no
`http.Header` map involved. `X-Cache: HIT` (or `STALE`) and
`X-Cache-Source: hot` (or `warm`/`peer`) are appended to the header
block before writing. Pre-allocate the string values (same as
`headerHIT`, `headerSTALE`, `sourceSlice` today).

9.10. Handle `Age` header in `TryHit` (L3). `ComputeAge(obj, now)`
returns `time.Duration`. `ageHeader` converts to `[]string` via
`strconv.Itoa(int(d.Seconds()))` with a pre-allocated 600-entry cache
([handler.go:129-145](internal/cache/handler.go#L129)). The fast path
must replicate this: convert to seconds, format via `strconv.AppendInt`
into the header buffer (zero-alloc). The `time.Now()` call is
unavoidable (same as the existing handler — `Fresh()` needs nanosecond
precision). Do not use `CoarseNow` here (see handler.go:704 comment).

9.11. Add `BenchmarkFastPath_Hit` — end-to-end fast path: parse a
realistic HTTP/1.1 GET request → `store.Get` → `EvaluateFromRaw` →
`writev` response. Gate: allocs/op = 0, ns/op < 1000 (target: ~500 ns
including all overhead vs ~5-10 µs for `net/http` end-to-end).

9.12. Add `BenchmarkFastPath_Fallthrough` — parse a request that misses,
construct `*http.Request`, call the handler chain. Gate: allocs/op ≤ 2
(the `*http.Request` + header map; this is the miss path, not gated to
zero).

9.13. Add k6 gate: 0.1 uncapped throughput with `h1_fast_path=true`.
Gate: max RPS ≥ previous + 20%. Compare against Varnish on the same
hardware. Secondary gate: p99 server latency ≤ Varnish + 5%.

9.14. Add conformance gate: run `cache-tests` with
`h1_fast_path=true`. No regressions allowed. The fast path must produce
byte-identical responses to the `net/http` path for all hit cases.

### Prerequisites

- **Phase 2 (Put-time header work) is strongly recommended before Phase 9.**
  The fast-path response serialization reads from `header.Map` directly.
  If `stripNoCacheFields` and `X-Bouine-*` removal are already done at
  `Put` time (Phase 2), the fast path writes headers as-is with zero
  post-processing. Without Phase 2, the fast path must replicate the
  `stripNoCacheFields` + `dst.Del` logic, adding per-hit work that
  defeats the purpose.
- **Phase 1 (tracing skip) is required.** The fast path assumes tracing
  is off by default. If tracing is wired unconditionally, the fast path
  must create a span per request, adding ~100 ns — not fatal but
  wasteful when tracing is not configured.

### Memory impact

- Each connection in the fast path uses a pooled 16 KB read buffer and a
  pooled 4 KB header write buffer. At 10k connections, that's ~200 MB
  of pooled buffers — but they're pooled, so steady-state memory is
  `GOMAXPROCS × 20 KB` = ~160 KB. Negligible.
- No `*http.Request` allocation on hits — saves ~400 B + header map
  (~200 B) per hit. At 100k hits/s, that's ~60 MB/s of avoided
  allocation.

### Risk

- **Timeout enforcement (security-critical).** The custom parser
  bypasses `*http.Server`, which enforces `ReadHeaderTimeout` (10s),
  `ReadTimeout` (30s), `WriteTimeout` (5m), and `IdleTimeout` (120s)
  via `conn.SetReadDeadline` / `conn.SetWriteDeadline`. Without these
  deadlines, the fast path is vulnerable to slowloris attacks (slow
  header sending), stuck write clients, and idle keep-alive fd leaks.
  The custom parser must enforce its own deadlines:
  - Before reading the request line + headers: `conn.SetReadDeadline(time.Now().Add(readHeaderTimeout))`.
  - Before writing the response: `conn.SetWriteDeadline(time.Now().Add(writeTimeout))`.
  - Before each keep-alive loop iteration: `conn.SetReadDeadline(time.Now().Add(idleTimeout))`.
  These timeout values are passed from `ListenerConfig` into the custom
  parser. This is not optional — it is a security requirement (AGENTS.md
  §6: resource exhaustion defense, §11: cancellation honored within 10 ms).
- **HTTP/1.1 parsing correctness.** The custom parser must handle:
  chunked transfer encoding (fall through to `net/http`), `Expect:
  100-continue` (fall through), pipelined requests (parse next request
  from remaining buffer), connection close semantics, trailer headers.
  The parser does not need to handle HTTP/1.0 keep-alive quirks —
  fall through to `net/http` for HTTP/1.0. Test with the fuzz corpus
  from the header parser fuzzer + new `h1parser` fuzzer.
- **Response correctness.** The fast path must produce byte-identical
  responses to `net/http` for the same cached object. This means:
  correct `Content-Length`, `Content-Type`, `Date` header (set by
  `net/http` normally via `time.Now().UTC().AppendFormat` — the fast
  path must add it if the stored object doesn't have one, using
  `time.Now().UTC().AppendFormat` into the header buffer),
  `Connection: keep-alive` (or `close`), and correct header
  capitalization (HTTP/1.1 is case-insensitive, but some clients are
  buggy — match `net/http`'s canonical form for safety). The conformance
  gate (9.13) catches this.
- **Metrics accuracy.** The fast path bypasses the middleware chain.
  Metrics must be incremented correctly — same labels, same values.
  A metrics drift test compares fast-path vs `net/http` path counters
  after N requests.
- **Tracing span context.** When tracing is active, the fast path
  creates the span after the response is written. The span is not
  propagated to downstream code (there is no downstream code on the
  fast path). If tracing of miss-path requests requires the span to be
  in the request context, the fall-through path must create the span
  before calling the handler chain. This is handled in 9.7.
- **TLS connections.** On the TLS listener, ALPN negotiation happens
  before the application sees data. If ALPN selects `http/1.1`, the
  custom parser can run on the TLS-wrapped `net.Conn`. The parser
  reads from the decrypted stream — no change to the parsing logic.
  Verify with the TLS test harness.
- **`net.Buffers` / `writev` cross-platform.** `net.Buffers.WriteTo`
  uses `writev` on Linux/macOS when the writer is a `*net.TCPConn` and
  falls back to sequential `Write` calls otherwise. No platform-specific
  code in the fast path. Benchmark both Linux and macOS to confirm
  the `writev` path is taken on TCP connections.
- **Connection handoff on fall-through.** When the fast path falls
  through to `net/http`, it must hand off a half-read connection where
  some bytes may have been consumed from the read buffer. The
  `*http.Request` constructed from `RawRequest` carries the already-
  parsed headers. The connection is passed via a custom `net.Listener`
  that returns the already-accepted `net.Conn`. The remaining read
  buffer (if any) must be prepended to the connection's read stream —
  use a `bufio.Reader` wrapper or an `io.MultiReader` of the remaining
  bytes + the `net.Conn`. This is the trickiest part of the
  implementation. Test with pipelined requests where the second
  request is a miss.
- **Feature flag default.** The fast path defaults to off
  (`experimental.h1_fast_path: false`). Operators opt in per route or
  globally. The flag is removed (fast path becomes default) only after
  soak testing + conformance + no regressions for one release cycle.

### Files touched

| File | Layer | Changes |
|------|-------|---------|
| `internal/server/h1parser/parser.go` | L1 | HTTP/1.1 request parser, `RawRequest` struct |
| `internal/server/h1parser/serve.go` | L1 | Fast-path serve via `net.Buffers`, fall-through to `net/http` |
| `internal/server/h1parser/deadlines.go` | L1 | Timeout enforcement (`SetReadDeadline`/`SetWriteDeadline`) |
| `internal/server/h1parser/fallthrough.go` | L1 | `*http.Request` construction from `RawRequest` |
| `internal/server/fastpath.go` | L1 | `FastPathHandler` interface, `FastPathResponse` type, `RawRequest`/`RawHeader` types |
| `internal/server/h1parser/parser_test.go` | L1 | Parser unit tests, fuzz tests |
| `internal/server/h1parser/serve_test.go` | L1 | `BenchmarkFastPath_Hit`, `BenchmarkFastPath_Fallthrough` |
| `internal/cache/fastpath.go` | L3 | `FastPathHandler` implementation: `TryHit`, `BuildKeyFromRaw`, `EvaluateFromRaw`, `VariantKeyFromRaw`, response serialization |
| `internal/cache/fastpath_test.go` | L3 | `TryHit` unit tests, `EvaluateFromRaw`/`VariantKeyFromRaw` benchmarks |
| `internal/server/listener.go` | L1 | Connection router (H2 vs H1 custom parser), feature flag |
| `internal/config/config.go` | — | `experimental.h1_fast_path` field |
| `cmd/bouine/cmd/builder.go` | — | Wire `FastPathHandler` (L3) into listener (L1) |
| `internal/observability/dataplane.go` | L7 | Fast-path metrics increment (direct, not middleware) |
| `internal/observability/tracing/tracing.go` | L7 | Fast-path span creation (when tracing active) |
| `docs/decisions/0031-h1-fast-path.md` | — | ADR |

---

## Implementation Order & Dependencies

```
Phase 1 (tracing skip)         — no dependencies, trivial, ship first
Phase 4 (GOGC + GOMEMLIMIT)    — no dependencies, trivial, ship early
Phase 5 (header interning)     — no dependencies, small, ship early
Phase 3 (SO_REUSEPORT)         — no dependencies, medium, ship after 1+4
Phase 2 (Put-time header work) — no hard code dependency on Phase 1;
                                  recommended ordering for clean
                                  benchmark baselines, not a merge blocker
Phase 6 (pre-resolved labels)  — no hard dependency on Phase 2;
                                  recommended ordering, not a merge blocker
Phase 7 (cardinality scaling)  — no dependencies on 1-6, ship in parallel
                                  7A is high-risk (warm tier), 7B-7D are low-risk
Phase 9 (H1 fast-path parser)  — depends on Phase 1 (required) and
                                  Phase 2 (strongly recommended);
                                  largest single-phase impact, ship last
```

### Expected cumulative impact

| Phase | RPS gain (cumulative) | p99 latency | Memory |
|-------|----------------------|-------------|--------|
| Baseline | — | — | — |
| + Phase 1 | +5-10% | -5% | 0 |
| + Phase 4 | +0% | -10-20% | neutral |
| + Phase 5 | +0-3% (indirect, less GC) | -5% (less GC) | -50MB at 1M |
| + Phase 3 | +15-25% on multi-core | -5% | +64KB |
| + Phase 2 | +5-10% (cumulative +25-45%) | -5% | -48MB (net savings) |
| + Phase 6 | +2-5% | -2% | neutral |
| + Phase 7 | +0-5% (background CPU reclaimed) | -10-50% (eliminates compaction stall) | +0 MB (slice replaces map copy) |
| + Phase 9 | +30-50% (cumulative +60-90%) | -10-20% (zero-alloc hit path + writev) | -60 MB/s avoided allocs |

**Target:** within 10% of Varnish RPS-per-core, p99 ≤ Varnish + 5%,
RSS ≤ current + 5%.

> **Phase 9 is the phase that closes the gap.** Phases 1-7 chip away
> at overhead (10-25% cumulative). Phase 9 eliminates the dominant
> cost: `net/http` per-request allocation + header canonicalization +
> two-syscall response. The custom H1 parser + `writev` response
> targets ~500 ns end-to-end on the hit path vs ~5-10 µs through
> `net/http`. This is where bouine reaches parity with Varnish's
> per-request cost on HTTP/1.1. HTTP/2 remains on `net/http` (Varnish
> also delegates H2 to a separate process).

### Impact table notes (revised after review)

- **Phase 2** revised from +15-25% to +5-10%. The original plan
  claimed savings from eliminating a `WriteTo` "conversion" that
  doesn't exist — `WriteTo` is already zero-alloc direct map
  assignment. The real savings are 2 map deletes + 1
  `stripNoCacheFields` skip (~50-100 ns on a 298 ns/op path = 5-10%).
  Memory impact revised from +320 MB to **-48 MB** (net savings from
  removing internal headers from `header.Map`).
- **Phase 5** RPS gain revised from +5-10% to +0-3%. Interning is on
  the miss path (`FromHTTP` at `Put` time), so it reduces GC scan
  time, not per-request CPU. The RPS gain is indirect (less GC
  pressure → shorter pauses → better p99 → higher sustainable RPS).
- **Phase 6** `ResponseController` sub-task dropped (no dispatch
  savings on hit path). `CoarseNow` sub-task added (saves ~50-75 ns
  per request from clock calls).
- **Phase 7** does not improve the hit path directly. It eliminates
  periodic CPU spikes from warm compaction (index copy + global
  write lock during swap), refresh scheduler lock contention, URLRing
  per-miss overhead, and peer fetch per-request allocations. The RPS
  gain is indirect: background CPU reclaimed from these spikes is
  available for request serving. The p99 improvement is from
  eliminating periodic stalls. 7A is the highest-impact item — at 1M
  warm entries, the index copy allocates ~50 MB and the global write
  lock blocks all I/O for 100-500 ms during the swap. The fix
  eliminates the index copy (slice instead of map) and reduces the
  lock hold to a pointer swap (~1 µs). 7B is lower severity than
  originally claimed — the scheduler lock blocks refresh registration
  for ~1 ms every 60s, not a p99 problem. 7C is simplified from a
  bounded LRU replacement to just sampling — the `sync.Map` is capped
  at 512 entries, not a cardinality problem. 7D's pooling strategy
  uses grow-on-demand with a 64 KiB discard threshold.
- **Phase 9** is the single highest-impact phase. It bypasses `net/http`
  on cache hits — eliminating `*http.Request` allocation (~600 B),
  header canonicalization, `http.ResponseWriter` allocation, and the
  two-syscall response path. The custom H1 parser + `writev` response
  targets ~500 ns end-to-end vs ~5-10 µs through `net/http`. This is
  the same approach Varnish uses (custom HTTP parser, zero-allocation
  workspace, `writev` responses). HTTP/2 and HTTP/3 are unaffected —
  they stay on `net/http` (Varnish also delegates H2 to a separate
  process). The +30-50% RPS estimate is grounded: `net/http` overhead
  is ~5-10 µs per request; eliminating it on a path that currently
  takes ~5-10 µs total is a 50-100% improvement on the hit path, but
  saturation RPS is bounded by kernel network stack + connection
  handling, so the real-world gain is 30-50%. The feature flag
  (`experimental.h1_fast_path`, default off) lets operators opt in
  incrementally. Phase 2 is a prerequisite so the fast-path header
  serialization needs zero post-processing.
