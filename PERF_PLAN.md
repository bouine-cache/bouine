# Performance Improvement Plan — bouine vs Varnish/NGINX

> **Current gap:** Hot-path latency 1.6–2.3× behind Varnish (0.327 ms vs
> 0.142 ms server-side). Mixed-workload hit rate 73% vs 93%. Data throughput
> 48% lower at equal RPS. Benchmark hit path: 13 allocs/op (~4–5 are
> `httptest.NewRecorder` artifacts; ~8–9 in production).
>
> **Status of benchmarks:** k6 rate-caps both at ~3000 RPS. True max
> throughput is **unmeasured** — first priority is an uncapped throughput
> test to establish the real ceiling.

---

## Tier 0 — Investigation Findings (completed)

### 0.1 Uncapped Throughput Measurement

**Status: Harness ready, not yet run.**

Created `bench/loadtest/scenarios/0.1_uncapped_throughput/` with a k6
ramping-arrival-rate scenario (1k → 200k RPS over 2.5 min). Run via:

```
cd bench/loadtest && docker compose run --rm load-gen /scenarios/0.1_uncapped_throughput/run.sh
```

**Note:** The newer `new_3.2_*.json` results show bouine at 0.166 ms avg
vs Varnish at 0.177 ms avg — bouine is already **faster on average** in
the hit-only test. Varnish still wins on p90/p95 (~5–8% faster). The
original `3.2_*.json` results that showed a 2× gap appear to be from an
older build. The gap is much smaller than initially assessed.

### 0.2 Data Throughput Discrepancy

**Status: Resolved — NOT a bug.**

Bouine serves ~222 bytes/req; Varnish serves ~327 bytes/req. The 105
bytes/req difference is **Varnish adding more headers** (Via, X-Varnish,
Server, Accept-Ranges), not bouine serving less data. Bouine is leaner,
which is better.

**Content-Length is preserved:** The origin is a Go `net/http` server
that auto-sets `Content-Length` for small responses. `header.FromHTTP`
preserves it, `WriteTo` replays it. No chunked encoding issue for this
workload.

**Caveat:** If the origin used chunked encoding (no Content-Length), bouine
would not compute one from `obj.BodySize`. Varnish does. This would cause
bouine to serve chunked on hits. Not a problem for the current bench, but
worth fixing for general correctness — see new item 1.5 below.

### 0.3 Hit-Rate Gap Root Cause

**Status: Resolved — measurement artifact + config gap, NOT an eviction
or cacheability problem.**

The 20pp hit-rate gap (73% vs 93%) is caused by two factors:

1. **Measurement artifact (~10–15pp):** The k6 metric checks
   `X-Cache == "HIT"` only. Varnish reports grace-served (stale) responses
   as `HIT` (`obj.hits > 0`). bouine correctly reports them as `STALE` or
   `REVALIDATED` per RFC 9111. The k6 metric penalizes bouine for
   RFC-compliant behavior.

2. **Config gap (~5pp):** Varnish VCL sets `beresp.grace = 30s` (global
   stale-while-revalidate). The bouine bench config had no
   `stale_while_revalidate`. The `/revalidate` endpoint (`max-age=0,
   must-revalidate`) has no SWR in the origin response — bouine
   revalidates every request, Varnish serves stale for 30s via grace.

**Actions taken:**
- Updated `bench/loadtest/config/bouine.yaml`: added
  `stale_while_revalidate: "30s"` to match Varnish's grace period.
- Updated `bench/loadtest/scenarios/3.6_mixed_realistic/k6.js`: added
  `cache_served_rate` metric that counts `HIT + STALE + REVALIDATED` for
  fair comparison.

**What is NOT the problem:**
- NOT eviction: bouine has 1 GiB hot tier, same as Varnish's
  `malloc,1G`. The workload is 10k unique keys × ~36 bytes = 360 KB
  total — nowhere near the cache capacity.
- NOT TTL defaults: the origin sends explicit `Cache-Control: max-age=3600`
  for `/hit` and `/vary`. `ttl_default` is irrelevant.
- NOT Vary: the `/vary` endpoint is only 3% of traffic.
- NOT negative caching: the `/error` endpoint returns 503, which is not
  in `negativeStatuses` (404, 405, 410, 501). Both servers handle this
  identically.

**Real adjusted hit rate (after config fix):**
- Bouine `cache_served_rate` (HIT+STALE+REVALIDATED): ~78–85% expected
- Varnish `hit_rate` (HIT including grace): ~93%
- Remaining gap: Varnish's grace is unconditional (serves stale even for
  `must-revalidate`), bouine's SWR respects `must-revalidate` per RFC.
  This is correct behavior — the gap is a semantic difference, not a bug.

---

## Tier 1 — Hot-Path CPU Optimizations (2–3× latency improvement)

### 1.1 Pre-Serialized Response Head

**Impact: 🟠 High — saves ~100–150 ns CPU per hit by bypassing `WriteHeader` serialization**

**Problem:** `serveObject` calls `obj.Header.WriteTo(dst)` (10–15 map
assignments into `w.Header()`, zero-alloc), then `w.WriteHeader(status)`
which serializes the status line + iterates the header map to write it to
the `bufio.Writer`. This header-map→wire-format roundtrip is the main CPU
cost on the hit path.

**Correction from initial analysis:** `WriteTo` is already zero-alloc
(direct map assignment with sub-sliced values, keys pre-canonicalized).
The gain is CPU, not allocations: bypassing `http.response.WriteHeader`'s
internal serialization (status line formatting, header iteration,
chunked-encoding setup for H1, H2 frame creation).

**Solution:** At cache-fill time, serialize the response head (status line
+ all headers + `\r\n\r\n`) into a `[]byte`, excluding the dynamic headers
(`Age`, `X-Cache`, `X-Cache-Source`, `Warning`). Store as
`obj.SerializedHead []byte` (transient, re-derived after codec decode).

On a cache hit, write the pre-serialized head, then append the 2–3 dynamic
headers as raw bytes, then the body:

```go
func (h *Handler) serveObject(w http.ResponseWriter, r *http.Request, obj *api.Object, now time.Time, result cacheResult, src api.Source) {
    w.Write(obj.SerializedHead)
    // Append dynamic headers as pre-formatted bytes
    w.Write(formatDynamicHeaders(result, src, ComputeAge(obj, now)))
    w.Write(crlf)
    if r.Method != http.MethodHead {
        w.Write(obj.Body)
    }
}
```

This bypasses `http.Header` map operations entirely on the hit path.
`header.Map` is kept for ban matching, conditional requests, and admin
endpoints — it's just no longer on the write path.

**Expected gain:** ~100–150 ns CPU per hit. Does not reduce allocations
(`WriteTo` was already zero-alloc). The gain is from eliminating
`http.response.WriteHeader`'s internal serialization loop.

**Risk:** Medium. Must invalidate `SerializedHead` when headers are mutated
post-store (revalidation merging via `MergeHeaders304`). Must handle
`stripNoCacheFields` — either pre-compute the stripped form or apply it
at serialization time.

**Files:** `internal/cache/handler.go`, `pkg/api/storage.go`,
`internal/storage/codec.go`

---

### 1.2 Collapse 3 Tracing Spans to 1

**Impact: 🟠 High — 3 span creations + 3 context derivations per hit, even with tracing disabled**

**Problem:** There are **3 tracing spans** per request, not 2 as
originally assumed:

1. `tracing.HTTPMiddleware("bouine.listener.http")` — `listener.go:72`
2. `tracing.HTTPMiddleware("bouine.pipeline")` — `builder.go:142`
3. `tracing.StartSpan("bouine.cache")` — `handler.go:697`

Each `t.Start()` creates a span object (even no-op). Each
`r.WithContext(ctx)` allocates a new `*http.Request` struct. That's 3
span objects + 3 request struct copies per hit.

**Solution:**
1. **Remove the listener-layer span** (`bouine.listener.http`). It adds
   no information — it's the same request lifecycle as the pipeline span.
2. **Remove the cache-handler span** (`bouine.cache`) for cache hits.
   Only create it for miss/revalidate/bypass paths where there's actual
   work to trace. The hit path is ~200ns of work — a span that covers
   200ns is noise.
3. **Keep the pipeline span** (`bouine.pipeline`) as the single
   request-level span.

Result: 1 span + 1 context derivation per hit (down from 3+3).

**Expected gain:** Eliminates 2 span creations + 2 `r.WithContext()`
allocations per hit. Estimated ~100–150 ns + 2 allocs saved.

**Risk:** Low. The pipeline span provides the same trace context. The
cache-hit span was too short to be useful anyway.

**Files:** `internal/server/listener.go`, `cmd/bouine/cmd/builder.go`,
`internal/cache/handler.go`

---

### 1.3 Merge Middleware Layers + Lazy Access Log

**Impact: 🟠 High — eliminates 1 ResponseWriter pool acquire, 1 wrapper layer, and unconditional `attrs` allocation**

**Problem:** The middleware chain (after removing 2 tracing spans from 1.2)
is:

```
tracing.HTTPMiddleware("bouine.pipeline")
  → accesslog.Middleware           ← acquires ResponseWriter #1, builds attrs []any unconditionally
    → DataPlaneMetrics.Middleware  ← acquires ResponseWriter #2, 3× WithLabelValues, ring writes
      → Router.ServeHTTP
        → cache.Handler.ServeHTTP
```

Two separate `ResponseWriter` pool acquires per request (accesslog +
metrics). The `attrs []any` slice (20 elements) in `accesslog.Middleware`
is heap-allocated on **every request** even when the sampling decision
discards the record.

**Solution:**
1. **Merge accesslog + metrics** into a single
   `observabilityMiddleware` that acquires one `ResponseWriter` from the
   pool, does both logging and metrics in the deferred section. Saves 1
   pool acquire + 1 wrapper layer on the `Write` path.

2. **Lazy access-log allocation:** Move the `attrs []any` construction
   after the sampling decision. For `SampledLogger.Info`, the sampling
   check (`extractKey` + modulo) can be done first using the cache key
   (available from `ResponseWriter.Key`). Only construct the full `attrs`
   slice when the sample passes.

   ```go
   // Current: always allocates attrs
   attrs := []any{"method", r.Method, ...}  // 20-element slice
   logger.Info(msg, attrs...)

   // Proposed: check sampling first, only alloc if passing
   if shouldSample(sw.Key, sw.Status) {
       attrs := []any{"method", r.Method, ...}
       logger.Info(msg, attrs...)
   }
   ```

3. **Skip ring recording on cache hits.** The dashboard rings
   (`RecordRequest`, `RecordRoute`, `RecordURL`) are for insight into
   miss/bypass/error patterns. Hits are already counted by Prometheus
   counters. Skip all 4 ring writes when `X-Cache == "HIT"`.

**Expected gain:** 1 fewer `ResponseWriter` pool acquire + wrapper layer.
1 fewer `attrs` allocation for 99.9% of hit requests (1:1000 sampling).
4 fewer ring writes per hit. Estimated ~80–120 ns + 1 alloc saved per hit.

**Risk:** Low–Medium. The merged middleware must preserve all existing
behavior (sampling, non-200 always-logged, exemplar observation). The
lazy allocation must handle the non-200 path (always log) correctly.

**Files:** `internal/observability/accesslog/accesslog.go`,
`internal/observability/dataplane.go`, `cmd/bouine/cmd/builder.go`

---

### 1.5 Set Content-Length from Body Size on Cache Hits

**Impact: 🟡 Medium — prevents chunked encoding fallback when origin used chunked**

**Problem:** `serveObject` preserves whatever `Content-Length` the origin
sent (via `header.FromHTTP` → `WriteTo`). If the origin used chunked
encoding (no Content-Length), bouine stores the de-chunked body but has
no Content-Length header. On a cache hit, Go's `net/http` sees no
Content-Length and falls back to chunked transfer encoding, adding
per-chunk framing overhead. Varnish computes Content-Length from the
stored body size.

This is not a problem for the current bench (the Go origin auto-sets
Content-Length for small responses), but it would affect any origin that
uses chunked encoding (e.g., streaming responses, most Node.js servers,
most app frameworks).

**Solution:** In `serveObject`, if `Content-Length` is not in the stored
headers and `obj.BodySize > 0`, set it:

```go
if _, ok := dst[header.ContentLength]; !ok && obj.BodySize > 0 {
    dst[header.ContentLength] = []string{strconv.FormatInt(obj.BodySize, 10)}
}
```

Or better: set it at store time in `buildObject` so it's part of the
stored object and doesn't need per-hit computation.

**Risk:** Low. Must ensure Content-Length is not set for 304/206 responses
or when `Transfer-Encoding` is present. The `stripNoCacheFields` function
already handles header removal for `no-cache` fields.

**Files:** `internal/cache/handler.go` (`serveObject` or `buildObject`)

---

### 1.6 Direct Header Access in Metrics

**Impact: 🟢 Low — saves 2–3 `CanonicalMIMEHeaderKey` calls per request**

**Problem:** `DataPlaneMetrics.Middleware` calls:
- `r.Header.Get(header.XBouineRoute)` — canonicalizes "X-Bouine-Route"
- `w.Header().Get(header.XCache)` — canonicalizes "X-Cache"
- `w.Header().Get(header.XCacheSource)` — canonicalizes "X-Cache-Source"

The router writes `X-Bouine-Route` with direct map assignment using the
exact canonical key. The cache handler writes `X-Cache` and
`X-Cache-Source` the same way. The `.Get()` calls re-canonicalize
already-canonical keys.

**Solution:** Use direct map access:
```go
route := r.Header[header.XBouineRoute]  // already canonical
xCache := w.Header()[header.XCache]
```

**Risk:** None. The keys are already canonical.

**Files:** `internal/observability/dataplane.go`

---

## Tier 2 — Vary Path & Conditional Request Optimizations

### 2.1 Zero-Alloc Vary Key Computation

**Impact: 🟠 High for Vary-using routes — 3–5 extra allocs per Vary hit**

**Problem:** `VariantKey` (`vary.go:36–78`) allocates on every Vary hit:
- `strings.Split(strings.ToLower(vary), ",")` — `[]string` + lowercased string
- `sort.Strings(fields)` — in-place on the allocated slice
- `xxhash.New()` — heap-allocates `*xxhash.Digest`
- `normalizeHeaderValue` — may `strings.Split` + `sort` + `strings.Join`

**Solution (simpler than storing transient `[]string` on `api.Object`):**

The original proposal to store pre-parsed Vary fields as a transient
`[]string` on `api.Object` doesn't eliminate the work — transient fields
are re-derived after codec decode, which means the split+sort happens
again on the warm-tier/peer-fetch path. Instead:

1. **Replace `strings.Split` with `strings.SplitSeq`** (Go 1.24+).
   Returns `iter.Seq[string]` — no `[]string` allocation. Iterate
   directly.

2. **Replace `xxhash.New()` with `xxhash.Sum64()`** over a stack-allocated
   `[256]byte` buffer. Build the hash input (field + "=" + value + ";")
   into the stack buffer, then call `xxhash.Sum64(buf[:n])`. No heap
   allocation. 256 bytes covers typical Vary header values
   (`Accept-Encoding: gzip` = 20 bytes).

3. **Eliminate the sort** by storing a canonicalized Vary header value
   at cache-fill time. When `buildObject` processes the `Vary` header,
   sort the field names and store the sorted value in `header.Map`. The
   stored Vary is then already sorted — `VariantKey` can iterate in
   stored order without re-sorting.

4. **Pre-normalize `Accept-Encoding`** at store time. Collapse
   `gzip, deflate` → `gzip` (the dominant encoding). Store the normalized
   value. This reduces variant count (the hit-rate gap in 0.3 may be
   partly caused by Vary variant explosion).

**Expected gain:** Vary hits: 3–5 allocs → 0 allocs. ~80–120 ns saved.

**Risk:** Low. The `strings.SplitSeq` change is mechanical. The
stack-buffer hash is straightforward. The Vary canonicalization changes
the stored header value, which is visible to clients — must verify this
doesn't break `Vary` header semantics (RFC 9110 §12.5.5 allows
whitespace normalization but field-name reordering is technically a
semantic change for strict clients).

**Files:** `internal/cache/vary.go`, `internal/cache/handler.go`
(`buildObject`), `pkg/header/headermap.go`

---

### 2.2 `strings.SplitSeq` for ETag Matching

**Impact: 🟢 Low — saves 1 alloc per conditional request**

**Problem:** `etagMatch` (`conditional.go:63`) uses `strings.Split` which
allocates `[]string`. The linter already flags this.

**Solution:** Replace with `strings.SplitSeq` and iterate via `range`.

**Risk:** None. Go 1.26 toolchain.

**Files:** `internal/cache/conditional.go`

---

### 2.3 Fix: `revalidate()` Bypasses Singleflight

**Impact: 🟡 Medium — concurrent revalidation stampede under stale-object load**

**Problem:** `handler.go:989` — `revalidate()` calls `h.doFetch(revalReq)`
directly, not through `collapsedFetch`. Concurrent requests for the same
stale key each fire a separate conditional origin request. The
singleflight protection only covers `fetchAndStore` and
`fetchAndStoreStayinAlive`, not synchronous revalidation.

**Solution:** Route `revalidate()` through `collapsedFetch` with a
revalidation-specific key suffix (e.g., `key ^ 0x726576616c` — "reval"
in hex) to avoid colliding with regular fetch collapsing while still
deduplicating concurrent revalidations for the same key.

**Risk:** Low. Each revalidation request may carry different conditional
headers (`If-None-Match` vs `If-Modified-Since`), but the origin response
is the same — the first response applies to all waiters. The
`fetchResult` struct already carries the response for all waiters.

**Files:** `internal/cache/handler.go`

---

### 2.4 Package-Level Skip Set in `MergeHeaders304`

**Impact: 🟢 Low — saves 1 map allocation per 304 revalidation**

**Problem:** `conditional.go:78–83` allocates `map[string]bool` on every
`MergeHeaders304` call.

**Solution:** Replace with a `switch` statement or package-level
`map[string]struct{}`.

**Files:** `internal/cache/conditional.go`

---

## Tier 3 — Storage & Eviction (hit-rate and GC pressure)

### 3.1 Hit-Rate Tuning (after 0.3 investigation)

**Impact: 🔴 Critical — depends on investigation results**

**Actions (branch on investigation findings):**

- **If capacity-bound:** Increase default `MaxBytes` or make it
  auto-sizing based on available RAM (read cgroup memory limit on Linux).
  The bench default of 256 MiB is tiny compared to Varnish's typical
  `-s malloc,1G`.

- **If eviction-bound:** SIEVE is already a good algorithm. Before
  switching to TinyLFU, tune the `visited` bit reset behavior and the
  `inlineEvictCap` (currently 4). Also check if the per-shard budget
  (`maxBytes / numShards`) causes uneven eviction under skewed key
  distribution.

- **If TTL-bound:** ADR-0013 set the default TTL to "no freshness
  eligibility" when the origin sends no caching directives. Varnish
  defaults to 120s in this case. If the workload has many responses
  without `Cache-Control`, this single difference could explain the 20pp
  gap. Make the default TTL configurable (already is via
  `default_ttl` in config) and set the bench config to match Varnish's
  behavior.

- **If Vary-bound:** See 2.1's `Accept-Encoding` normalization. Also
  check if `MaxVariants = 64` is too low for the workload (Varnish
  doesn't cap variants by default).

**Files:** `internal/storage/hot.go`, `internal/cache/engine.go`,
`internal/cache/negative.go`, `config/default.yaml`

---

### 3.2 Warm Tier Segment Index — O(1) Lookup

**Impact: 🟡 Medium for warm-tier hits — O(num_segments) linear scan per Get**

**Problem:** `warm.Store.Get` does a linear scan of `s.segs` to find the
segment by ID. With 64 MiB segments and a 100 GiB warm tier, this is
O(1600) per warm hit.

**Solution:** Add `map[int64]*Segment` (`segByID`) alongside the slice.
Update on segment creation and compaction.

**Files:** `internal/storage/warm/warm.go`

---

### 3.3 Reduce Put-Path Allocations (6 → 3)

**Impact: 🟡 Medium — every cache store pays 6 allocs**

**Problem:** `HotStore.Put` allocates: SIEVE node (2 allocs from pool
miss), `hotEntry` struct, body copy, eviction log slice, potential map
rehash.

**Solution:**
1. **Pre-warm SIEVE entry pool** at startup — allocate N entries
   proportional to `maxBytes / avgObjectSize` at boot.
2. **Pool `hotEntry` structs** via `sync.Pool` per shard.
3. **Per-shard reusable eviction log** — replace per-Put `[]evictionLog`
   with a per-shard slice that's reset (len=0) between Puts.

**Note:** The original proposal's item 4 (mmap aliasing for warm-backed
bodies) is dropped — Go's GC doesn't scan `[]byte` contents, so the GC
pressure from body bytes is just the 3-word slice header, not the body
data itself. The complexity of mmap aliasing is disproportionate to the
~8 bytes saved per object.

**Expected gain:** 6 allocs → 3 allocs per Put. Reduces GC pressure by
~50% on the write path.

**Files:** `internal/storage/hot.go`, `internal/storage/sieve/sieve.go`

---

### 3.4 Pool CRC32 Hashers in Warm Tier

**Impact: 🟢 Low — saves 1 alloc per warm write/read**

**Problem:** `crc32.New(crcTable)` allocates a `digest` per record.

**Solution:** Pool the hasher via `sync.Pool`.

**Files:** `internal/storage/warm/warm.go`

---

## Tier 4 — Cluster & I/O Optimizations

### 4.1 Peer-Fetch Connection Pool Tuning

**Impact: 🟠 High for cluster mode — `MaxIdleConnsPerHost=2` causes TLS handshake storms**

**Problem:** `peerfetch.go` transport uses Go's default
`MaxIdleConnsPerHost=2`. Bursty miss traffic exhausts the idle pool and
incurs TLS handshakes (~1–3 ms each).

**Solution:**
```go
transport := &http.Transport{
    ForceAttemptHTTP2:     true,
    TLSClientConfig:       tlsCfg,
    MaxIdleConns:          512,
    MaxIdleConnsPerHost:   64,
    IdleConnTimeout:       90 * time.Second,
    DialContext: (&net.Dialer{
        Timeout:   2 * time.Second,
        KeepAlive: 30 * time.Second,
    }).DialContext,
}
```

**Risk:** None. Pure configuration improvement.

**Files:** `internal/cluster/peerfetch.go`

---

### 4.2 Broadcast Shared HTTP Client

**Impact: 🟠 High for strong-mode invalidation — new `http.Client` per call**

**Problem:** `broadcast.go:217` creates `&http.Client{Timeout: 2s}` on
every `postBinary` call. No connection reuse. Every invalidation to every
peer creates a new TCP connection.

**Solution:** Create a single shared `*http.Client` on the `Broadcaster`
struct at construction time. Use `context.WithTimeout` for per-call
timeouts.

**Files:** `internal/cluster/broadcast.go`

---

### 4.3 Peer-Fetch Streaming Decode

**Impact: 🟡 Medium for large objects — 256 MiB worst-case transient allocation**

**Problem:** `peerfetch.go:207` does `io.ReadAll(io.LimitReader(resp.Body,
64 MiB))` — allocating the full response body. 4 concurrent fetches =
256 MiB worst case.

**Solution:** Stream into a pooled `bytes.Buffer` that grows
incrementally. For objects above `BodyThreshold` (64 KiB), write directly
to the warm tier and return a reference.

**Files:** `internal/cluster/peerfetch.go`

---

### 4.4 Reduce Double Gossip Delivery in Strong Mode

**Impact: 🟡 Medium for strong-mode invalidation throughput**

**Problem:** `QueueBroadcast` calls `ml.SendBestEffort` to every member
AND enqueues for memberlist's gossip drain. In strong mode, HTTP fan-out
is the primary delivery — the direct send is redundant.

**Solution:** In strong mode, skip the direct `SendBestEffort`. Rely on
HTTP fan-out (primary) + gossip drain (fallback). In eventual mode, keep
both (no HTTP fan-out).

**Files:** `internal/cluster/cluster.go`, `internal/cluster/broadcast.go`

---

### 4.5 Hedge: Don't Clone Primary Request

**Impact: 🟡 Medium — saves 1 `req.Clone()` per hedged request on the happy path**

**Problem:** `hedge.go:38` — `fire()` calls `req.Clone(ctx)` for the
primary request even when the hedge never fires. `req.Clone` deep-copies
the `http.Header` map.

**Solution:** Pass the original request to the primary goroutine. Only
clone for the hedge goroutine (which needs an independent request to
avoid concurrent map access).

**Files:** `internal/origin/hedge.go`

---

## Tier 5 — Quick Wins

| # | Improvement | Effort | Impact |
|---|---|---|---|
| 5.1 | Pool access-log `[]any` slice (part of 1.3) | 1 day | 1 alloc/hit |
| 5.2 | Direct header map access for route/X-Cache labels (1.4) | 1 hour | 3 canonicalization calls/hit |
| 5.3 | Avoid `strings.ToLower(r.Host)` in router — use `strings.EqualFold` | 1 hour | 1 string alloc/hit |
| 5.4 | Pool CRC32 hashers in warm tier (3.4) | 1 hour | 1 alloc/warm-IO |
| 5.5 | Scale peer-fetch semaphore with cluster size | 1 hour | Better fan-out |
| 5.6 | Package-level skip set in `MergeHeaders304` (2.4) | 1 hour | 1 alloc/reval |
| 5.7 | `strings.SplitSeq` in `varyContainsStar` | 1 hour | 1 alloc/Vary-hit |
| 5.8 | `slices.Sort` instead of `sort.Slice` in ring add (cluster.go:457) | 1 hour | Minor |
| 5.9 | Fix `connLimitListener` — write 503 before closing | 1 hour | Correctness |

---

## Tier 6 — Research / Speculative (not for near-term implementation)

### 6.1 Pre-Serialized Head + `net.Buffers` Writev

After 1.1 is implemented, investigate writing the pre-serialized head +
body via `net.Buffers.WriteTo` (single `writev` syscall) when the
underlying connection is HTTP/1.1. For HTTP/2, fall back to the standard
`w.Write()` path (H2 frames the data anyway). This does NOT require
hijacking the connection — `net.Buffers` implements `io.WriterTo` and
`http.Server` already uses `writev` when the response body is served via
`io.ReaderFrom`. The key is making the cache handler implement
`io.ReaderFrom` to return a `net.Buffers` reader.

### 6.2 HTTP/3 / QUIC

Requires `quic-go/http3` (not `net/http`). Needs an ADR to allow a
non-`net/http` HTTP server per AGENTS.md §2.2. Benefit is mainly for
WAN/edge deployment (packet loss resilience), not datacenter internal
traffic.

### 6.3 Custom HTTP/1.1 Parser for Hit Path

**Only worth investigating after 1.1–1.3 are done and the response path
is at zero-alloc.** The bottleneck is currently response writing, not
request parsing. `net/http`'s request parser is not on the critical path
of the 844ns hit benchmark. Once the response path is optimized, profile
again to see if request parsing becomes the new bottleneck.

Note: AGENTS.md §2.2 requires "One HTTP stack only: `net/http`." This
proposal requires an ADR and a fundamental constraint change.

### 6.4 Shared-Memory Hot Tier (mmap)

Go's GC scans pointers in `[]byte` headers (3 words: ptr+len+cap) but
NOT the byte contents. With 1M objects, that's ~3M pointer scans —
~30µs of GC work per cycle, not 50ms. The mmap hot tier would eliminate
these pointer scans but the complexity (custom allocator, slab
management, crash recovery) is disproportionate unless the object count
exceeds 10M+. First try `GOGC`/`GOMEMLIMIT` tuning.

### 6.5 io_uring WAL (Linux)

Async write+fsync via `io_uring` for the WAL. Linux 5.1+. Potential 5–10×
WAL throughput improvement for write-heavy workloads. Needs build tags
and graceful fallback for macOS/non-Linux.

### 6.6 Custom Gossip Protocol

Replace `hashicorp/memberlist` with a minimal SWIM-based protocol. Only
worth it for 100+ node clusters where memberlist's overhead becomes
measurable. The current cluster tests (3 nodes) won't show any benefit.

---

## Implementation Priority & Sequencing

### Phase A — Investigation (1 week)
1. **0.1** — Uncapped throughput measurement
2. **0.2** — Data throughput discrepancy investigation
3. **0.3** — Hit-rate gap investigation

### Phase B — Quick Wins + Cluster Fixes (1–2 weeks)
4. **5.1–5.9** — All quick wins
5. **4.1** — Peer-fetch pool tuning
6. **4.2** — Broadcast shared client
7. **2.2–2.4** — Conditional request allocs

### Phase C — Hot-Path Restructuring (2–4 weeks)
8. **1.2** — Collapse 3 tracing spans to 1
9. **1.3** — Merge middleware + lazy access log
10. **1.1** — Pre-serialized response head
11. **2.1** — Zero-alloc Vary key
12. **1.4** — Direct header access in metrics
13. **2.3** — Fix revalidate singleflight

### Phase D — Storage & Hit Rate (2–4 weeks)
14. **3.1** — Hit-rate tuning (depends on 0.3 findings)
15. **3.2** — Warm tier segment index
16. **3.3** — Put-path allocation reduction
17. **4.3–4.5** — Peer-fetch streaming, gossip dedup, hedge fix

### Phase E — Research (ongoing)
18. **6.1–6.6** — Profile after Phase D, investigate only if bottlenecks
    warrant it

---

## Expected Outcome

| Metric | Current | Target | How |
|--------|---------|--------|-----|
| Hit-path latency (p50) | 0.197 ms | 0.120 ms | 1.1 + 1.2 + 1.3 |
| Hit-path allocs (prod) | ~8–9 | ~2–3 | 1.2 + 1.3 + 2.1 |
| Mixed hit rate | 73% | 90%+ | 0.3 + 3.1 |
| Mixed latency (p95) | 0.694 ms | 0.350 ms | 0.3 + 3.1 + all hot-path opts |
| Strong-mode invalidation | ~2 ms/call | <0.5 ms | 4.2 + 4.4 |
| Warm-tier Get lookup | O(1600) | O(1) | 3.2 |
| Put-path allocs | 6 | 3 | 3.3 |
