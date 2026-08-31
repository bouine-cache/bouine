# Hit-path p99 optimization — evidence and rejected candidates

Date: 2026-08-29
Branch: `perf/hit-path-latency-p99`

## Context

k6 loadtests (`bench/loadtest/results/`, scenario 3.2 hit-only) showed
bouine beating nginx and varnish on p50 but losing on p90/p95/p99.
Profiling a live instance under load (`pprof` CPU, heap `alloc_space`,
goroutine, `GODEBUG=gctrace=1`) found:

- Application code (parse, key build, store Get, serialize) was already
  tight: ~195ns for TryHit, ~18ns for HotStore.Get, ~220ns for parse —
  ~0.2% of a ~100µs end-to-end request.
- `h1parser.parseRequest` allocated a fresh `api.RawRequest`
  (~4KB, `[100]RawHeader` array) per request — **99.3% of all allocated
  bytes**, driving ~63 GC cycles/s with 2–3ms clock phases and
  mark-assist stealing CPU from request goroutines.
- `BenchmarkGate_H1Parse_Get` pre-allocated the `RawRequest` outside
  the loop, so the 0-allocs gate could not see this class of regression.
- `serveHit` called `SetWriteDeadline` per request (~110ns + timer work).
- `handleFallThrough` converted every header with `[]byte(...)`
  (2 allocs per header on the miss path).

All four were fixed on this branch. Measured combined result
(M1 Pro, 32 keep-alive conns, warm cache, alternating A/B rounds):

| metric        | before      | after       | delta  |
|---------------|-------------|-------------|--------|
| p99           | 610–624µs   | 542–560µs   | ~-10%  |
| p95           | 439–441µs   | 417–425µs   | ~-4%   |
| GC cycles     | ~63/s       | ~0.3/s      | ~-200× |
| allocs/15s    | 7.29GB      | ~9KB        | ~-99.9%|

## 2026-08-30 follow-up 2: native histogram off + the epoll reactor

Two changes land after the fast-path-enabling audit:

1. **`NativeHistogramBucketFactor` dropped from
   `request_duration_seconds`** — the plan's own revisit note above
   ("no dashboard uses native histogram queries; all PromQL uses
   classic `_bucket` series") plus the per-Observe sparse-bucket math
   cost on the hit path's duration histogram. Classic buckets are
   unchanged; dashboards unaffected.

2. **epoll reactor for batch hit serving** (ADR-0041,
   `experimental.h1_reactor`, Linux, default off, requires
   `h1_fast_path`). This attacks the "Known remaining gap" below
   directly: one goroutine per listener multiplexes all hit-path
   connections via `epoll_wait` — one wakeup per batch instead of one
   per request, no goroutine park/unpark, no Go runtime poller on the
   hit path. Everything that is not a plain hit (miss, conditional,
   range, pipelined body, oversize headers, malformed) hands off to a
   per-connection goroutine running the existing blocking Parser with
   the buffered bytes replayed, so all correctness semantics stay
   shared. Verified: portable state-machine unit tests (12), Linux
   epoll end-to-end tests (4) under `-race`, a 0-alloc
   `BenchmarkGate_Reactor_Hit` (370-405ns on the Linux runner), and a
   live-daemon smoke (1 MISS handoff + 29 keep-alive HITs on one
   connection).

The reactor is NOT enabled for benchmarks in this change: the nightly
runner must first establish the blocking-path numbers with the fast
path on (PR #563), then flipping `h1_reactor: true` in
`bench/loadtest/config/bouine.yaml` is a one-line measured increment.

## Rejected: sharding the hot Prometheus metrics tuple

**Candidate**: `RecordHit` updates
`requests_total` / `request_duration_seconds` / `response_bytes_total`
on pre-resolved shared Prometheus children. A microbenchmark showed
`Observe` on a shared child costs ~410ns under parallel load versus
2–8ns unshared — contention on a single cacheline — and the
`request_duration_seconds` histogram also pays native-histogram
sparse-bucket math (`NativeHistogramBucketFactor: 1.1`).

**Caveat on what was measured**: in the default production wiring
(`NewFastPathHandlerFromStore`, cmd/bouine/cmd/engine.go) the fast-path
handler carries no route name, so `lookupRouteMetrics("")` misses and
every fast-path hit takes the `WithLabelValues` fallback — the *slower*
path (hash lookup plus three child acquisitions on shared children).
The A/B below measured that path, so the rejection holds a fortiori.
Side effect worth fixing separately: fast-path hits attribute to
`route=""` in dashboards.

**Why rejected**: an end-to-end A/B (metrics hook wired vs
`WithMetricsHook(nil)`, two alternating rounds, 32 conns, 1.8M reqs per
run) measured **no difference** — p99 542–547µs in both configurations.
The contention does not materialize at this concurrency: writers hit
the same child from different Ps, but the atomic CAS retries stay rare
relative to the rest of the request. Removing it would have meant ~300
lines of custom concurrent collector code (LongAdder-style sharded
series aggregated at scrape time) with real correctness risk, for an
unmeasurable gain on the reference loadtest hardware.

**Revisit if**: the pinned Linux bench runner (4 CPUs, 50k RPS scenario
3.2) shows p90/p99 regressions attributable to metric contention —
profile first, land only on evidence. If revisited, also drop
`NativeHistogramBucketFactor` (no dashboard uses native histogram
queries; all PromQL uses classic `_bucket` series).

## Known remaining gap

Per request, the h1parser parks/unparks a goroutine for read and again
for writev (`pthread_cond_signal` was 10.7% of CPU samples). nginx
batches many connections per `epoll_wait` wakeup. Matching this
requires an event-loop connection multiplexer in h1parser — a large
rewrite with real risk. Land the cheap wins first; re-measure on the
Linux runner before considering.

## 2026-08-30 follow-up: the fast path was never enabled in benchmarks

Auditing the loadtest configuration found that **every committed
nginx/varnish/envoy comparison ran with `experimental.h1_fast_path`
absent from `bench/loadtest/config/bouine.yaml`** — the flag defaults
to false, so all rounds (baseline through v3) measured the slow
middleware chain (tracing → metrics → router, ~550ns/request of
middleware plus the fasthttp worker machinery) while the 183ns/0-alloc
hit path sat unused. The nightly runner (4 dedicated CPUs per proxy)
is the environment where the gap to nginx actually lives; it now runs
with the fast path on.

Enabling it required closing five correctness gaps in the h1parser and
fast path first (each would have been a silent behavioral regression
vs the middleware path):

1. **SWR never refreshed via fast path** — fast-path StaleHit served
   stale objects but never triggered background revalidation, so stale
   content would stay stale forever under pure-hit traffic. Fixed with
   an `onStale` hook on `FastPathHandler` wired by the engine to a
   refresh-enabled handler's `TriggerBgRevalidateFromFastPath`
   (handler.go), which materializes the RawRequest fields (they alias
   the connection read buffer) before the goroutine escapes.
2. **Fall-through body truncation** — `handleFallThrough` copied only
   the bytes already buffered in `readBuf` into the RequestCtx
   (`SetBodyRaw`), silently truncating bodies that spanned multiple
   TCP reads. Fixed by replaying the rebuilt request head through a
   `prefixConn` and letting fasthttp re-parse the request from the
   live socket with full body framing (CL, chunked, trailers,
   Expect: 100-continue).
3. **Pipelined bytes after a hit were discarded** — the Serve loop
   overwrote `readBuf` on the next iteration, dropping any bytes the
   client pipelined past a hit's header block. The hit path now hands
   excess bytes to the fallback handler.
4. **Oversize headers were dropped** — requests with >16 KiB of
   headers returned nothing. They now fall through to the fallback
   handler via the same prefix mechanism.
5. **Smuggling was served, not rejected** — CL+TE / duplicate-CL
   requests fell through to fasthttp, which served them normally.
   The parser now rejects with 400 + connection close per RFC 9110
   §6.6.2 (two pre-existing tests asserted the served behavior and
   were updated to assert the 400).

Also fixed while here:

- h1parser idle timeout was 10s vs fasthttp's 120s (nginx: 65s) —
  visible as elevated `http_req_connecting` in k6 results (k6
  reconnects when keep-alives die). Now 120s, matching the fasthttp
  listener.
- The h1parser clock is now `platform.CoarseNow` on Linux (~2-4ns vs
  ~25-40ns per call; the dataplane middleware already used it).
- Fast-path hits carried `route=""` in Prometheus labels and took the
  `WithLabelValues` fallback path; `RecordHit` now maps empty route to
  `_default` (pre-resolved array path, consistent with dashboards).

Verification: full `go test -race` suite green, `bench/run.sh gate`
PASS (FastPath_Hit 177-188ns, 0 allocs), cache-tests conformance with
`BOUINE_FAST_PATH=true` shows **zero regressions vs the fast-path-off
baseline** (337 vs 340 pass — the 3 extra baseline passes are flaky
setup entries, not fast-path failures; diff of per-test results
confirms no test passes without and fails with the fast path),
`make integration` green.

## Rejected: fasthttp Prefork

**Candidate**: `fasthttp/prefork` forks the daemon into one child per
core with SO_REUSEPORT, claiming throughput and p99 gains by letting
each core own a private GC and scheduler ("Go doesn't have to share
and manage memory between cores"). nginx itself uses this model.

**Why rejected**: the prefork model is fundamentally incompatible with
a daemon that *is* shared state:

- **The hot store is process memory.** Prefork children inherit
  copy-on-write memory at fork time; cache fills in child A are
  invisible to children B..N. A node becomes N independent 1/N caches:
  hit rate collapses, and a purge/ban lands in one child while the
  others serve the invalidated object until their own TTL expires —
  a correctness violation of the invalidation contract. fasthttp's own
  README carries the warning: "using prefork prevents the use of any
  global state! Things like in-memory caches won't work."
- **The Engine cannot start in a child.** Each child re-executes the
  process; the admin listener and the memberlist gossip UDP/TCP ports
  are bound once, so every child after the first fails EADDRINUSE.
  Cluster mode, the graceful-drain `Close(ctx)` contract (AGENTS.md
  §11), rolling restarts, and the shutdown sequencer all assume a
  single process.
- **Warm tier and WAL would be opened N times** — the mmap'd warm tier,
  eviction accounting, async fsync (ADR-0024), and the tombstone drain
  goroutine (ADR-0029) would race across processes with no locks.
- **The claimed benefit is 2019-era** (go1.13.6, 2017 MacBook, a
  `time.Sleep(100ms)` handler): +7.5% RPS, p99 15ms vs 75ms — a pure
  goroutine park/unpark scheduling artifact. Go 1.27's scheduler has
  reclaimed most of that, and bouine already has the modern
  equivalents: cgroup-quota-aware GOMAXPROCS (engine.go), the
  zero-alloc hit path (GC 63/s → ~0.3/s, no cross-core GC pressure),
  sharded hot store + sharded singleflight, and SO_REUSEPORT with
  N accept loops on Linux (listener.go serveMulti) — the same
  kernel-level accept distribution prefork buys, without the fork.
- **The hit path bypasses `fasthttp.Server.Serve` anyway** — the
  h1parser accepts connections directly (fp_conn.go), so a Prefork
  wrapper would not touch the path this effort optimizes.

**Revisit if**: none foreseeable. If accept/worker contention shows up
on the Linux runner, extend `serveMultiFastPath` (per-CPU accept loops
already exist) or the event-loop multiplexer above — both keep the
cache in one process.

## Benchmark hygiene

- `BenchmarkGate_H1Parse_Get` now drives the full `parseRequest`
  production path (scratch reset included) so per-request allocations
  fail the 0-allocs gate.
- `BenchmarkH1Parse_FallThroughHeaderCopy` pins the fall-through header
  copy at 0 allocs/op.
