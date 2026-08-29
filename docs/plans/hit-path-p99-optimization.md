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

## Benchmark hygiene

- `BenchmarkGate_H1Parse_Get` now drives the full `parseRequest`
  production path (scratch reset included) so per-request allocations
  fail the 0-allocs gate.
- `BenchmarkH1Parse_FallThroughHeaderCopy` pins the fall-through header
  copy at 0 allocs/op.
