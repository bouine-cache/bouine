# ADR-0034: Reverse ADR-0006, adopt fasthttp as the sole HTTP stack

- **Status**: Accepted
- **Date**: 2026-08-22
- **Deciders**: @thylong
- **Phase**: phase 0
- **Supersedes**: ADR-0006

## Context

ADR-0006 dropped Fiber (which wrapped `valyala/fasthttp`) and unified the
daemon on `net/http` for both the data plane and the admin plane. The
rationale was sound at the time: the admin surface serves ≤10 RPS and
does not benefit from fasthttp's allocation model, Fiber pulled in 7
dependencies, and `net/http.ServeMux` gained method+pattern routing in
Go 1.22.

Since then, the performance requirements have intensified. At 100K RPS
per core with an 80/20 hit/miss ratio, the `net/http` miss path
generates ~740K allocs/s (37 allocs/op × 20K misses/s). GC runs every
3-5 seconds and dominates hit-path p99 latency (20-50µs of GC jitter
per p99 request). The custom h1parser already achieves 0 allocs/op on
the hit path, but the miss path — which goes through
`httputil.ReverseProxy`, `responseRecorder` (with its `http.Header` map
and `bytes.Buffer`), and `context.WithValue` — is allocator-heavy.

The full analysis and migration plan are in issue #521. The performance
estimates, cluster-mode analysis, and implementation details are
documented there.

## Decision

Adopt `github.com/valyala/fasthttp` as the sole HTTP stack for the
entire daemon: data plane, origin fetch, peer fetch, cluster broadcast,
active healthcheck, admin server, dashboard, and observability
middleware.

**Drop HTTP/2 and HTTP/3 support.** fasthttp does not support either
protocol. HTTP/2 is currently used in two places: (a) the data-plane
listener (ALPN h2 / h2c), and (b) the peer-fetch transport. HTTP/3 was
already deferred (§16.3) and has no implementation. Both are removed.

**Rewrite the custom h1parser** (Option C from the plan discussion) to
use fasthttp's `RequestCtx`/`Response` types as its internal backing
store while preserving the zero-allocation direct-write hit path via
`net.Buffers.WriteTo`.

**Admin plane uses `fasthttpadaptor`** to wrap `net/http/pprof` and
`promhttp.Handler()` — these are `http.Handler`-based and require an
adapter. At admin RPS (≤10), the per-request adapter allocation is
negligible.

## Consequences

### Positive

- 60-73% allocation reduction on the miss path (37 → 10-14 allocs/op).
- 2.5-3.5× less frequent GC at 100K RPS.
- 35-40% miss-path p99 latency improvement.
- 15-30% hit-path p99 latency improvement (from GC jitter reduction).
- Eliminates the h1parser fallback machinery (`prefixedConn`,
  `closeNotifyConn`, `singleConnListener`, `reconstructRawRequest`).
- Enables streaming origin responses (Phase 4 of #521), eliminating
  full-body buffering on the miss path.
- HTTP/1.1 pipelining for peer fetch reduces connection pool memory by
  75-85% vs naive HTTP/1.1 (from 3.2MB to 400KB per peer).
- One HTTP stack, one handler model, one set of types.

### Negative / trade-offs

- **HTTP/2 multiplexing lost.** Peer fetch and origin fetch use
  HTTP/1.1 keep-alive + pipelining instead of HTTP/2 multiplexed
  streams. Connection pool memory increases (mitigated by pipelining:
  400KB per peer vs 50KB with HTTP/2).
- **`golang.org/x/net/http2` removed.** Any client requiring HTTP/2
  will fail. Origins must support HTTP/1.1. Documented in the runbook.
- **`fasthttpadaptor` overhead for pprof/prometheus.** Per-request
  allocation (~100 bytes) at admin RPS — negligible.
- **Peer protocol wire change.** HTTP/2-over-mTLS → HTTP/1.1-over-mTLS.
  Rolling upgrade is safe (ALPN negotiation falls back to HTTP/1.1).
  Documented in ADR-0035.
- **72 test files need migration** from `net/http/httptest` to
  `fasthttputil`. Mechanical but voluminous.

### Risks

- **Hit path 0-alloc regression.** The rewritten h1parser must preserve
  0 allocs/op. fasthttp's `RequestCtx` is heap-allocated (pooled), vs
  the current stack-allocated `RawRequest`. Risk: 0% to -2% on hit-path
  ns/op. Mitigated by benchmark gates (`BenchmarkGate_FastPath_Hit`
  must remain 0 allocs/op).
- **`TCP_NODELAY` on h1parser connections.** Without it, Nagle's
  algorithm could cause a 1000× regression on small cache-hit
  responses. Must be explicitly set (1-line fix, see #521 §1.5).
- **`fasthttp.RequestCtx` lifetime with singleflight.** The pooled ctx
  may be reused while an origin fetch is in-flight. Requires
  decoupling the origin fetch from the caller's ctx (see #521 §3.3).

## Alternatives considered

- **Keep `net/http`, optimize the miss path independently.** Rejected:
  the miss path's allocation pattern is inherent to `net/http`'s type
  system (`http.Header` map, `*http.Request` per request,
  `httputil.ReverseProxy` allocation). Eliminating these requires
  changing the HTTP types, which means changing the HTTP library.
- **Use fasthttp only for the data plane, keep `net/http` for admin.**
  Rejected by the project owner: "fasthttp everywhere" is the stated
  requirement. The admin plane migration is low-cost (fasthttpadaptor)
  and eliminates the cognitive overhead of two HTTP stacks.
- **Keep the custom h1parser as-is, use fasthttp only for misses.**
  Rejected (Option A in the plan discussion): preserves the 0-alloc hit
  path but leaves two parser codepaths. Option C (rewrite on fasthttp
  types) was chosen to unify the parser while preserving the direct
  `net.Buffers.WriteTo` write path.

## References

- Issue #521 — full migration plan with 12 phases, performance
  estimates, and cluster-mode analysis.
- Issue #522 — zero-copy header interning with `unsafe.String`.
- ADR-0006 — the decision being reversed.
- `AGENTS.md §2.2` (updated: "One HTTP stack only: `fasthttp`").
- `docs/architecture.md §2.1` (updated to reflect fasthttp).
- `docs/deps.md` (fasthttp added to allow-list).
