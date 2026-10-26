# ADR-0069: The H1 fast path is the default path

- **Status**: Accepted
- **Date**: 2026-10-26
- **Deciders**: @thylong
- **Phase**: performance (post-phase 9 fast path)
- **See also**: ADR-0041 (superseded), ADR-0068 (reactor removal),
  ADR-0034 (fasthttp adoption), issue #636 (fast-path peer branch),
  issue #696 (per-route fast-path attribution)

## Context

The H1 fast path — the zero-allocation HTTP/1.1 parser that serves
cache hits without fasthttp's pooled `*fasthttp.RequestCtx` machinery —
has been gated behind `experimental.h1_fast_path` since its landing,
with the cluster peer branch behind a second flag,
`experimental.h1_fast_peer_path` (issue #636).

Every promotion signal is green:

- **Benchmarks.** The canonical load-test config and every
  nginx/varnish/envoy comparison round already ran with the flag on;
  the committed `bench-gate` budgets (`FastPath_Hit`,
  `RoutedFastPath_Hit`, `FastPath_PeerHit`, ...) pin allocs/op = 0 on
  the hit path. The flag-off configuration is the one nobody measures.
- **Conformance.** `make conformance-fastpath` (cache-tests with the
  flag forced on) reported no score difference versus the flag-off
  run.
- **Attribution parity.** Issue #696 moved the fast path to per-route
  handlers: pool attribution, KeyPolicy, lazy-ban evaluation, and
  traffic-class labels match the slow path (ADR-0047).
- **Consistency.** The slow path has always done owner-first peer
  fetch in strong mode (`applyClusterClosures`). With the flag off,
  the fast path was the *only* surface that skipped it: identical
  traffic took a different miss path depending on which parser read
  it. Default-on removes the inconsistency rather than adding a
  behavior.

Two blockers held promotion back, both now resolved:

1. The reactor (ADR-0041) was incompatible with the peer branch
   (TryHit must never block an event loop on peer I/O). The reactor
   is gone (ADR-0068); all fast-path traffic uses the blocking
   h1parser path.
2. The fast path had no shutdown drain. `serveFastPath` closed the
   listener on context cancellation and then blocked in `wg.Wait()`
   until every idle keep-alive connection hit its idle deadline —
   the exact hang the engine's ordered drain (issue #76) exists to
   prevent on the fasthttp path, which solves it with
   `fasthttp.Server.Shutdown` (close idle connections, wait for
   in-flight responses).

## Decision

1. The H1 fast path is **always enabled**: the engine builds it
   whenever a store exists, with no configuration gate. It still
   declines per request (non-GET/HEAD, conditional headers, no
   cache-enabled route) exactly as before; only the global switch is
   gone.
2. The fast-path peer branch is **always wired** in strong-mode
   clusters, using the same closures as the slow path. Single-node
   and weak-mode deployments log at Debug (a normal deployment
   shape, not an error) and skip it.
3. Both experimental keys are **retired, not removed**: strict
   parsing still accepts `experimental.h1_fast_path` and
   `experimental.h1_fast_peer_path` (any combination, including the
   formerly rejected peer-without-fast-path) so pre-promotion
   configs keep loading; the engine logs a retirement notice at
   startup when either is set. There is no opt-out: the flags are
   inert. The cross-field validation
   (`h1_fast_peer_path requires h1_fast_path`) is deleted along with
   the semantics it guarded.
4. `serveFastPath` gets the shutdown drain the promotion exposes as
   a requirement: the listener tracks each connection's idle /
   in-flight state through a new zero-allocation h1parser hook
   (`WithConnStateHook`). On shutdown, idle connections are closed
   immediately and in-flight responses are allowed to finish —
   `fasthttp.Server.Shutdown` parity — so `Serve` returns once
   in-flight work drains instead of hanging on parked keep-alive
   connections. A request whose bytes have arrived but which no
   goroutine has begun serving is closed as idle; fasthttp's
   `closeIdleConns` has the same edge.
5. Harness surfaces follow the default: `make conformance` exercises
   the fast path (the `conformance-fastpath` target is deleted);
   the load-test config drops its `experimental:` block; the
   integration cluster driver drops the two experimental options
   (every strong-mode cluster now runs the peer branch, increasing
   default integration coverage of issue #636).

## Consequences

- Every deployment with a cache-enabled route switches its hit path
  to the h1parser. Deployments that never set the flag get the
  parser's behavior (per-route attribution, per-connection blocking,
  5-minute idle client-write budget for streams — runbook 52)
  without any config change.
- The hit path gains two non-allocating hook calls per request (the
  conn-state transitions) and the listener a mutex-guarded map
  touched twice per request; the `BenchmarkGate_*` budgets are
  unchanged and still enforced at zero allocs/op.
- Shutdown is bounded by in-flight response completion, matching the
  fasthttp path; idle keep-alive connections no longer pin listener
  goroutines past cancellation.
- Removing the flags entirely (strict parse rejecting the keys) is
   deliberately deferred: it would break pre-promotion configs for
   zero benefit, and the retirement log makes the inertness
   discoverable. A future major version may delete the fields.
