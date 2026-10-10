# ADR-0042: H1 reactor return path, pipelined hits, and telemetry

- **Status**: Accepted
- **Date**: 2026-09-02
- **Deciders**: @theotime
- **Phase**: perf round 4 follow-up
- **Consulted**: (stress-test A/B pr567-5d38db0 analysis)

## Context

ADR-0041 introduced the single-goroutine epoll hit loop, measured at a
4x keep-alive RTT improvement on pure-hit k6 scenarios. A subsequent
1-hour A/B (`h1_reactor` on/off, vegeta, Linux, 3-node cluster, 6k
req/s, 100 connections) showed **no improvement and a slight tail
regression** — and the CPU profile explained why: the reactor loop
consumed 0.01 s of 234 s of samples. Every connection had left the
reactor.

Two structural gaps, both consequences of handoff being terminal per
connection:

1. **Mixed traffic starves the reactor.** A cluster workload serves
   ~25% local hits, ~55% peer fetches, ~20% origin misses; a miss hands
   the connection to the blocking parser *forever*, so within the first
   few requests every long-lived connection exits the reactor and never
   returns. The reactor pays accept + parse + handoff-spawn overhead on
   every new connection while serving nothing.
2. **Pipelined bytes force handoff.** A load generator writing many
   requests per connection leaves bytes queued past a request's header
   terminator; the reactor handed off on `excess` even when the request
   itself was a plain hit.

Compounding both: no telemetry existed to see any of this — nothing
reported how many connections the reactor tracked, how many hits it
served, or why connections left.

## Decision

We make three changes to the h1parser reactor, all Linux-only behavior
behind the existing `experimental.h1_reactor` flag:

1. **Return-to-reactor.** After the blocking parser finishes serving a
   request (hit, miss, or fall-through) on a handed-off connection, it
   offers the connection back to the reactor loop via the accept path's
   pending queue instead of parking on the next keep-alive read.
   `Parser.Serve` reports the transfer with the `errReactorReturned`
   sentinel; the handoff spawner skips the conn close on it. The
   reactor re-registers the fd (already non-blocking) and serves the
   connection's subsequent requests inline. A declined return (queue
   full, missing fd) degrades to the current behavior: the blocking
   parser keeps the connection.
2. **Pipelined hits stay inline.** A hit whose header block is followed
   by pipelined bytes is served inline; the excess (always the next
   request — qualifying requests carry no body) is preloaded into the
   read buffer and consumed by an internal post-flush pass
   (`actFlushed`, consumed inside `advance()`'s loop, never seen by the
   transport). Non-hits and preloaded incomplete requests keep the
   existing handoff-with-replay semantics.
3. **Reactor telemetry.** A new optional capability interface,
   `api.ReactorMetrics` (satisfied by `DataPlaneMetrics`), reports
   `bouine_h1_reactor_conns_registered_total`, `_hits_total`,
   `_handoffs_total{reason}` (closed set: miss, disqualified,
   malformed, oversize, overflow, cap), `_returns_total`, and
   `_conns_dropped_total`. Every method is a single counter increment,
   safe to call from the loop goroutine.

## Consequences

### Positive
- Mixed hit/miss workloads keep the reactor engaged: a miss costs one
  blocking round-trip, then the connection returns to batch hit
  serving.
- Batch-writing clients (the vegeta/stress-test pattern) no longer
  exile connections on the first pipelined request.
- Reactor engagement is observable without pprof; a starved reactor
  shows as `hits_total` ~0 with `handoffs_total{reason="miss"}` high
  and `returns_total` low.

### Negative / trade-offs
- Under pure-miss traffic each request pays one extra goroutine spawn
  (handoff) plus one channel push (return) versus the previous
  park-once model. The spawn runs on the spawner goroutine (off-loop),
  and misses already pay an origin/peer fetch, so the added scheduling
  cost is negligible next to the fetch.
- Handoff reasons split "miss" (no local object) from "disqualified"
  (conditional/range/body/non-GET/HEAD), changing nothing behaviorally
  but requiring dashboard updates.

### Risks
- Return/accept share one pending queue (1024 slots); sustained accept
  storms could starve returns. Returns decline (blocking parser keeps
  serving), so the failure mode is degraded scheduling, not dropped
  connections.
- Shutdown ordering: a connection returned after the loop's cleanup
  drain could leak. Mitigated by a final pending drain in `Close` that
  runs only after every handoff goroutine (the only return pushers) has
  exited; covered by the race-detector storm-shutdown tests.

## Alternatives considered

- **Return only after N idle requests or on hit outcomes** — rejected:
  adds a tuning knob and predicts the future no better than
  return-always; the cost asymmetry (µs scheduling vs ms fetch) makes
  return-always the safe default.
- **Per-reactor return queue** — rejected: the accept pending queue is
  already sized and drained by the loop; a second queue doubles the
  wakeup machinery for no measurable gain.
- **Keep pipelined handoff, fix only starvation** — rejected: the A/B
  workload triggers both gaps simultaneously (batch writes + misses);
  fixing one leaves the reactor mostly idle in the same scenario.
- **Atomic-counter snapshot metrics polled via `prometheus.CounterFunc`**
  — rejected: method-per-counter on an injected capability interface
  matches the existing `api.FastPathMetrics` pattern and keeps h1parser
  free of Prometheus imports (layer rule).

## References

- ADR-0041 (`docs/decisions/0041-h1-epoll-reactor.md`) — the reactor
  itself; this ADR amends its handoff contract.
- `docs/runbook/51-h1-reactor.md` — operator-facing symptoms and the
  new telemetry.
- `internal/server/h1parser/reactor.go`,
  `internal/server/h1parser/reactor_epoll_linux.go` — implementation.
- Stress-test A/B analysis `pr567-5d38db0-reactor-ab` (reactor loop
  0.004% of CPU samples under mixed traffic).
