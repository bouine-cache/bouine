# H1 reactor: stuck writers, dropped hit metrics, spawner saturation, starved loop

## Symptoms

- **Stuck writers**: with `experimental.h1_reactor: true`, connections to a
  client that stops reading mid-response are dropped after
  `reactorWriteTimeout` (5 minutes). The client sees a cut response
  (same outcome as the blocking path's write deadline). This is the
  safety net, not a bug: before it existed, such connections parked a
  `reactorConn` in the write state forever and silently consumed the
  per-loop connection budget (`reactorMaxConns`, 4096). A full budget
  surfaces as new hit-path connections being served by the slower
  blocking parser (over-cap handoff), visible as a p99 increase on the
  `http` (data) listener.
- **Dropped hit metrics**: the log line
  `H1 reactor dropped hit-metric records` at shutdown means the async
  metrics ring overflowed during that loop's lifetime — that many hit
  observations were not counted in `bouine_requests_total` /
  `request_duration_seconds` / `response_bytes_out_total` for the
  fast path. Steady state should see zero.
- **Spawner saturation**: under extreme miss storms, a full handoff
  spawn queue (128 slots per listener) resets the excess miss
  connections instead of serving them — clients see connection resets
  while hit-path latency stays flat. This is deliberate: the
  alternative would stall every cache hit multiplexed on that
  listener.
- **Starved loop** (diagnose with the telemetry below, not pprof): if
  `bouine_h1_reactor_hits_total` is flat while
  `bouine_requests_total{cache_result="HIT"}` climbs, the reactor is
  running but not serving. Before the return-to-reactor path
  (ADR-0042) a single miss exiled a connection to the blocking parser
  for its lifetime; mixed hit/miss workloads (cluster peer fetch,
  origin misses) left the reactor with ~zero connections. If it
  recurs, check `bouine_h1_reactor_returns_total` — a low return rate
  with high `handoffs_total{reason="miss"}` means connections are not
  coming back (pending-queue saturation or a regression in the return
  path).

## Diagnosis

1. Check `bouine_requests_total{cache_result="HIT"}` continuity across
   the restart window; the "dropped" log line quantifies exactly the
   gap for reactor-served hits.
2. Reactor engagement (ADR-0042 telemetry):
   - `bouine_h1_reactor_conns_registered_total` vs
     `bouine_h1_reactor_handoffs_total` — how much of the connection
     population the loop actually holds.
   - `bouine_h1_reactor_handoffs_total{reason}` — why connections
     leave: `miss` (no local object; peer/origin fetch), `disqualified`
     (conditional, range, body framing, non-GET/HEAD), `malformed`,
     `oversize` (>16 KiB headers), `overflow` (accept queue full),
     `cap` (reactor at 4096 conns).
   - `bouine_h1_reactor_returns_total` — blocking-parser hand-backs;
     this is what keeps mixed-traffic connections cycling back to the
     loop.
   - `bouine_h1_reactor_conns_dropped_total` — reactor-initiated
     closes (errors, idle expiry, stuck writers).
3. If drops recur (not just at shutdown): capture `pprof` CPU on the
   instance — the drainer goroutine (20 ms poll) being starved points
   to CPU saturation on the node, not a reactor defect. The drop
   counter is a symptom of node pressure, same class as GC throttle.
4. For stuck writers: no action is normally required (the sweep is the
   fix). If clients legitimately stream responses slower than
   5 minutes, raise the blocking-path write timeout and
   `reactorWriteTimeout` together — they must stay in sync (both are
   5-minute constants in `internal/server/h1parser` and
   `internal/server`).

## Mitigation

- Set `experimental.h1_reactor: false` to fall back to the blocking
  parser path (same fast-path hit core, goroutine-per-connection
  scheduling) if a reactor-specific issue is suspected mid-incident.
  The flag requires `experimental.h1_fast_path: true`.
- Rebalance load (or scale pods): drops and spawner saturation are
  node-pressure signals, not configuration errors.

## References

- `docs/plans/h1-reactor-perf-round-4.md` — W3 (async metrics ring)
  and W7 (stuck-writer safety net) rationale and contracts.
- `internal/server/h1parser/reactor_metrics.go` — ring semantics;
  drop-newest policy.
- `docs/decisions/0042-h1-epoll-reactor-return-path.md` —
  return-to-reactor, pipelined-inline hits, and the telemetry
  counters.
