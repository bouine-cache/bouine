# ADR-0068: Remove the H1 epoll reactor

- **Status**: Accepted
- **Date**: 2026-10-26
- **Deciders**: @thylong
- **Phase**: performance (post-phase 9 fast path)
- **See also**: ADR-0041 (superseded), ADR-0034 (fasthttp adoption),
  docs/plans/h1-reactor-perf-round-4.md (historical)

## Context

ADR-0041 added `experimental.h1_reactor`: a Linux-only,
single-goroutine epoll event loop that batch-served fast-path cache
hits and handed every non-hit connection off to the blocking h1parser
path. The bet was that nginx-style event-loop batching would beat the
per-connection goroutine park/unpark cost on the hit path.

The bet did not pay off in production-shaped traffic. A 1-hour A/B
from the external stress harness (vegeta on Linux, 3-node cluster,
6k req/s, 100 connections; analysis dir `pr567-5d38db0-reactor-ab`)
measured **zero end-to-end benefit**, and the CPU profile explained
why: `reactorEpoll.run` consumed 0.01 s of 234 s of samples (0.004%),
while the blocking `Parser.Serve` did 46% of the work. Handoff was
terminal per connection, so under ~75% non-local-hit traffic (peer
fetch + origin miss) every long-lived connection left the reactor
within its first few requests and never returned. The only measured
win was an in-repo k6 scenario that is pure-hit, single-node,
keep-alive-only — a shape the reactor was hand-tuned for (bounded
adaptive busy-poll, `BOUINE_REACTOR_SPIN_BUDGET`) and that does not
occur on its own in real deployments.

Meanwhile the reactor carried real structural costs: a
platform-gated code path (Linux-only, never exercised on the darwin
dev matrix), an SPSC metrics ring with its own drop failure mode
(runbook 51), a hit-metrics drainer goroutine, and a hard
incompatibility with `experimental.h1_fast_peer_path` (TryHit must
never block the event loop on peer I/O). Round-4/round-5 perf plans
kept finding new work to make the reactor competitive, each with
non-trivial complexity (return-to-reactor, inline pipelined hits,
per-listener workers, io_uring).

## Decision

We remove the H1 epoll reactor entirely. All fast-path traffic uses
the blocking h1parser path (one accept goroutine per listener, one
goroutine per connection), which the reactor already fell back to for
every non-hit request.

Concretely:

- Delete `internal/server/h1parser/reactor*.go` (state machine,
  Linux epoll transport, non-Linux stubs, SPSC metrics ring, tests,
  Linux e2e suites, gate benchmarks).
- Remove the `experimental.h1_reactor` config field and its
  validation; strict parsing (`KnownFields`) now rejects the key with
  an unknown-field error, which is the migration signal.
- Remove the `BOUINE_REACTOR_SPIN_BUDGET` operator knob, the reactor
  runbook (51), and the `Reactor_Hit` / `Reactor_Hit_Metrics` /
  `Reactor_Dispatch` bench budgets.
- Drop the `h1_reactor` / `h1_fast_peer_path` incompatibility: the
  peer branch is per-connection blocking by design, the same
  concurrency the parser fallback path gives.

## Consequences

### Positive

- ~3.8k lines deleted, including the only `GOOS`-gated code in the
  data plane; dev machines now exercise the exact production path.
- One fewer metrics loss mode (dropped hit-metric records) and three
  fewer runbook failure modes; runbook surface shrinks.
- `experimental.h1_fast_peer_path` is no longer blocked by a flag
  interaction.
- The H1 fast path keeps its zero-alloc hit gates (`H1Parse_Get`,
  `FastPath_Hit`, `RoutedFastPath_Hit`): the parse/TryHit/serialize
  core the reactor reused is unchanged.

### Negative / trade-offs

- Pure-hit keep-alive microbenchmarks lose the reactor's busy-poll
  RTT win (the k6 4x keep-alive RTT scenario). No measured production
  shape regresses; if a pure-hit workload ever dominates, the correct
  fix is round-5's W4 (reactor workers per listener), not restoring
  ADR-0041.
- Configs setting `experimental.h1_reactor` fail to load until the
  key is removed. Acceptable for an experimental flag documented as
  default-off; the error names the field.

### Risks

- A future hit-path latency regression could be misattributed to this
  removal. Mitigation: the nightly load-test baseline now runs
  blocking-path-only, so all future deltas are measured against the
  post-removal world.

## Alternatives considered

- **Keep the reactor, land the round-5 W4/W5/W6 work** (return-to-
  reactor, per-listener workers, store de-contention, io_uring):
  rejected — each step is complexity purchased for a workload shape
  (pure-hit, single-node) that the stress harness showed does not
  exist in the measured deployment, while the blocking path already
  delivers the same end-to-end numbers.
- **Keep the code, default the flag off**: rejected — the flag was
  already default-off; dead platform-specific code with its own
  failure modes and telemetry is the cost being removed.

## References

- ADR-0041 (superseded by this ADR)
- ADR-0034 (one HTTP stack: fasthttp)
- `docs/plans/h1-reactor-perf-round-4.md` (historical plan)
- Stress-harness A/B analysis `pr567-5d38db0-reactor-ab` (external)
