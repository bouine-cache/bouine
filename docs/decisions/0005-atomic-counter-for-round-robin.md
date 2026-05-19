# ADR-0005: Round-robin upstream selection uses an atomic counter

- **Status**: Accepted
- **Date**: 2026-05-19
- **Deciders**: @thylong
- **Phase**: phase 1 (pre-flight)

## Context

`internal/origin` (L5) must pick a target inside an upstream pool on
every fetch. Phase-1 ships the simplest reasonable selector —
round-robin — and graduates to weighted / least-conn / P2C in phase 5
(`PLAN.md §15`).

The seed strategy matters in three places:

1. **Determinism in tests.** If selection is seeded with rand at
   process start, tests cannot assert on which target was hit without
   reseeding or injecting a clock.
2. **Performance.** Every fetch hits this code. `crypto/rand` is out;
   even `math/rand.Intn` allocates a `*Rand` state when sharded.
3. **Fairness.** Random start can create transient hot spots when
   the pool size is small and request rate is bursty.

## Decision

Round-robin uses a per-pool `sync/atomic.Uint64` counter, incremented
by `Add(1)` on every fetch, modulo the live target count.

- Counter starts at `0`, NOT a random offset.
- Pool reconfiguration (target added / removed) does NOT reset the
  counter; the modulo absorbs the change.
- "Live targets" excludes targets ejected by passive health checks.
- The selection function is allocation-free and lock-free.

```go
type RR struct{ next atomic.Uint64 }

func (r *RR) Pick(targets []*Target) *Target {
    n := uint64(len(targets))
    if n == 0 { return nil }
    return targets[r.next.Add(1)%n]
}
```

## Consequences

### Positive
- Trivially deterministic in tests: the Nth fetch always hits
  `targets[N % len]`.
- Allocation-free, lock-free, fits cleanly in the hit-path budget
  (`AGENTS.md §7`).
- Predictable distribution for small pools.

### Negative / trade-offs
- Two daemons that boot at the same time and immediately receive
  identical traffic will hit the same target — a synchronization
  artifact that doesn't matter at any meaningful traffic level but
  could confuse the very first integration test. Mitigated by the
  health-check warmup period (phase 1) that staggers traffic.
- Round-robin is naïve. Weighted / EWMA selection ship in phase 5 and
  remain optional (`PLAN.md §15`).

### Risks
- None notable.

## Alternatives considered

- **Random selection with `math/rand.Intn`** — rejected. Allocates a
  state object per goroutine if used safely, or adds a mutex around a
  shared source. Less testable.
- **Time-based seed** — rejected. Non-deterministic, harder to test.
- **Power-of-two-choices (P2C) from day one** — rejected for phase 1.
  Requires latency tracking that ships in phase 3 alongside hedged
  requests. Once ready, P2C becomes the default and round-robin
  remains as an opt-in fallback.

## References

- `PLAN.md §6`, `§15`
- `AGENTS.md §7` (hit-path budget), `§11` (concurrency)
