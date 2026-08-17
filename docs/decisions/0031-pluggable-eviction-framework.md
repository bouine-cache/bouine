# ADR-0031: Pluggable eviction framework

- **Status**: Accepted
- **Date**: 2026-08-17
- **Deciders**: @thylong
- **Phase**: phase 3
- **Consulted**: (none)
- **Informed**: (none)

## Context

Both the hot tier (`internal/storage/hot.go`) and the warm tier
(`internal/storage/warm/`) use SIEVE for eviction. Until this ADR, the
SIEVE implementation lived in `internal/storage/sieve` and both tiers
held concrete `*sieve.List` and `*sieve.Entry` types directly. This
coupled both tiers to a single policy and meant that adding a new
policy (e.g. LRU-K, LFU, ARC) required touching
every call site in both tiers.

`PLAN.md` phase 3 introduces `sieve_freq` as an alternative policy and
requires that the policy be selectable via config per tier. That is
not achievable when the tier code constructs concrete `sieve.List`
directly.

`AGENTS.md` §10 requires an ADR for eviction-algorithm changes.

## Decision

We extract a shared `evictor` package (`internal/storage/evictor`)
that defines the common data structures and interface:

- `Entry[K comparable]` — the linked-list node, embedded by both tiers
  alongside their per-key metadata. Same 40-byte layout as the old
  `sieve.Entry` on 64-bit: 16B key + 4B `atomic.Bool` visited + 4B pad
  + 8B prev + 8B next. The `visited` field is `atomic.Bool` so the hot
  store can read it safely under a shard read lock (the hit-path fast
  path) without a data race against the eviction path that writes under
  the shard write lock.
- `List[K comparable]` — the interface implemented by concrete policies
  (`sieve.List`, future `freqcost.List`). Methods: `Access`,
  `EvictBounded`, `Remove`, `Len`, `Clear`. The hit-path fast path does
  NOT dispatch through this interface — it reads `Entry.Visited()`
  directly. Interface dispatch occurs only on the slow path (Access)
  and the eviction path (EvictBounded), neither of which is the 0-alloc
  hit path.
- `EntryPool[K]` — a typed `sync.Pool` wrapper that recycles `Entry`
  structs across insertion/removal cycles, avoiding per-`Access`
  allocation.

Both tiers hold `evictor.List[api.Key]` and `*evictor.Entry[api.Key]`
instead of concrete `sieve` types. Each tier has a `newEvictList()`
dispatch function that returns `sieve.NewList()` today and will branch
on a config field when the second policy lands.

### Shared `Entry` design: accept coupling

We accept that `evictor.Entry` bakes SIEVE's `visited` bit into the
common struct. This is a deliberate tradeoff:

- **Zero overhead for SIEVE users.** The 40-byte layout is identical to
  the old `sieve.Entry`, verified by `TestObjSize_StructSizeConstantsNotDrifted`
  and `TestEstimatedWarmLocHeapBytesAccountsForStructSizes`. Any
  alternative that per-policy state would add memory cost for SIEVE,
  violating the hit-path budget.
- **4B padding slot for future policies.** The struct has 4 bytes of
  padding between `visited` and `prev`. A frequency counter
  (`uint32`) or a small cost bucket ID fits there without growing the
  struct. The `sieve_freq` policy (next PR) uses this slot.
- **Revisit if a policy needs >4B.** If a future policy needs more
  per-entry state than fits in the padding, we split `Entry` into a
  per-policy type and make `List` generic over it. That is a larger
  refactor but is deferred until the constraint is real, not
  hypothetical.

### What is NOT in the interface

- `Evict()` (unbounded sweep) — removed. It was equivalent to
  `EvictBounded(len * 2)`, which defeats the `maxSweepProbes` cap
  introduced by ADR-0026. No production caller used it. Tests use
  `EvictBounded(l.Len() * 2)` directly when they need an unbounded
  sweep.
- `Defer()` (move-to-head preserving visited) — removed. After the
  `evictPreferBacked` bug fix (re-insert via `Access + MarkVisited`
  instead of `Defer` on a freed entry), no production caller uses
  `Defer`. The method was a SIEVE-specific primitive that every future
  policy would have to implement without ever calling.
- `AccessWithMeta()` — not added. No shipped policy uses metadata on
  access. When a policy needs it, the method is added in the same PR
  that needs it, with all implementors updated mechanically.

## Consequences

### Positive
- Both tiers can switch policy via config without touching call sites.
- Zero memory regression for SIEVE users (same 40-byte `Entry`).
- Zero alloc regression on the hit path (no interface dispatch on the
  fast path).
- `EntryPool` centralizes the pool contract (`Get` returns reset
  entries, `Put` accepts ownership), replacing per-policy `sync.Pool`
  boilerplate.
- Removing `Evict()` and `Defer()` shrinks the interface to what
  production actually calls, reducing the implementation burden for
  future policies.

### Negative / trade-offs
- `evictor.Entry` couples all policies to SIEVE's data shape. A policy
  needing >4B of per-entry state triggers a refactor to per-policy
  `Entry` types. This is accepted: the constraint is not yet real, and
  premature abstraction would add complexity without benefit.
- The `visited` field on `Entry` is meaningless for policies that don't
  use a visited bit (e.g. pure LRU). They pay 4B of dead space per
  entry. This is the same 4B that SIEVE users pay for padding, so the
  cost is bounded and already accounted for in the heap-size
  constants.

### Risks
- A future policy that needs >4B of per-entry state will require
  splitting `Entry`, which touches both tiers and all call sites. The
  `newEvictList` dispatch function localizes the construction change,
  but the `Entry` type change is cross-cutting. Mitigation: the
  `sieve_freq` policy (next PR) validates the 4B slot is sufficient
  for a frequency counter. If it fits, the constraint is deferred
  further.
- The `Access` callback pattern (`lookup func(K) *Entry[K]`) is less
  ergonomic than a direct map lookup. It exists so the policy controls
  whether to reuse an existing entry or insert a new one, without the
  tier having to pass the map into the policy (which would couple the
  policy to the tier's index type).

## Alternatives considered

- **Per-policy `Entry`, shared interface only.** Each policy owns its
  `Entry` type (e.g. `sieve.Entry`, `freqcost.Entry`). `List` is
  generic over the entry type: `List[E any, K comparable]`. Pros:
  zero coupling, no dead space. Cons: both tiers must be generic over
  the entry type, which propagates generics through `hotEntry`,
  `warmLoc`, `shard`, and every function that touches an entry. The
  complexity is not justified until a policy actually needs more state
  than the shared `Entry` provides. Rejected as premature.

- **Generic `Entry[PolicyState any]`.** `Entry[K comparable, S any]`
  with SIEVE using `struct{}` (40B) and freq using `struct{ count
  uint32 }` (still 40B via padding fill). Pros: type-safe per-policy
  state, no dead space. Cons: the extra type parameter propagates
  through every function signature and both tiers. Same complexity
  concern as above. Rejected; the 4B padding slot achieves the same
  memory density without the type-parameter sprawl.

- **Keep `Evict()` and `Defer()` in the interface.** Pros: preserves
  the SIEVE API surface for future policies that might want them.
  Cons: every policy implements methods nobody calls, and `Evict()`
  is a footgun (bypasses the sweep cap). Rejected; YAGNI.

## References

- ADR-0023: Warm-tier eviction (SIEVE) — established SIEVE as the warm
  tier policy.
- ADR-0026: SIEVE sweep cap — introduced `maxSweepProbes` and
  `EvictBounded`; the basis for removing `Evict()`.
- `PLAN.md` phase 3: `sieve_freq` policy, per-tier config selection.
- `AGENTS.md` §7: hit-path budget (0 allocs, < 5 us p50).
- `AGENTS.md` §10: ADR required for eviction-algorithm changes.
