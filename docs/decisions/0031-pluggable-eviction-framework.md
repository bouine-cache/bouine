# ADR-0031: Pluggable eviction framework

- **Status**: Accepted
- **Date**: 2026-08-17
- **Deciders**: @thylong
- **Phase**: phase 3
- **Related**: ADR-0026 (SIEVE sweep cap), ADR-0023 (warm-tier SIEVE)
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

`docs/architecture.md` phase 3 introduces `cachaner` as an alternative policy and
requires that the policy be selectable via config per tier. That is
not achievable when the tier code constructs concrete `sieve.List`
directly.

`cachaner` extends SIEVE's 1-bit visited field with a 3-bit
saturating frequency counter, giving hot objects up to 7 second
chances (vs SIEVE's 1) before eviction. A 5-minute overflow soak
comparison (2026-08-17) showed the freq-only variant delivers a -4.1%
origin bandwidth saving and -20% RSS at the cost of +5 us p50 latency
— within noise on hit ratio. The "cost" component (3-bit refetch-cost
bucket based on body size + origin latency) was removed before testing
and is not justified by the data.

`AGENTS.md` §10 requires an ADR for eviction-algorithm changes.

## Decision

We extract a shared `evictor` package (`internal/storage/evictor`)
that defines the common data structures and interface:

- `Entry[K comparable]` — the linked-list node, embedded by both tiers
  alongside their per-key metadata. Same 40-byte layout as the old
  `sieve.Entry` on 64-bit: 16B key + 4B `atomic.Bool` visited + 4B
  `ioBits` + 8B prev + 8B next. The `visited` field is `atomic.Bool`
  so the hot store can read it safely under a shard read lock (the
  hit-path fast path) without a data race against the eviction path
  that writes under the shard write lock. The `ioBits` field fills the
  4B padding slot that would otherwise follow `atomic.Bool` — zero
  memory overhead for SIEVE users (ioBits stays at 0 under SIEVE).
- `List[K comparable]` — the interface implemented by concrete policies
  (`sieve.List`, `cachaner.List`). Methods: `Access`, `EvictBounded`,
  `Remove`, `Len`, `Clear`. The hit-path fast path does NOT dispatch
  through this interface — it reads `Entry.Visited()` directly.
  Interface dispatch occurs only on the slow path (Access) and the
  eviction path (EvictBounded), neither of which is the 0-alloc hit
  path.
- `EntryPool[K]` — a typed `sync.Pool` wrapper that recycles `Entry`
  structs across insertion/removal cycles, avoiding per-`Access`
  allocation.

Both tiers hold `evictor.List[api.Key]` and `*evictor.Entry[api.Key]`
instead of concrete `sieve` types. Each tier has a `newEvictList`
dispatch function that returns `sieve.NewList()` by default and
branches on `cfg.HotEvictionAlgorithm` / `cfg.WarmEvictionAlgorithm`
when a second policy is selected.

### `cachaner` policy

The shipped `cachaner` policy is frequency-only. If a cost-aware
variant is added later, it is additive — no rename needed.

Config: `storage.eviction_algorithm` is the shared default for both
tiers, accepting `""`, `"sieve"`, or `"cachaner"`.
`storage.hot_eviction_algorithm` and `storage.warm_eviction_algorithm`
override the shared default per-tier. When non-empty, the per-tier
field takes precedence. This lets operators use `cachaner` on both
tiers with one knob, or experiment with different policies per tier
(e.g. `cachaner` on hot, `sieve` on warm) for A/B testing.

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
  struct. The `cachaner` policy uses this slot via `ioBits`.
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
- Zero memory regression for SIEVE users (same 40-byte `Entry`,
  `ioBits` fills existing padding).
- Zero alloc regression on the hit path (no interface dispatch on the
  fast path, verified by `BenchmarkGate_SIEVE_Access` and
  `BenchmarkGate_Cachaner_Access`, both 0 allocs/op).
- `EntryPool` centralizes the pool contract (`Get` returns reset
  entries, `Put` accepts ownership), replacing per-policy `sync.Pool`
  boilerplate.
- Removing `Evict()` and `Defer()` shrinks the interface to what
  production actually calls, reducing the implementation burden for
  future policies.
- The framework is minimal: no `AccessWithMeta`, no `size` field, no
  dead scaffolding. Metadata plumbing is added in the same PR that
  needs it.

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
- The `evictor.List` interface adds one level of indirection on the
  slow path (Access) and eviction path (EvictBounded). Neither is the
  0-alloc hit path, so the cost is negligible (~1.9 ns/op for the
  freq slow path, ~14 ns/op for freq eviction).
- `cachaner`'s uncapped `Evict()` budget is `len * 9` (vs SIEVE's
  `len * 2`), reflecting the extra freq second chances. The hot tier
  uses `EvictBounded(128)` regardless of policy, so hot-tier sweep
  latency is capped. The 9x factor only applies to warm-tier
  eviction.

### Risks
- A future policy that needs >4B of per-entry state will require
  splitting `Entry`, which touches both tiers and all call sites. The
  `newEvictList` dispatch function localizes the construction change,
  but the `Entry` type change is cross-cutting. Mitigation: the
  `cachaner` policy validates the 4B slot is sufficient for a
  frequency counter. If it fits, the constraint is deferred further.
- The `Access` callback pattern (`lookup func(K) *Entry[K]`) is less
  ergonomic than a direct map lookup. It exists so the policy controls
  whether to reuse an existing entry or insert a new one, without the
  tier having to pass the map into the policy (which would couple the
  policy to the tier's index type).
- **Freq counter saturates at 7, no aging**: stale-popularity drift
  could keep dead objects alive too long. The 5-minute soak showed no
  drift. Revisit after a 30m+ soak; add aging if needed.
- **Stale `cachaner` entries after policy switch**: changing
  `hot_eviction_algorithm` requires a restart (the list is allocated
  at construction time). This is documented in the runbook.

## Alternatives considered

- **Per-policy `Entry`, shared interface only.** Each policy owns its
  `Entry` type (e.g. `sieve.Entry`, `cachaner.Entry`). `List` is
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

- **Ship with the cost plumbing but no cost feature**:
  rejected. Dead scaffolding (`size`, `SetSize`, `AccessWithMeta`,
  `useMetaAccess`) is premature abstraction. None of it is needed for
  the freq-only policy. When the cost bucket is designed and tested,
  `AccessWithMeta` is added in the same PR — mechanical, one method,
  all implementors updated.

- **Separate `hot_eviction_algorithm` and `warm_eviction_algorithm`
  with no shared default**: rejected. Two independent knobs with no
  shared default is error-prone — operators must set both or risk
  asymmetric behavior. The shared `eviction_algorithm` field with
  per-tier overrides gives a single knob for the common case (both
  tiers use the same policy) while still allowing per-tier
  experimentation.

- **Keep SIEVE as the only policy, hardcode freq as a flag**: rejected.
  The abstraction is free (zero memory, zero hit-path cost) and enables
  A/B testing in production without code changes.

## References

- ADR-0023: Warm-tier eviction (SIEVE) — established SIEVE as the warm
  tier policy.
- ADR-0026: SIEVE sweep cap — introduced `maxSweepProbes` and
  `EvictBounded`; the basis for removing `Evict()`.
- `docs/architecture.md` phase 3: `cachaner` policy, per-tier config selection.
- `AGENTS.md` §7: hit-path budget (0 allocs, < 5 us p50).
- `AGENTS.md` §10: ADR required for eviction-algorithm changes.
- SIEVE paper: Zhang et al., "SIEVE is Simpler than LRU", NSDI 2024
- Soak comparison: 2026-08-17 overflow soak, `sieve` vs `cachaner`
