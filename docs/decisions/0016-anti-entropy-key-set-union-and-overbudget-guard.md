# ADR-0016: Anti-entropy key-set union and over-budget backfill guard

- **Status**: Accepted
- **Date**: 2026-07-03
- **Deciders**: @thylong
- **Phase**: phase 4.5 (hardening)

## Context

ADR-0014 introduced anti-entropy reconciliation for `full` cluster mode.
`PeerKeysHandler` advertised the **hot-tier** key set (`HotStore.Keys()`),
and `TieredStore.Keys()` returned hot-only keys. Under memory pressure,
SIEVE evicts warm-backed keys from the hot tier; the warm tier still owns
them, but anti-entropy saw them as "missing" and backfilled them via `Put`,
re-overfilling the hot tier. SIEVE evicted them again next round — a
self-sustaining feedback loop (#175).

A secondary amplification compounded this: `reconcile()` built `localSet`
once per round and never updated it as peers were reconciled. A key
backfilled from peer 1 was still "missing" when diffing against peer 2, so
every peer backfilled the same keys once per round.

## Decision

1. **`TieredStore.Keys()` returns the hot + warm union.** The key set
   advertised to peers reflects keys the node *owns* (in either tier), not
   just those currently in RAM. `warm.Store.Keys()` was added to expose the
   warm-tier index; `TieredStore.Keys()` deduplicates across both tiers.

2. **`reconcile()` records backfilled keys in `localSet`** so subsequent
   peers in the same round see them as present, eliminating the
   N-times-per-peer backfill amplification.

3. **`OverBudget()` guards backfill.** Anti-entropy skips backfill when the
   hot tier exceeds its byte budget: once at the top of `reconcile()` (the
   sustained-pressure case, avoids N wasted peer key-set fetches) and once
   per-peer inside `reconcileWithPeer()` (the mid-round case where a prior
   peer's backfill pushed the store over budget). `OverBudget()` is declared
   on `cluster.Storer` (the consumer interface), not on `storage.Store`,
   keeping the cluster concern out of the cache-layer interface.

## Consequences

### Positive
- The eviction ↔ backfill feedback loop is broken for both warm-backed keys
  (union fix) and hot-only small keys (over-budget guard).
- Backfill amplification across peers in a single round is eliminated.
- Anti-entropy avoids N wasted network round-trips when the store is
  sustainably over budget (top-of-reconcile guard).

### Negative / trade-offs
- `PeerKeysHandler` now advertises warm-only keys. A peer backfilling from
  this node hits `TieredStore.Get` → warm hit → **promotes to hot** on the
  *serving* node (tiered.go:169). Serving a peer fetch thus re-overfills the
  serving node's hot tier. This is pre-existing behavior (peer fetch always
  promoted), but the union expands the set of keys that trigger it. It is
  not a loop (the serving node did not initiate the backfill) and is bounded
  by the serving node's own SIEVE eviction.
- `TieredStore.Keys()` allocates a map and slice for the union on every
  anti-entropy round. Acceptable: anti-entropy runs at 30s intervals and is
  not on the hit path.
- `OverBudget()` is a point-in-time snapshot (`Stats().HotBytes > maxBytes`).
  The store may transition between over and under budget between the
  top-of-round check and a per-peer check. The per-peer guard handles this.

### Risks
- A node that is persistently over budget will skip all backfill, so keys
  that exist only on peers will not be repaired until the store drops below
  budget. This is the intended behavior: anti-entropy should yield to
  eviction, not fight it. Once memory pressure subsides, backfill resumes.

## Alternatives considered

- **Keep hot-only `Keys()`, add per-key over-budget re-check inside the
  backfill loop.** Rejected: O(numShards) RLock acquisitions per key, and
  `Put`'s inline eviction already bounds the case it guarded (a single
  backfill inserting a giant object). The pre-loop guard is the cheap,
  sufficient defense for small hot-only keys that never reach warm.

- **Advertise warm-only keys separately (two key-set endpoints).** Rejected:
  unnecessary protocol complexity. The union is what anti-entropy needs —
  "which keys does this node own?"

## References

- Issue #175: Eviction / anti-entropy feedback loop.
- ADR-0014: Anti-entropy reconciliation for full cluster mode.
- `internal/storage/tiered.go` — `Keys()`, `OverBudget()`.
- `internal/cluster/antientropy.go` — `reconcile()`, `reconcileWithPeer()`.
