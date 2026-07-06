# ADR-0018: Backfill cooldown for anti-entropy reconciler

- **Status**: Accepted
- **Date**: 2026-07-06
- **Deciders**: @thylong
- **Phase**: phase 4.5 (hardening)

## Context

Anti-entropy reconciliation (ADR-0014) backfills missing keys from peers
every 30s in full cluster mode. SIEVE eviction evicts freshly-backfilled
keys before the next round — they are low-priority (just inserted, never
served) — so the same keys are "missing" again every round. The
reconciler re-fetches them, SIEVE evicts them again, and the loop runs
unbounded as long as the hot tier is under its memory budget. The
existing `OverBudget` guard only fires when `hotBytes > maxBytes`; it
does not detect churn under budget (issue #187, fix #2).

In preprod this caused 200+ MB/s of garbage allocation at 0.1 RPS of
real traffic, driving GC to 40–65 cycles/min.

## Decision

We add a per-key backfill cooldown: after a successful `backfillKey`,
the key is recorded in a `map[api.Key]time.Time` on the `AntiEntropy`
struct with an expiry of `now + BackfillCooldown`. The cooldown check
lives in the missing-key diff — before the peer-fetch RPC is issued —
so cooled-down keys generate zero network traffic, not merely a skipped
store write. Expired entries are pruned at the top of each reconcile
round to bound memory.

The cooldown is configured via `cluster.backfill_cooldown` (default `0`
= disabled for backward compatibility). The recommended value is `5m`
(≤ 10 rounds at the default 30s interval).

The cooldown map is accessed only from the single reconcile goroutine;
no mutex is needed. An injectable `now func() time.Time` on the
`AntiEntropy` struct makes the cooldown deterministic in tests.

A `bouine_cluster_anti_entropy_cooldown_skips_total` counter surfaces
sustained SIEVE eviction pressure to operators.

## Consequences

### Positive
- Breaks the self-sustaining backfill storm without requiring the hot
  tier to hold the full key set.
- Zero network traffic for cooled-down keys (skip is before the RPC).
- O(1) per-key overhead (one map lookup); benchmarked at 0 allocs/op.
- Backward compatible: default `0` preserves existing behavior.

### Negative / trade-offs
- Delayed convergence: a key that is legitimately missing (not
  SIEVE-evicted, but never replicated) will not be repaired until the
  cooldown window expires. This is acceptable in full mode where
  replication gossip is the primary delivery path; anti-entropy is the
  repair mechanism, not the primary sync.
- The cooldown is per-node, not cluster-wide. Different nodes may
  backfill the same key at different times, but since each node tracks
  its own cooldown independently, this is correct — a node only skips
  keys it itself backfilled.

### Risks
- If `BackfillCooldown` is set too high relative to `Interval`, drift
  repair latency grows. The recommended 5m / 30s ratio gives ≤ 10
  rounds of skip, which is the maximum useful window (after that the
  key is either served and promoted, or the operator has grown the hot
  tier).

## Alternatives considered

- **LRU or bloom filter for the cooldown map**: rejected for now.
  Preprod scale is 17k keys; a plain `map[api.Key]time.Time` is fine.
  Optimize later if benchmarks show pressure.

- **Detect churn in the `OverBudget` guard (fix #5)**: complementary,
  not alternative. The cooldown prevents the storm before it starts;
  the churn detector would catch the case where the hot tier is
  actively evicting backfilled keys faster than they are inserted.
  Implemented in ADR-0019.

- **Set a non-zero `BackfillLimit` default (fix #1)**: complementary.
  `BackfillLimit` caps the storm volume per round; the cooldown breaks
  the self-sustaining loop. Both are needed. Tracked as a separate PR.

## References

- Issue #187 (fix #2)
- ADR-0014: Anti-entropy reconciliation for full cluster mode
- `internal/cluster/antientropy.go`
- AGENTS.md §10 (ADR requirement for cluster protocol changes)
