# ADR-0019: Churn detection for anti-entropy backfill


> **Superseded** by [ADR-0025](0025-remove-full-cluster-mode.md). Full cluster mode has been removed.
- **Status**: Accepted
- **Date**: 2026-07-06
- **Deciders**: @chrisdupin
- **Phase**: phase 4.5 (hardening)

## Context

The `OverBudget` guard (ADR-0014) skips anti-entropy backfill when
`hotBytes > maxBytes`. The backfill cooldown (ADR-0018) suppresses
re-fetching the *same* key for N rounds after it was backfilled. Neither
detects the underlying condition that causes the backfill storm: **SIEVE
is evicting recently-backfilled keys faster than the reconciler is
inserting them** (issue #187, fix #5).

When the hot tier is under its memory budget — the preprod case (60 MB <
max) — `OverBudget` never fires. SIEVE evicts freshly-backfilled keys
(low priority: just inserted, never served) before the next round. The
cooldown prevents re-fetching the *same* keys, but every *new* key we
backfill will also be evicted. Backfill is wasted work regardless of the
cooldown — the work just shifts to new keys that are also doomed.

## Decision

We extend the top-of-reconcile guard with a **churn detector** that reuses
the cooldown map from ADR-0018 as its measurement window. The cooldown map
tracks keys backfilled in the last `BackfillCooldown` duration. At the top
of each round, after pruning expired cooldown entries and building the
local key set, the reconciler counts cooldown keys that are **absent from
the local key set** — those are keys SIEVE evicted after backfill. When
the evicted-to-backfilled ratio exceeds `ChurnThreshold`, the round is
skipped the same way `OverBudget` skips: log a warning, set keys-repaired
to 0, return.

This requires **no storage-layer changes**. The cooldown map (PR #191) is
the window; the local key set (`KeySource.Keys()`) is already computed by
reconcile. A key in the cooldown map that is absent from the local key set
was evicted by SIEVE — no eviction callback or SIEVE instrumentation is
needed.

The threshold is configured via `cluster.churn_threshold` (default `0` =
disabled for backward compatibility). The value is a float in `[0, 1]`. A
reasonable default is `0.5` (skip when more than half of recent backfills
were evicted). Churn detection requires `BackfillCooldown > 0` — the
cooldown map is the measurement window. With `BackfillCooldown = 0` the
cooldown map is empty and churn detection is a no-op.

A `bouine_cluster_anti_entropy_churn_skips_total` counter surfaces
sustained churn to operators.

## Consequences

### Positive
- Catches the under-budget churn condition that `OverBudget` misses.
- Zero storage-layer changes — reuses the cooldown map and local key set.
- Round-level check: no per-key overhead on the hot path. The churn scan
  is O(cooldown entries) per round, not per request.
- Backward compatible: default `0` preserves existing behavior.

### Negative / trade-offs
- Coupled to `BackfillCooldown`: churn detection is only active when the
  cooldown is enabled. This is acceptable because the cooldown is the
  recommended configuration for full mode (ADR-0018) and the cooldown map
  is the natural measurement window — duplicating it would waste memory.
- Delayed convergence: when churn is detected, the round is skipped
  entirely. New missing keys are not backfilled until churn subsides.
  This is the intended behavior — backfilling into a churning cache is
  wasted work.
- Per-node, not cluster-wide: each node detects its own churn
  independently. This is correct — a node only skips when its own SIEVE
  is evicting its own backfills.

### Risks
- If `ChurnThreshold` is set too low (e.g. 0.01), churn detection may
  fire too aggressively, stalling convergence. The recommended 0.5
  threshold requires a majority of backfills to be evicted before
  skipping — a clear signal that the hot tier is undersized.
- The churn scan iterates the cooldown map each round. At preprod scale
  (17k keys, ≤ 10 rounds of cooldown) this is negligible. If the
  cooldown map grows very large, the scan cost grows linearly —
  documented as a future optimization target if benchmarks show pressure.

## Alternatives considered

- **SIEVE eviction callback**: add a counter or callback to
  `internal/storage/sieve/` that fires when a recently-backfilled key is
  evicted. Rejected for now — it requires storage-layer changes and
  coupling between the SIEVE eviction path and the cluster layer (which
  would violate the layer boundary in AGENTS.md §3). The cooldown-map
  approach achieves the same signal without crossing layers.

- **Separate "recently backfilled" set**: maintain a dedicated set of
  recently-backfilled keys, independent of the cooldown map. Rejected —
  it duplicates state already tracked by the cooldown map and wastes
  memory. The cooldown map is the natural window.

- **Eviction rate vs backfill rate**: compute a rate (evictions/sec vs
  backfills/sec) instead of a ratio. Rejected — a ratio is simpler, has
  no time-unit dependency, and the cooldown window already bounds the
  time horizon. A rate would require a rolling time window and more
  state.

## References

- Issue #187 (fix #5)
- ADR-0014: Anti-entropy reconciliation for full cluster mode
- ADR-0018: Backfill cooldown for anti-entropy reconciler
- `internal/cluster/antientropy.go`
- AGENTS.md §10 (ADR requirement for cluster protocol changes)
