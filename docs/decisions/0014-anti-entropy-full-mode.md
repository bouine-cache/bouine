# ADR-0014: Anti-entropy reconciliation for full cluster mode


> **Superseded** by [ADR-0025](0025-remove-full-cluster-mode.md). Full cluster mode has been removed.
- **Status**: Accepted
- **Date**: 2026-06-29
- **Deciders**: @thylong
- **Phase**: phase 4.5 (hardening)

## Context

`full` cluster mode promises that every node holds every cached object.
In practice replication was fire-and-forget gossip (`memberlist.SendBestEffort`,
UDP). Dropped messages are silently lost with no repair. Preprod pods diverged
(218 / 220 / 242 object counts) with zero evictions — the delta was missing
data, not churn.

The package doc and `docs/architecture.md §5.4` both promised "anti-entropy
reconciliation," but `MergeRemoteState` only reconciled the ring/membership
digest — it never touched the object store. The promised mechanism was never
built.

## Decision

Implement anti-entropy for `full` mode using a **sorted hashset diff**
approach: each node periodically exchanges its complete key set with peers
via `GET /v1/peer/keys`, computes the diff locally, and backfills missing
objects via the existing peer-fetch HTTP path (`POST /v1/peer/fetch`).

### Design

- **Key-set exchange**: `GET /v1/peer/keys` returns a `KeySet` JSON payload
  containing the node name and all cache keys (uint64 hashes). Auth-exempt,
  same as peer-fetch.
- **Diff**: the reconciler builds a `map[api.Key]struct{}` of local keys,
  then iterates the peer's key set to find missing keys.
- **Backfill**: for each missing key, the reconciler issues a
  `PeerFetchRequest` to the peer that reported it. Backfill is bounded per
  round (`BackfillLimit`, default 0 = unlimited but rate-limited by
  `FetchTimeout`).
- **Interval**: configurable via `cluster.anti_entropy_interval` (default
  30s). Set to 0 to disable.
- **Scope**: `full` mode only. `strong` and `eventual` do not need it.

### Metrics

- `bouine_cluster_anti_entropy_reconcile_total{direction="sent"}` — rounds
  completed.
- `bouine_cluster_anti_entropy_repaired_total` — individual keys backfilled.
- `bouine_cluster_anti_entropy_keys_repaired` — gauge of keys repaired in
  the last round.

## Consequences

### Positive
- `full` mode now converges: after a forced drop of N% of gossip messages,
  the cluster reconverges to equal object counts within 2 reconciliation
  intervals.
- Reuses the existing peer-fetch HTTP path — no new transport.
- Bounded: backfill is rate-limited and capped per round.
- The doc finally matches the code.

### Negative / trade-offs
- Key-set exchange transfers the full key list every round. For very large
  caches (>1M keys) this is ~8 MB of JSON per peer per round. Acceptable for
  the small clusters (2–5 nodes) that `full` mode targets.
- Backfill adds load to the peer-fetch path during reconciliation. Mitigated
  by the `FetchTimeout` and `BackfillLimit`.
- Does not handle deletion propagation — a key deleted on one node is not
  removed from peers. This is correct: purges already propagate via gossip;
  anti-entropy only fills gaps, it does not reconcile deletions.

### Risks
- On a partitioned node, anti-entropy will try to fetch from a peer that is
  unreachable. The `FetchTimeout` bounds each attempt; the round moves on.
- During a rolling restart, all nodes may start reconciling simultaneously.
  The staggered start (ticker-based) spreads the load.

## Alternatives considered

- **Merkle tree**: build a per-shard Merkle tree and exchange only the root.
  On mismatch, walk the tree to find divergent leaves. Rejected: the tree
  construction is O(n) per round, same as the hashset diff, but with more
  code complexity and two round-trips per peer (root exchange + leaf
  exchange). The hashset approach is one round-trip and simpler.
- **Full snapshot exchange**: serialize the entire object store and diff.
  Rejected: O(object_size) not O(key_count) — far more bandwidth for no
  benefit when the peer-fetch path already exists for backfill.
- **Sync replication**: make `BroadcastReplicate` synchronous with ACKs.
  Rejected: changes the gossip transport contract and adds latency to the
  fill path. Anti-entropy is a repair mechanism, not a primary delivery
  path.

## References

- Issue #98: Full cluster mode never converges.
- Issue #99: Implement anti-entropy object reconciliation for full cluster
  mode.
- ADR-0008: Cluster consistency modes — strong, eventual, full.
- RFC 9111 §4.2.1: Warnings (stale-while-revalidate semantics).
