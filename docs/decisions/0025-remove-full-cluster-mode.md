# ADR-0025: Remove full cluster mode

- **Status**: Accepted
- **Date**: 2026-07-11
- **Deciders**: @thylong

## Context

Full cluster mode replicated every cached object to all peers, giving each
node a complete copy of the working set. This did not scale: memory grew
linearly with cluster size (N× working set), bandwidth was
`fills/s × avg_size × (N-1)` per node, and the anti-entropy reconciler
(472 lines) existed only to repair dropped replications. The mode caused
production pod restarts at 150 RPS due to gossip queue overflow, requiring
a migration from gossip to HTTP POSTs for replication transport.

## Decision

Remove `full` as a valid `cluster.mode` value. Users who need redundancy
should use `strong` with `replicas >= 2` (same HA guarantee, 1/N memory,
near-zero bandwidth). Users who need independent caching should use
`eventual`.

## Consequences

- `cluster.mode: full` is rejected at config validation time with a
  migration message.
- 4 config fields removed: `anti_entropy_interval`, `backfill_limit`,
  `backfill_cooldown`, `churn_threshold`.
- ~2000 lines of code removed (antientropy.go, replication transport,
  handlers, tests, dashboard UI, insights rules).
- ADRs 0014, 0018, 0019 superseded.

## Migration

- `full` → `strong` + `replicas: 2`: same redundancy, fraction of memory.
- `full` → `eventual`: zero cross-node bandwidth, independent caching.
