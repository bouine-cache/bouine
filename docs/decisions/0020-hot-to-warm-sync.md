# ADR-0020: Hot→Warm background sync with eviction tombstones

- **Status**: Accepted
- **Date**: 2026-07-06
- **Deciders**: @thylong
- **Phase**: phase 4.5 (hardening)

## Context

`body_threshold` defaults to 64 KiB and is not exposed in config. Only
objects larger than 64 KiB are written to the warm tier during normal
operation. The majority of cacheable objects by request count (API
JSON, HTML, CSS/JS) are smaller and live hot-only — lost on every pod
restart. The warm tier survives (PVC-backed StatefulSet), but it only
contains the large objects.

Setting `body_threshold` to 0 would write all objects to warm on every
Put, but that adds two `fsync` calls per cache fill (one segment sync,
one WAL sync), crushing miss-path throughput — especially on
network-attached storage (EBS, GCP PD).

Draining hot to warm on graceful shutdown would capture the working
set at SIGTERM, but does not survive crashes or OOM kills.

## Decision

Implement a **recurrent background hot→warm sync** in `TieredStore`.
A `warmSyncLoop` goroutine (sibling to `compactLoop`) periodically
batches hot-only entries into the warm tier. SIEVE's eviction policy
is the prioritization function — entries that survive in hot are the
popular ones, so a snapshot of `hot.Keys()` captures the working set.

### Sync cycle

1. Drain `tombstoneQueue` — tombstone evicted warm-backed keys in warm.
2. Snapshot `hot.Keys()` and `warm.Keys()`, compute the diff (hot-only).
3. Cap at `warmSyncBatchSize` (default 5000) with rotation.
4. Write each entry to warm, collect WAL entries.
5. Single `warm.Sync()` + single `wal.AppendBatch()` — one fsync pair
   per cycle, not per entry.

### Eviction tombstones

When SIEVE evicts a `hasWarm` entry from hot, the warm copy becomes
stale (the object was unpopular). An `OnEvict` callback on `HotStore`
enqueues the key into a bounded `tombstoneQueue` (capacity 4096,
non-blocking send). The sync loop drains the queue and tombstones the
key in warm + WAL.

The callback executes under the shard write lock — it MUST NOT block,
perform I/O, or call back into HotStore. This constraint is documented
on the `OnEvict` type.

### WAL batching

`wal.AppendBatch` writes multiple entries and syncs once. Existing
`wal.Append` (per-entry fsync) is unchanged for callers that need
per-operation durability.

### WAL rewrite after compaction

After a successful `warm.Compact()`, the WAL is rewritten with the
live warm-tier index (write to temp file, atomic rename). This keeps
the WAL bounded — without it, the WAL grows monotonically and startup
replay time grows with it.

### Startup fallback

If WAL replay produces an empty index but warm segments contain
records, the index is rebuilt from a full segment scan. This protects
against all WAL loss scenarios.

### Config

| Field | Default | Effect |
|-------|---------|--------|
| `body_threshold` | 64 KiB | Objects above this go to warm on every Put |
| `warm_sync_interval` | 60s | Hot→warm sync cycle period; -1 = disabled |
| `warm_sync_batch_size` | 5000 | Max entries per sync cycle |

## Consequences

### Positive
- Small objects survive restarts — the primary goal.
- Zero fill-path overhead: no per-fill fsync for small objects.
- Survives crashes (up to one sync interval of data loss).
- SIEVE is the prioritization function — no explicit request counting.
- WAL stays bounded via rewrite after compaction.
- Backward compatible: `warm_sync_interval: 0` preserves old behavior.

### Negative / trade-offs
- Up to one sync interval (60s default) of data loss on crash for
  hot-only entries written since the last cycle.
- Background goroutine adds periodic disk I/O (one fsync pair per
  cycle).
- Tombstone queue overflow drops tombstones (stale warm entries served
  after restart — correct but wasteful).

### Risks
- `OnEvict` callback runs under shard write lock. A buggy callback
  that blocks stalls all readers/writers on the shard. Mitigated by
  the type doc constraint and the non-blocking channel send.
- Warm tier has no admission policy — `warm.Put` does not enforce
  `maxBytes`. The sync loop could overfill warm. Mitigated by the
  batch cap and periodic compaction.

## Alternatives considered

- **`body_threshold=0`**: per-fill fsync. Rejected: crushes miss-path
  throughput, especially on network-attached storage.
- **Drain-on-shutdown**: captures working set at SIGTERM. Rejected:
  does not survive crashes or OOM kills.
- **Hot-tier snapshot/restore**: serialize hot tier to disk on
  shutdown, reload on startup. Rejected: complex, large I/O burst at
  shutdown, and the warm tier already provides the persistence layer.

## References

- Plan: `/tmp/warm-sync-plan.md`
- ADR-0014: Anti-entropy reconciliation for full cluster mode (superseded by ADR-0025)
- `internal/storage/tiered.go`
- `internal/storage/hot.go`
- `internal/storage/wal/wal.go`
- AGENTS.md §10 (ADR requirement for storage persistence model changes)
