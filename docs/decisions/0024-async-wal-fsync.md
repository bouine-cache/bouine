# ADR-0024: Async WAL fsync for miss-path throughput

- **Status**: Proposed
- **Date**: 2026-07-07
- **Deciders**: @thylong
- **Phase**: phase 4.5 (hardening)
- **Consulted**: (none)
- **Informed**: (none)

## Context

`wal.Append` holds a mutex across `f.Sync()`, serializing all warm-tier
writes through one disk barrier. Under miss-heavy load (2000 req/s, 10 KiB
objects), 69% of goroutines (387 of 560) block on the WAL mutex
(`l.mu.Lock` → `l.f.Sync()`), capping miss-path throughput at ~260 req/s
versus 1046 req/s for hits. p50 miss latency is 742 ms, p99 is 11 s —
dominated by WAL wait (issue #220).

The root cause is structural: every `tiered.Put` that writes to warm calls
`wal.Append`, which holds `l.mu` across `f.Sync()`. No batching, no async
fsync, no group commit. The lock-held-across-fsync pattern serializes all
writers regardless of how fast the disk is.

## Decision

Move WAL fsync to a background goroutine (`walSyncLoop`). Callers enqueue
entries to a bounded channel (4096, drop-on-full); the sync loop batches
and fsyncs on a timer (100 ms default). `rebuildIndexFromScan` is the
durability backstop — the WAL is a fast-replay optimization, not a
durability guarantee.

### Wakeup mechanism

The sync loop uses `select` on three signals:

- `tick.C` — a `time.Ticker` firing every `syncInterval` (100 ms default).
  Normal batching trigger: drain all entries since the last tick, write +
  fsync once.
- `flushCh` — a `chan struct{}` (buffered=1) that `Sync()` sends to.
  Triggers an immediate drain + fsync without waiting for the next tick.
  Prevents 100 ms latency penalty for shutdown or explicit flush.
- `stopCh` — a `chan struct{}` closed by `Close()`. The loop drains
  remaining entries, fsyncs, then exits.

### Open vs OpenAsync

`wal.Open` stays synchronous (no sync loop) for tests, `rewriteWAL` tmp
files, and any caller that wants per-call durability. `wal.OpenAsync`
starts the `walSyncLoop`. `TieredStore` uses `OpenAsync` for the main WAL;
`rewriteWAL` uses `Open` for the tmp log (synchronous `AppendBatch` is fine
there — no contention, fresh file) and `OpenAsync` for the reopen after
rename.

`Enqueue` on a sync-only `Open` log (no sync loop, `syncCh` is nil) falls
back to synchronous `Append`. This prevents silent data loss when tests or
`rewriteWAL`-reopened logs receive `Enqueue` calls.

`syncInterval = -1` means "synchronous mode" — `Enqueue` falls back to
`Append` (same as `Open`). This lets operators force per-entry durability
on low-traffic deployments where recovery speed matters more than
throughput.

### walMu stays on the Enqueue path

`walMu` guards `t.wal` field access during `rewriteWAL` (which swaps
`t.wal` under `walMu`). It is uncontended when not in a compaction — a
read-mostly lock. The problem was never `walMu`; it was `l.mu` held across
`f.Sync()`. So `Enqueue` takes `walMu` to safely read `t.wal`, then calls
`t.wal.Enqueue` (channel send, no lock). `walMu` hold time is
nanoseconds — no serialization.

### Segment sync stays synchronous

The current code calls `warm.SyncSegment(segID)` before `wal.Append`. This
stays. `SyncSegment` is per-segment (not a global lock), and it ensures the
segment data is durable before the WAL pointer to it. Moving segment sync
to background is a separate, future change.

## Consequences

### Positive

- Miss-path throughput: ~4× improvement (260 → ~1000+ req/s expected).
- Goroutine count: 387 blocked → <50 blocked (channel sends are ns).
- Natural group commit: all entries within one sync interval batched.

### Negative / trade-offs

- Up to `syncInterval` (100 ms default) of WAL entries lost on crash.
  `rebuildIndexFromScan` handles this — scans segments, honors tombstones.
- `Delete` no longer surfaces WAL errors (entry may be dropped).
  `rebuildIndexFromScan` is the backstop.
- Dropped WAL entries under sustained overload (metric:
  `bouine_wal_dropped_entries_total`). Recovery falls back to
  `rebuildIndexFromScan`, which is slower but correct.
- `rewriteWAL` + async lifecycle: `rewriteWAL` holds `walMu` for the
  entire duration. `Close()` on the old `OpenAsync` log blocks until the
  sync loop drains + fsyncs + exits — this happens under `walMu`, so
  `Enqueue` callers wait. Under load this could stall the miss path for one
  final fsync (~1-10 ms). Acceptable: compaction runs every 30 minutes.

## Alternatives considered

- **Group commit (batch fsync under lock)**: reduces fsync count but
  still serializes goroutines waiting for the batch. Rejected: doesn't
  eliminate the lock-held-across-fsync problem.
- **Per-shard WAL**: multiple WAL files, shard-by-hash routing. Rejected:
  high complexity, replay complexity, compaction complexity. The async
  approach eliminates the serialization with far less code.
- **No WAL (nginx approach)**: rely solely on segment scan on restart.
  Rejected: WAL provides fast replay (seconds vs minutes for full scan
  at millions of entries). Async fsync keeps the fast replay while
  removing the serialization.

## References

- Issue: #220
- ADR-0020: Hot→Warm background sync (same WAL, same durability model)
- `internal/storage/wal/wal.go`
- `internal/storage/tiered.go`
- AGENTS.md §10 (ADR requirement for storage persistence model changes)
