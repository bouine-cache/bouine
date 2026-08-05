# ADR-0029: Dedicated tombstone drain goroutine

Date: 2026-08-04

## Status

Proposed

## Context

Under sustained eviction pressure (36k evictions in 125s = ~290/s), the
tombstone queue (4096-buffer) overflows, dropping WAL delete entries
(issue #221). While the dropped entries are not data loss (tombstones are
on disk and `rebuildIndexFromScan` honors them), crash recovery falls
back to the slower segment-scan path instead of the WAL fast-replay.

The root cause is a drain-rate mismatch: the queue is drained only once
per warm sync cycle (default 60s), but evictions arrive continuously at
~290/s. Between drain cycles, the 4096-buffer fills and overflows.

## Decision

Implement both fixes from the issue's "fix direction":

1. **Configurable queue size** (default 65536, up from 4096) — 16x larger
   buffer absorbs bursty eviction pressure. Configurable via
   `tombstone_queue_size` in the storage config.

2. **Dedicated drain goroutine** (default 1s interval) — decouples
   tombstone draining from the warm sync cycle. The drain goroutine
   flushes the tombstone and warm-evict queues to the warm tier + WAL
   independently of the 60s sync cycle. Configurable via
   `tombstone_drain_interval`. Set to -1 to disable (reverts to pre-fix
   behavior).

The warm sync cycle (`runWarmSyncCycle`) still drains the queues as a
fallback, ensuring backward compatibility when the dedicated goroutine
is disabled.

## Consequences

- **Positive**: Tombstone queue overflow is eliminated under sustained
  eviction. WAL fast-replay is preserved on crash recovery.
- **Positive**: Queue size and drain interval are operator-tunable
  without code changes.
- **Positive**: Backward compatible — disabling the drain goroutine
  reverts to the pre-fix behavior.
- **Negative**: One additional goroutine when warm tier is enabled and
  drain interval is positive. Negligible CPU overhead (one ticker + one
  drain per interval).
- **Negative**: Dropped-tombstone counter swaps are now split between
  the drain goroutine and the sync cycle. Both report independently;
  the sync cycle logs the remaining drops after the drain goroutine has
  already processed them.
