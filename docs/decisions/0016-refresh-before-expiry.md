# ADR-0016: Refresh-Before-Expiry Per Route

**Date:** 2026-07-01

## Status

Proposed

## Context

Operators need a way to keep high-value routes perpetually fresh with
minimal origin traffic. SWR handles TTL expiry reactively (client
request triggers revalidation). The cache key is an irreversible
xxhash64, so the scheduler needs a handler-side registry to
reconstruct origin requests for background refresh.

## Decision

Per-route `refresh_before_expiry` opt-in. A handler-side min-heap
scheduler fires at `TTL - margin`. Conditional revalidation via the
existing `collapsedFetch` path. A refresh registry stores Vary-relevant
request headers only (~200-450 B/entry). Lazy cancellation via
`store.Get` on pop — no cancel map, no generation counters. Periodic
compaction (every 60s) bounds dead entries.

The scheduler is per-Handler (not shared across routes) so hot reload
can stop and GC the old scheduler cleanly. `storeAndReplicate` gains
an `*http.Request` parameter to enable registration at the single
chokepoint for all cacheable stores.

## Alternatives Considered

1. **Per-object `time.AfterFunc`** — rejected: one goroutine per
   object, unacceptable for 1M objects.
2. **Scanner (like reaper)** — rejected: O(n) scan per interval, coarse
   granularity, wastes CPU scanning non-refresh entries.
3. **Store the URL in `api.Object`** — rejected: changes public API,
   increases per-object memory for all objects, serializes to warm tier.
4. **Integrate with reaper** — rejected: 30s granularity too coarse for
   short TTLs. Mixing reaper (delete expired) and refresh (refresh
   before expiry) in one scan complicates lock discipline.

## Consequences

- New per-route memory cost: ~232-482 B per scheduled object (heap +
  registry).
- One drainer goroutine per handler with refresh enabled.
- Up to `refreshConcurrency` (8) refresh goroutines per route.
- `Handler` gains `Close(ctx) error` and a `done` channel.
- `storeAndReplicate` signature changes to include `*http.Request`.
- New config fields (4) with validation.
- Zero storage-layer changes.
- Zero hit-path impact.
