# ADR-0021: Refresh popularity gate

- **Status**: Proposed
- **Date**: 2026-07-07
- **Deciders**: bot-group-staffeng

## Context and Problem Statement

`refresh_before_expiry` (ADR-0016) schedules a background conditional
revalidation for every cached object on enabled routes, regardless of
whether anyone is accessing the object. For routes with many distinct
long-tail paths (e.g. `/product-page/` with 100k products, only 5k
accessed per minute), this generates massive upstream traffic for
objects nobody is looking at.

The refresh scheduler min-heap grows unbounded with the number of cached
objects. Each entry fires a conditional GET to the origin at `TTL -
margin`. For a 30s TTL with 100k cached objects, that's ~4,167
origin requests per second — most of which are for objects that will
never be requested again.

## Decision

Add a `refresh_min_hits` config field per route (int, default 0). When
> 0, an object must have accumulated at least N cache hits during its
TTL window to qualify for re-scheduling after a background refresh.

### How it works

- `api.Object.Hits` is already incremented on the first cache hit after
  store (slow path in `hot.go:Get`, when the SIEVE visited bit flips
  from false to true). This is a single `Hits++` under the shard lock —
  no atomic, no allocation, no hot-path overhead.
- On 304 refresh: `refreshFrom304` copies `*stale`, preserving `Hits`.
  An explicit `refreshed.Hits = stale.Hits` documents the intent.
- On 200 refresh: `doBackgroundRefresh` sets `obj.Hits = stale.Hits`
  after `buildObject` to carry over the hit count.
- In `storeAndReplicate`, the scheduling block checks:
  `if isRefresh && h.refreshMinHits > 0 && obj.Hits < minHits → skip`
- `isRefresh` is a new `bool` parameter to `storeAndReplicate`. It is
  `true` only for calls from `doBackgroundRefresh` (background refresh
  re-store) and `false` for all foreground stores. This ensures every
  object gets at least one refresh cycle — the gate only applies on
  re-scheduling.

### Behaviour with `refresh_min_hits: 1`

1. Object cached (MISS) → scheduled for first refresh (foreground store,
   `isRefresh=false`, gate bypassed).
2. No client accesses the object during its TTL window → `Hits` stays 0.
3. Background refresh fires at `TTL - margin` → 304 → `refreshed.Hits = 0`.
4. `storeAndReplicate(isRefresh=true)` → `0 < 1` → gate blocks → not
   re-scheduled. Object expires naturally.
5. If a client accesses the object before expiry → `Hits = 1`. Next
   refresh → `1 >= 1` → re-scheduled. Object stays perpetually fresh.

This naturally filters long-tail content: only objects that are
actually accessed during their TTL window are kept fresh.

## Considered Options

1. **`refresh_min_hit_rate` (hits per second)** — more robust across
   TTL lengths but requires dividing by TTL and is harder to reason
   about. Rejected: the operator already knows their TTL, and the
   config is per-route.

2. **Track last-access time, gate on recency** — would require adding
   a `lastAccess` field to `api.Object` or `hotEntry` and updating it
   atomically on every Get. More memory, more complexity. Rejected:
   `Hits` already exists and is sufficient.

3. **LRU-based refresh eviction** — remove the oldest entries from the
   refresh registry when it exceeds a cap. Rejected: doesn't solve the
   problem (all entries are equally recent after a cache warm-up).

## Consequences

- **Positive**: Long-tail objects expire naturally instead of generating
  perpetual origin requests. Origin traffic drops proportionally to the
  popularity distribution.
- **Positive**: Zero hot-path overhead — `Hits` is already incremented.
- **Positive**: Opt-in (default 0 = current behaviour). No changes to
  existing deployments.
- **Positive**: `storeAndReplicate` gains an explicit `isRefresh`
  parameter that documents whether the store is a foreground initial
  store or a background refresh re-store.
- **Negative**: `storeAndReplicate` signature changes (9 call sites
  updated).
- **Negative**: `Hits` only increments once per TTL window (on the first
  access after store, when the SIEVE visited bit flips). So
  `refresh_min_hits` effectively acts as a boolean "was this object
  accessed at all" gate when set to 1. Higher values accumulate across
  TTL windows (after each refresh, the carried-over Hits persists and
  increments on the next first-access).
