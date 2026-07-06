# ADR-0022: Refresh persist cycles

- **Status**: Proposed
- **Date**: 2026-07-07
- **Deciders**: bot-group-staffeng

## Context and Problem Statement

`refresh_min_hits` (ADR-0021) gates re-scheduling after a background
refresh: objects that accumulated fewer than N hits during their TTL
window are not re-scheduled and expire naturally. This filters long-tail
content on routes like `/product-page/` (100k products, ~165 req/s,
1-minute TTL).

The problem is that the average inter-access time for a product is
~10 minutes, while the TTL is 1 minute. An object that was accessed at
t=0 gets one refresh cycle at t=54s. If nobody accesses it during
[0, 54s], the gate blocks re-scheduling and the object expires at t=70s.
When the product is accessed again at t=300s, it's a full cache MISS.

With `refresh_min_hits: 1`, only objects accessed during their 54s
window stay fresh. For the product-page route, this is ~10% of the
catalog — the other 90% expire and re-miss on next access, yielding a
14% hit ratio.

Increasing the TTL to 10 minutes would fix the hit ratio but risks
serving stale content to downstream CDN caches for too long without
origin validation. The route owner rejected this approach.

## Decision

Add a `refresh_persist_cycles` config field per route (int, default 0).
When > 0, the popularity gate does not immediately kill re-scheduling.
Instead, the object continues to be refreshed for up to N additional
TTL cycles. Each background refresh that finds `Hits < minHits`
decrements the persist counter. A popular refresh (`Hits >= minHits`)
resets the counter to the configured value.

### How it works

- `refreshEntry` gains a `persistCycles int` field, initialized to the
  configured value on every `Register` call.
- `refreshRegistry.DecrementPersist(key)` atomically decrements the
  counter under the registry mutex. Returns `true` if the counter was
  > 0, `false` otherwise.
- In `storeAndReplicate`, when the popularity gate would block
  (`isRefresh && Hits < minHits`):
  1. If `refreshPersistCycles > 0` and `DecrementPersist(key)` returns
     `true`: re-schedule the refresh without re-registering. The entry
     keeps its current request info; only the persist counter changed.
  2. Otherwise: unregister and let the object expire (current behavior).

- On a popular refresh (`Hits >= minHits`), `Register` is called as
  before, which resets `persistCycles` to the configured value.

### Behaviour with `refresh_min_hits: 1, refresh_persist_cycles: 9`

1. Object cached (MISS) at t=0 → scheduled for refresh at 54s,
   persist=9.
2. No client accesses the object → Hits stays 0.
3. Background refresh at 54s → 304 → refreshed.Hits=0 < minHits=1.
   Gate would block, but persist=9 > 0 → decrement to 8, re-schedule
   at 108s.
4. Steps 3-4 repeat for 10 total cycles (~10 minutes).
5. If a client accesses the object at any point → Hits=1 ≥ minHits=1
   → popular refresh → persist reset to 9, stays alive indefinitely.
6. If never re-accessed → persist=0 at t=540s → gate blocks, object
   expires naturally.

### Content freshness guarantee

The origin is consulted every 54s via conditional GET. If the origin
returns 200 (content changed), the object is replaced immediately.
If it returns 304, the object is confirmed fresh. Downstream caches
see correct `Age` and `Cache-Control` headers. No stale content is
served — the TTL remains 1 minute, and the object is always
origin-validated within one TTL cycle.

## Considered Options

1. **Increase `ttl_override`** — longer TTL means fewer origin
   requests but risks serving stale content to downstream caches.
   Rejected by the route owner: stale content risk is unacceptable
   for product pages that downstream CDNs may cache.

2. **Set `refresh_min_hits: 0`** (disable the gate) — every cached
   object gets refreshed. With 100k objects and 1-minute TTL, that's
   ~1,850 conditional GETs/s. Viable but generates significant origin
   traffic for objects that may never be re-accessed. Persist cycles
   provide a bounded middle ground.

3. **Increase `stale_while_revalidate`** — extends the SWR window so
   expired objects can be served stale while revalidating. Requires
   `Warning: 110` header compliance from downstream caches. Rejected
   as a standalone solution because it depends on downstream behavior.

4. **Track last-access time instead of hit count** — would require
   adding a `lastAccess` field to `api.Object` and updating it on
   every Get. More memory, more complexity. `persistCycles` achieves
   the same goal (bridge the gap between short TTL and long inter-access
   times) without new hot-path state.

## Consequences

- **Positive**: Objects survive for N×TTL after last access without
  increasing the TTL or serving stale content. Hit ratio improves
  proportionally to the persist budget.
- **Positive**: Zero hot-path overhead — the persist counter is only
  touched on the background refresh path (not on cache hits).
- **Positive**: Opt-in (default 0 = current behaviour). No changes to
  existing deployments.
- **Positive**: Origin is consulted every TTL cycle, so content
  freshness is guaranteed. A 200 response replaces the object
  immediately.
- **Negative**: Each persist cycle generates one conditional GET per
  object. With `persist_cycles: 9` and 100k cached objects, worst case
  is ~1,850 additional conditional GETs/s (all 304s if origin supports
  ETag/Last-Modified). The `refresh_concurrency` semaphore bounds
  concurrent refreshes.
- **Negative**: The refresh registry holds entries for up to N×TTL
  after last access. With 100k entries at ~300 bytes each, this is
  ~30 MB — bounded and acceptable.
