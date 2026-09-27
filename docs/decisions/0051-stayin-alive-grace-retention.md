# ADR-0051: Health-gated grace retention for stayin_alive routes

- **Status**: Accepted
- **Date**: 2026-09-27
- **Deciders**: @thylong
- **Phase**: phase 6 (hardening)

## Context

`stayin_alive` (config, ADR-0011) promises: serve the cached object
while the upstream is unreachable or 5xx-ing, "regardless of how long
ago it expired". The storage layer broke that promise:

1. The TTL reaper (`internal/storage/hot.go reapShard`) deletes every
   entry past `TTL + SWR + SIE` (default tick 30 s) and cascades the
   delete to the warm tier. After the freshness horizon plus one reaper
   tick, the stale copies the feature exists to serve are gone — the
   "indefinitely" in the route contract was false advertising.
2. SIEVE memory-pressure eviction is route-blind; for hot-only entries
   it is a true deletion, not a demotion.

Two facts reframed the fix:

- **The reaper is redundant for memory safety.** Hot-tier boundedness
  comes from SIEVE admission against `MaxBytes`; the warm tier is
  bounded by its own disk budget (it has no TTL reaper at all). The
  reaper is hygiene — freeing dead bytes early — so gating it degrades
  cost, never safety. Its implicit contract ("expired → a future miss
  can be refilled from origin") is simply false while the origin is
  down, for every route, not just stayin_alive ones.
- **Varnish's model.** In Varnish, TTL governs *serving*, never
  *retention*; objects live until LRU/nuker pressure removes them.
  bouine's reaper made a serving-policy-shaped (time-based) retention
  decision inside storage. This ADR moves that decision to where the
  policy lives (the wiring layer), on a signal that says whether
  deletion is currently safe (origin pool health).

A third defect was uncovered on the way: `PoolFastClient.doSingleFetch`
— the fetch path every cached route uses — did not record passive
health (only the proxy `FastHandler` path did), so the existing pool
ejection signal was dead on cached routes and could not serve as the
health gate.

## Decision

We implement health-gated grace retention ("P1b") plus eager warm
backing for graced fills ("P5"):

1. **The object carries the policy intent.** `api.Object` gains two
   additive JSON fields stamped at cache-fill time by `buildObject`
   (and restamped by `refreshFrom304`): `KeepGrace` (true when the
   route has `stayin_alive`) and `Pool` (the route's origin pool).
2. **The store implements a generic mechanism.** `HotConfig.MayReap`
   is a reap gate invoked by the TTL reaper only for *expired
   `KeepGrace`* entries. Returning false holds the entry for the next
   pass; nil keeps the historical behavior (reap everything) so an
   unwired store never pins memory. Capacity eviction (SIEVE, warm
   budget) is never gated — bounded resources always win.
3. **The wiring supplies origin health.** The builder installs
   `mayReapDecider(pools)`: a graced entry may be reaped only while
   its origin pool has at least one healthy target
   (`Pool.HasHealthyTarget`). Ungraced entries, unknown pools, and
   pool-less routes always reap.
4. **The health signal is made real on cached routes.**
   `PoolFastClient.doSingleFetch` records passive health (consecutive
   connection errors / 5xx eject, success resets, gated on
   `consecutive_5xx > 0`) exactly like the proxy `FastHandler` path.
5. **Graced fills are eagerly warm-backed.** `TieredStore.Put` writes
   `KeepGrace` objects to the warm tier (and `Protect`s them) regardless
   of `BodyThreshold`, so hot SIEVE pressure on them is a demotion and
   `TieredStore.Get` re-promotes.
6. **Observability.** A grace hold increments
   `bouine_hot_store_reaper_grace_holds_total` (via `Stats()` polling,
   like `hot_store_evictions_total`).

Healthy routes are unaffected: with a healthy pool the gate returns
true and the reaper removes expired graced entries on the normal
schedule — hygiene is fully preserved.

## Consequences

### Positive

- The stayin_alive route contract now holds for outages longer than
  `TTL + SWR + SIE`, including cold keys (entries nobody requested
  since the outage began).
- After the outage ends and the pool reports a healthy target, the
  next reaper pass collects everything on the normal schedule — no
  unbounded pin, no operator action.
- Passive ejection now also fail-fast protects cached routes (no more
  dialing a known-ejected origin per request once all targets are out).
- The warm tier turns hot memory pressure on graced entries into a
  recoverable demotion.

### Negative / trade-offs

- Graced fills on stayin_alive routes pay one eager warm write per fill
  (bounded to those routes' miss rate; miss path only).
- The grace guarantee requires a resolvable health signal: pools need
  `consecutive_5xx` (passive) and/or active health checks configured;
  pools without any health config keep the historical reap-during-outage
  behavior. `eject_for` is recommended so restores are automatic.
- Prolonged outages pin the route's pre-outage working set in memory
  and on warm disk until capacity pressure or recovery — accepted:
  that is exactly the memory the route already used before the outage,
  and SIEVE/the warm budget remain the final arbiters.

### Risks

- A pool stuck "unhealthy" (e.g. all targets ejected without
  `eject_for` and no active checks) holds graced entries indefinitely —
  visible as a sustained `reaper_grace_holds_total` rate with no
  ejections; operator action is MarkHealthy or enabling auto-restore.
- All-target ejection on a cached route changes miss-path behavior
  from "dial and fail after fetch_timeout" to "fail fast with
  'no healthy upstream'" — same client outcome (stayin_alive serves
  stale), strictly lower latency and origin load.

## Alternatives considered

- **Inflate the stored `stale-if-error` at fill time.** Rejected: lies
  in the object (wire, peers, soft-purge, dashboards all see a fake
  SIE), and pins the route's working set even while healthy.
- **Retention lease renewed by actual stale serves** (demand-
  proportional DHCP-style). Rejected as the primary mechanism: no
  protection for cold keys, which the route contract covers.
- **Outage-suspension of reaping (global).** Rejected alone: pool-level
  granularity pauses hygiene for healthy routes too; kept partially in
  spirit — the gate is per-object, driven by the object's own pool.
- **Demand-aware reaper via the SIEVE visited bit** ("reap only expired
  and unvisited"). Rejected: retains 502-garbage on broken
  non-stayin_alive routes under traffic; needs an arbitrary horizon cap.
- **Storage-level protect/pin flag on entries.** Rejected: storage
  learns routes; here only the *object* (pkg/api, additive) carries
  intent and storage stays route-agnostic.

## References

- ADR-0011 (per-route TTL features incl. stayin_alive), ADR-0013
  (buildObject/computeTTL choke points), ADR-0045 (shed/re-warm pair).
- `internal/storage/hot.go` (`reapShard`, `HotConfig.MayReap`),
  `internal/storage/tiered.go` (`Put`), `internal/cache/handler.go`
  (`buildObject`, `refreshFrom304`), `internal/origin/pool.go`
  (`HasHealthyTarget`, `doSingleFetch`), `cmd/bouine/cmd/builder.go`
  (`mayReapDecider`).
- `docs/runbook/54-origin-ejection.md` (health configuration),
  `docs/plans/stayin-alive-grace-retention.md` (implementation plan).
