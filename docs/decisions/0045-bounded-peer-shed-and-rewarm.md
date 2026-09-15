# ADR-0045: Bounded peer-fetch shedding and the post-ban re-warm allowance

- **Status**: Accepted
- **Date**: 2026-09-15
- **Deciders**: @chris.dupin
- **Phase**: phase 6
- **Consulted**: -
- **Informed**: -

## Context

A 3-node miss-storm stress run (6k req/s, surrogate-key purge of 80% of a
360k keyspace at T+15m) exposed two compounding saturation behaviors:

1. **The fast-path peer branch wedges connections.** The h1 fast path
   serves misses inline on the connection goroutine
   (`TryHit` → `tryPeerFetch` → `PeerFetcher.Fetch`). `Fetch` acquires a
   `fetchSem` slot by selecting on the semaphore and the caller's context
   — but the fast path passes `context.Background()` (zero-alloc hit
   path). Under saturation, every keep-alive connection goroutine parks
   in the queue forever: 4,386 parked goroutines at end of run vs 225 on
   the slow-path arm. The slow path fixed the identical disease in
   issue #562 (`fetchWaitTimeout` → shed with `ErrFetchShed`); the peer
   semaphore predates that fix and inherited the same dead-cancellation
   shape.
2. **A shed foreground miss is a lost refill.** After the purge ban, the
   miss storm (~4,000 distinct keys/s) saturated the origin-fetch
   semaphore; every shed request fetched nothing and stored nothing
   (461k sheds measured). Re-warm stalled at the shed equilibrium and the
   hit ratio pinned at ~7% until the 24h `banTTL` expired the ban. A
   30-second purge cliff became a multi-hour hit-ratio cliff.

## Decision

Three changes, all bounded and local:

1. **Peer-fetch and peer-put semaphore waits are bounded** by a 100ms
   timer (`peerFetchWaitTimeout`, mirroring the cache handler's
   `defaultFetchWaitTimeout`). On expiry the RPC sheds with
   `ErrPeerFetchShed`; fast-path callers already fall through to the slow
   path's shed/origin machinery on any peer error. A shed is also
   observed in `peer_fetch_queue_wait_seconds` (previously only
   successful acquisitions were) and counted in the new
   `bouine_peer_fetch_shed_total`. The timer fires only under saturation:
   healthy peer RPC p50 is ~3ms.
2. **A shed foreground miss schedules a bounded background refill**
   (`triggerShedRefill` → `doShedRefill`): the client keeps its
   503 + Retry-After / stale protection, but the store re-warms. The
   refill pool (`rewarmSem`, 32 per handler, `defaultRewarmConcurrency`)
   is deliberately separate from the foreground `fetchSem` it was just
   shed from — sharing that budget would keep the re-warm starved by the
   very demand it recovers from. Refills are singleflight-collapsed with
   foreground fetches (same `singleflight.Group`), tracked by `revalWg`
   for shutdown draining, and gated on cacheability exactly like the
   buffered miss path. Metric: `bouine_rewarm_fill_total` next to
   `bouine_fetch_shed_total`.
3. **The lazy-ban list TTL is configurable** (`cluster.ban_ttl`,
   `HotConfig.BanTTL`, default 24h unchanged). RFC 9111 §4.4 exempts
   objects stored after the ban, so the TTL only bounds how long pre-ban
   copies keep being rejected — and the reaper + TTL expiry reclaim
   those regardless. Cache-lifecycle surrogate invalidations are safe at
   minutes scale; the knob bounds the hit-ratio damage of an over-broad
   ban (a typo currently poisons the hit ratio for the full window).
   Validation: `>= 0` (0 = default), `>= 1s` when set.

## Consequences

### Positive
- Connection-goroutine occupancy under peer saturation is bounded by
  construction (wait ≤ 100ms), for every caller of `Fetch`/`Put`, not
  just the fast path.
- A purge-induced miss storm converges back to hits instead of pinning:
  each shed is no longer a lost refill. Recovery slope is bounded by the
  refill pool size, not by `banTTL`.
- Operators can bound the blast radius of an over-broad surrogate ban
  from 24h to minutes (`cluster.ban_ttl`).
- Shed volume at the peer layer is visible (`peer_fetch_shed_total`).

### Negative / trade-offs
- The re-warm allowance adds bounded extra origin load precisely during
  storms (32 concurrent GETs per handler). This is the point: the load
  already exists in the foreground and was being rejected unproductively.
- Refills that land after the client gave up spend origin capacity on
  objects nobody is waiting for anymore. Singleflight collapse plus the
  cacheability gate keep the waste bounded; the re-warm benefit
  dominates for hot keyspaces.
- One more bounded goroutine pool per handler (32) to reason about at
  shutdown. Tracked by `revalWg` and drained by `Close` like SWR.

### Risks
- A misconfigured tiny `ban_ttl` (< object TTL horizon) could allow
  pre-ban copies to be served again after expiry of the ban list entry.
  Mitigated by the 1s validation floor and the reaper reclaiming the
  objects themselves; RFC 9111 §4.4 does not require the ban predicate
  to persist, only that stored-after-ban copies are exempt.
- Shed-refill and foreground fetch share the singleflight key; a refill
  leader re-runs the fetch unslotted after a foreground leader shed. A
  pathological origin could then serve refills while foregrounds keep
  shedding — bounded by `defaultRewarmConcurrency` (32) and invisible to
  clients.

## Alternatives considered

- **Wrap the fast-path peer closure in `context.WithTimeout`** (the
  stress report's primary proposal): fixes only the fast-path call site
  and allocates per request; the unbounded select remains for every
  other caller. Rejected in favor of the structural fix in
  `acquireFetchSlot`.
- **Serve-and-fill on every shed without a dedicated pool** (refill on
  the shared `fetchSem`): re-introduces the starvation — the refill
  competes with the demand that caused the shed. Rejected.
- **Shorten the ban TTL unconditionally** (minutes for every ban class):
  changes semantics for host/path predicate bans where operators may
  legitimately rely on the long window. Rejected in favor of a knob with
  a conservative default.
- **Admission-control re-warm queue with priority ordering** (LRU of
  pending shed keys): strictly better refill selection, but adds a
  shared queue, ordering logic, and a new shutdown owner. Deferred until
  the singleflight-deduped pool proves insufficient in a re-run.

## References

- Stress evidence: `bouine-stress-test/results/issue-fastpath-peer-branch-wedge.md`
  (`peerpath-pr645-30m-3n-6k-pfc16-cpu4-{on,off}`).
- `AGENTS.md` §7 (hit-path budget), §11 (goroutine ownership), §12
  (error mapping).
- ADR-0039 (peer pipelining), issue #562 (origin fetch shed), issue #509
  (write-to-owner), issue #133 (peer-fetch concurrency bound).
