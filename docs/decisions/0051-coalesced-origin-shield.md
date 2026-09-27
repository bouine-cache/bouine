# ADR-0051: Coalesced origin shield for strong-mode cold keys

- **Status**: Accepted
- **Date**: 2026-09-27
- **Deciders**: @chridupin-33
- **Phase**: phase 3
- **Consulted**: design review (PR #731)

## Context

In strong mode the fleet cache is partitioned by the consistent-hash
ring: only the owner of a key stores it (issue #509). When a mass
purge, a deploy, or a TTL expiry colds a hot key, every node that sees
traffic for that key fetches origin independently — a cold key costs N
origin requests for an N-node cluster, which is exactly the stampede
the cache exists to absorb. The plain peer-fetch path cannot help: on a
miss the owner answers 404 and every waiter fetches origin itself.

## Decision

We add cluster-coordinated origin shielding, off by default
(`cluster.peer_fetch_coalesce`, strong mode only):

- **One origin fetch per cold key, cluster-wide.** On a hard miss the
  non-owner sends the owner a coalesced peer-fetch carrying an
  `OriginRequest` envelope (wire v3: the v2 fixed body plus a flags
  byte and the envelope). The owner joins its route's foreground
  inflight latch — the same singleflight a local client miss uses — so
  peer waiters and local clients collapse into exactly one origin
  fetch, and the owner keeps its fill.
- **Closed envelope.** The envelope carries method, request URI, host,
  and only the headers that change the origin's answer:
  `Accept-Encoding`, `Authorization` (responses with credentials are
  cacheable under the shared-cache opt-in; replaying without the
  header would fetch and negatively cache a different answer), and
  every header the route's `cache.key.include_headers` policy
  consults. An origin-declared `Vary` outside this set degrades to the
  origin fallback via the variant-assertion gate — a hit-rate loss,
  never wrong-variant content.
- **Detached owner flight, dedicated lane.** Coalesced RPCs run on
  their own pipeline clients (origin-scale `ReadTimeout`) and their own
  bounded semaphore, so a slow origin can never queue behind or stall
  fast peer cache HITs. The flight is bounded at 65s and detached from
  any single waiter's connection: a departing waiter must not abort the
  fetch others depend on. Coalesced RPCs are exempt from the peer
  breaker (origin latency is not a peer health signal).
- **Waiter budget and fallback.** The waiter waits at most
  `api.CoalesceFetchTimeout` (30s), strictly below the origin fetch
  budget, so a waiter that gives up still completes its own origin
  fetch in time. Every failure mode — owner down, timeout, lane shed,
  variant-gate rejection — falls back to the waiter's own origin fetch;
  availability is never traded for the shield.
- **Backfill knob.** `cluster.peer_fetch_backfill_probability`
  (default 1.0; 0.0 = strict owner-only partition) controls whether
  waiters also store the coalesced object locally.
- **Ownership gate.** A coalesced fetch runs only on the ring owner
  (`Cluster.IsLocal`); a stale-ring node answers 404 and the waiter
  falls back, instead of doubling the fetch.
- **Single timeout definition.** The 30s budget lives once, in
  `pkg/api` (shared kernel), and both `internal/cache` and
  `internal/cluster` consume it — no hand-maintained copies.

Wire v2 receivers reject a v3 body at the version byte (400) and the
requester falls back to origin: a rolling deploy degrades per node with
no flag flips.

## Consequences

### Positive
- A cold key costs one origin request for the whole cluster instead of
  one per node — the deploy/purge stampede is gone.
- Degradation is per-node and boring: any failure falls back to
  today's behavior.

### Negative / trade-offs
- The fast path pays one extra owner-lookup RPC per miss while
  coalescing is on (the `OwnerMiss` hint is withheld so the slow path
  can still coalesce).
- Objects above the 64 MiB peer-fetch cap are not streamed; oversized
  coalesced answers fail into the origin fallback.
- Waiter-side client cancellation is not propagated into the coalesced
  RPC (a bare fasthttp `RequestCtx` has no `Done` channel); the wait is
  bounded by the 30s budget and the owner flight is detached anyway.
- HEAD requests are canonicalized to GET in the envelope, mirroring the
  key canonicalization; the waiter suppresses the body at write time.

### Risks
- A saturated coalesced lane sheds waiters back to origin, which under
  a miss storm reproduces the stampede for the shedding lane only.
  `bouine_coalesced_fetch_shed_total` makes this visible; the
  `role="fallback"` series measures the real fallback rate.
