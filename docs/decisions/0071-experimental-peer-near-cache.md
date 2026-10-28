# ADR-0071: Experimental peer near-cache on non-owner nodes

- **Status**: Accepted (experimental, `experimental.near_cache` flag, default off)
- **Date**: 2026-10-28
- **Deciders**: @thylong
- **Phase**: performance (topology finding F2 / OQ1)
- **See also**: ADR-0044 (invalidation broadcast), ADR-0066 (fill
  flights and the unstorable memo — the admission-memo pattern),
  ADR-0069 (H1 fast path — the serving surface), issue #509
  (owner-only storage in strong mode — the invariant this feature
  narrows without breaking), `internal/cache/nearcache.go`,
  `notes/Bouine/topology-perf-audit.md` §3.1 (F2), §8

## Context

On a 3-node strong cluster, 64% of steady-state requests are `HIT/peer`
(measured, topology harness): a request lands on a node that does not
own the hot key and pays a full peer RPC — owner-side encode
(~15 µs + a fresh ~172 KiB allocation for a 165 KiB SSR body, audit
F21), wire transfer, requester-side decode and body copy (~11 µs +
168 KiB, audit F1), plus the admin-plane round trip — for bytes that
are identical on every call. On a real cluster each avoided RPC also
saves one intra-cluster RTT and the owner's serving goroutine.

Every invalidation surface already reaches every node: purges,
refreshes (soft purges) and bans broadcast to the whole fleet and
apply locally (ADR-0044; `engine.go` peer apply paths with issuer+seq
dedup), and bans evaluate inside the store's `Get` against the
`X-Bouine-Host`/`X-Bouine-Path` headers every stored object carries.
A local copy of peer-fetched bytes can therefore be invalidated with
exactly the machinery the owner's copy uses — no new propagation path.

OQ1 asked whether a near-cache is acceptable at all, given the
cross-client leakage risk a miss would create.

## Decision

Add an opt-in, bounded, node-local near-cache of peer-fetched objects
on non-owner nodes, gated behind `experimental.near_cache.enabled`
(default off). One shared `cache.NearCache` per engine (a dedicated
`storage.HotStore` instance — SIEVE eviction, byte-budgeted,
ban-evaluating — plus a popularity memo of the unstorable-memo shape).
Every route Handler, fast path and invalidation surface shares the
instance.

Safety model (fail-closed at every doubt):

1. **Admission guards**: only non-variant (blank `VaryKey` AND blank
   `VaryValue` — variant bodies and the ADR-0068 metadata resolver are
   refused outright), bodyful, still-fresh objects. The stored copy's
   TTL is bounded by BOTH the object's remaining freshness and
   `max_ttl` (default 30 s, capped at 5 m), and its grace windows are
   zeroed — a near entry never serves stale and never triggers
   revalidation (revalidation stays the owner's job, issue #509).
2. **Popularity gate**: a key peer-fetched twice within 30 s admits on
   the second fetch; first sight only arms the gate. One-pass traffic
   never churns the bounded store.
3. **Fresh-only serving**: `Lookup` refuses expired entries (and
   reclaims them); the serve path runs the same freshness evaluation a
   local hit runs, and a non-`Hit` decision falls through to the
   ordinary peer ask.
4. **Ordering**: the unstorable-fill memo (ADR-0066 am #2, finding
   F12) fires BEFORE the near lookup, so an origin that just settled
   `no-store` is not answered from a near copy inside its window.
5. **Invalidation parity**: `Handler.Purge` and `SoftPurge` delete the
   near copy (a refresh is a hard delete — near entries carry no grace
   to mark stale); the engine applies `Ban` expressions to the near
   store next to the main store, so bans evaluate identically on both.
6. **Bounded blast radius**: `max_bytes` per node (default 16 MiB,
   capped at 256 MiB), fresh-only serving, `max_ttl` as the staleness
   bound that holds even without invalidation propagation, all
   validation-capped; three counters (`bouine_near_cache_{hits,
   admissions,invalidations}_total`) make the feature observable.

The bytes a near hit serves are exactly the bytes the peer RPC would
have returned, to the same request population (requests that computed
the same key and passed the same gates), within the same freshness
window. The near cache changes WHERE the answer is read from, not WHAT
is answerable.

## Consequences

Measured on the topology harness (3-node strong cluster, hit-heavy
1000/s, Zipf s=1.1 over 2000 keys, near store 64 MiB/30 s per node,
interleaved A/B, 3 runs per side, darwin/arm64 co-located):

- `HIT/near` serves **54–55% of requests**; `HIT/peer` drops
  64% → 9.6–9.9% (−85%): the Zipf head leaves the RPC path entirely.
- Near-hit p50 ≈ local-hot-hit p50 (245–248 µs vs 241–244 µs),
  **−40% vs the peer-hit p50 it replaces** (412–449 µs). On a real
  network each avoided RPC additionally saves one cluster RTT.
- Whole-process mallocs/req **35.2 → 13.9 (−60%)**; alloc bytes/req
  **261 → 97 KiB (−63%)**; GC rate **0.67 → 0.20 /s (−70%)**; CPU
  **447–485 → 402–415 µs/req (−10–15%)** — the peer-hit path's two
  body-sized allocations per request (F1 + F21) simply stop happening
  for admitted keys.
- Owner-side peer-fetch serving: **9,646 → 1,477 RPCs per 15 s window
  (−85%)**; origin fetches unchanged (misses are not affected);
  invalidation-driven peer misses unchanged (~115/window).
- Cost: +~50 MB RSS per node at the measured 64 MiB bound (three
  stores, SIEVE-evicted); admission costs two body copies per key per
  freshness window (clone-for-storage + the store's ownership clone).

Risks accepted:

- **Invalidation propagation latency**: a near copy can serve within
  its freshness window while a purge/ban is still in flight — the same
  staleness every coalesced invalidation already accepts, now also
  bounded by `max_ttl`. Broadcast drops or a dead gossip link degrade
  to `max_ttl`-bounded staleness, never worse.
- **Policied multi-route setups**: near entries are keyed by the ask
  key (the route's policy key); invalidations built under a different
  route's policy miss them. Fail-open direction: the near entry
  expires at `max_ttl`. Listed as an open question below.
- **Variant objects are not admitted** (v1): Vary routes keep paying
  peer RPCs — including the ADR-0068 two-step. Deliberate: admitting
  variants multiplies the invalidation surface before the fleet has
  field data on the simple case.

Open questions (also listed in the PR):

1. Should variants be admitted under a `(key, varyKey)` near index
   once the non-variant behavior is validated in the field?
2. Should the popularity gate, admission window and per-key TTL be
   promoted to config knobs with operator demand?
3. Multi-route policied invalidation key-space coherence (see risks).
4. Co-located harness absolute numbers (hot-hit p50 shifted +~60 µs
   with near ON across interleaved runs; GC and CPU improved) —
   re-measure on the pinned Linux runner before trusting absolutes.
5. Unifying `ttlKeyMemo` with `unstorableMemo` (duplicated mechanism,
   kept separate to avoid churning a tested file in the feature PR).
