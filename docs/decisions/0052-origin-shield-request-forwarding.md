# ADR-0052: Cluster origin shield via request forwarding

- **Status**: Accepted — implemented 2026-09-28; see
  `docs/plans/origin-shield.md` §8 for the as-built notes (original-method
  HEAD forwards, requester never backfills a HEAD answer, shared
  30 s forward deadline).
- **Date**: 2026-09-28
- **Deciders**: @chridupin-33
- **Phase**: cluster / origin
- **Related**: ADR-0007 (cluster design), ADR-0030 (128-bit key), ADR-0035 (peer wire over mTLS), ADR-0039 (pipelined peer fetch), issue #509 (owner-only partition)

## Context

In strong mode a cold key — mass purge, deploy, TTL expiry — costs one
origin fetch **per node**: every node misses locally, each non-owner
asks the owner via peer-fetch, gets a 404 (the owner has nothing
either), and every pod then fetches origin itself. For N nodes the
origin sees N identical requests for one URL. The consistent-hash ring
cannot prevent this because peer-fetch carries only a 128-bit key
(ADR-0030): the owner cannot be asked to fetch on the requester's
behalf, because a hash cannot be turned back into a request.

PR #731 was a first attempt: a coalesced peer-fetch that extended the
peer-fetch wire format (v3) with an `OriginRequest` envelope (method,
URI, host, allow-listed headers) so the owner could rebuild the
upstream request and drive a collapsed origin fetch. It worked, but
review exposed a structural cost: the envelope is a lossy
reconstruction of the client request, and every place the
reconstruction can diverge needed a special case. The PR's own "known
trade-offs" list grew one entry per divergence:

- origin-declared `Vary` outside the closed envelope degrades to
  origin fallback (hit-rate loss);
- HEAD→GET canonicalization on both sides to avoid storing an empty
  body under the shared GET key;
- objects above the peer-fetch cap are not streamed;
- request rewrites assumed already applied by the requester;
- a parallel origin-fetch lane (65 s detached flight, separate
  semaphore and client pool) built next to the foreground latch;
- a wire version negotiation (v2 receivers reject v3 with 400, waiters
  fall back per node during rolling deploys).

## Decision

We implement cluster origin shielding by **forwarding the original
client request to the owner**, not by reconstructing one from a
key-derived envelope:

1. On a hard miss, the non-owner runs today's peer-fetch first
   (v2 wire, key-only, sub-millisecond). A peer hit is served as
   today; nothing below runs.
2. On a 404 from the owner, the non-owner forwards the **original,
   un-rewritten client request** to a new peer endpoint on the owner
   (`/v1/peer/forward`, admin plane, mTLS, hop header).
3. The owner drives the request through its **standard data-plane
   miss path** (router → route handler → origin fetch). As ring owner
   it stores the fill; the response bytes are proxied back to the
   requester, which serves them to its client.
4. Any failure of the forward (transport, deadline, endpoint absent,
   ownership gate, hop limit) falls back to the non-owner's own
   origin fetch — availability is never worse than today.
5. Gated by `cluster.origin_shield` (strong mode, default off) —
   renamed from #731's `peer_fetch_coalesce` before first release
   ("coalesce" described the deleted v3 mechanism). A node with the
   flag off neither forwards nor serves forwards. Backfill
   (`origin_shield_backfill_probability`, default 1.0) applies to
   shield fills only: storing warm peer hits would re-create the
   every-pod-caches-hot-keys failure the owner-only partition
   (issue #509) exists to prevent.

## Consequences

### Positive

- The owner sees the real request path: Vary handling, HEAD semantics,
  request rewrites, streaming, size caps and negative caching are
  whatever the standard miss path already does — no envelope, no
  divergence, no special cases. The ~600 lines PR #731 built to fake a
  request out of a hash (wire v3, `OriginRequest`, `FetchOrigin`, lane
  management, HEAD canonicalization) are not needed.
- The peer-fetch wire format stays v2; there is no version
  negotiation. A node without the endpoint answers 404/501 and the
  requester falls back to origin — mixed-version fleets degrade per
  node during a rolling deploy, like the v3 proposal, but without
  wire-format coupling.
- One origin fetch for the whole cluster on a cold key (the feature's
  goal) is preserved.
- The owner's existing foreground request-collapsing latch
  automatically coalesces shield forwards with its own clients.

### Negative / trade-offs

- A cold miss costs one extra intra-cluster hop (the sub-ms key
  peer-fetch already paid today, plus one proxied request) and a
  duplicate store lookup on the owner. Both are noise next to an
  origin round-trip, and warm hits never pay them.
- The owner's fetch concurrency is now shared with shield traffic: a
  cold-key storm lands on the owner's standard fetch semaphore
  alongside its own clients. This is deliberate (fairness; no second
  lane to reason about) but it makes owner saturation the cluster's
  saturation — same bargain any designated shield makes.

### Risks

- **Double rewrite**: if the requester forwards an already-rewritten
  request, the owner's route rewrites apply twice. Mitigation: forward
  the original bytes, never a post-rewrite view.
- **Deadline nesting**: all nodes run identical config in strong
  mode, so the owner's fetch budget equals the requester's. The
  forward must carry a deadline the owner clamps (min) against its own
  fetch budget, or the requester can give up while the owner is still
  mid-fetch. Mirrors the existing requirement that the coalesced wait
  stay below `fetch_timeout`.
- **Forward loops**: ring disagreement can make the "owner" forward
  to a third node. Mitigation: the endpoint checks ring ownership
  (`IsLocal`-style gate) and decrements the hop header; a non-owner
  answers 404 and the requester falls back to origin.

## Alternatives considered

- **Envelope-based coalesced peer-fetch (PR #731, first attempt).**
  Fully implemented and reviewed; rejected because the envelope is a
  lossy request reconstruction and each divergence needed a special
  case (see Context). Every trade-off that PR accepted is absent in
  the forwarding design, at the cost of one extra intra-cluster hop
  on cold misses only.
- **Varnish-style designated shield tier.** A second cluster tier
  whose nodes receive full client requests by construction.
  Rejected: it requires new topology concepts, capacity planning and
  routing; bouine's consistent-hash owner already designates exactly
  the node we need.
- **Do nothing.** Cold-key storms cost N origin fetches for N nodes;
  acceptable at small N, wasteful at production scale — and the
  documented motivation for the feature.

## References

- PR #731 — first attempt (closed); its "Known trade-offs" section is
  the evidence for the envelope alternative's cost.
- `docs/plans/origin-shield.md` — implementation plan for this
  decision.
- Varnish origin shield (shield nodes receive the full client
  request; that property is what this design borrows).
