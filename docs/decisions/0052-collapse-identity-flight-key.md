# ADR-0052: Never collapse requests carrying Authorization

- **Status**: Accepted
- **Date**: 2026-09-30
- **Deciders**: @bouine-core
- **Phase**: cache key policy
- **References**: RFC 9111 §3.5, §5.2.2.1; threat-model T06, T07; ADR-0046 (include_headers union); `internal/cache/collapse.go`

## Context and Problem Statement

Request collapsing (the singleflight dedup behind `collapsedFetch`,
`collapsedFetchBg`, `collapsedRevalidateBg`, the `inflightStreams`
table in `fetchAndStore`, and the shed-refill) parks every concurrent
request for the same cache key on one leader's in-flight origin fetch
and hands the leader's response to all of them.

Storage of the same response is gated: `isCacheBlocked` refuses to
store a response to an `Authorization`-bearing request unless the
response is explicitly shareable (`public`, `must-revalidate`, or
`s-maxage`), per RFC 9111 §3.5. **Collapsing bypasses that gate**: it
is not storage — no entry is written, the follower attaches to a live
response stream — so nothing stopped an authorized request from
receiving a *different* caller's authorized response.

This was not hypothetical. In production, a backend
service issued one request per tenant with a byte-identical
URI+Host and a shared service JWT in `Authorization`, differing only
in a tenant-selecting custom header. The origin
selects the tenant from that header; the cache key did not include it.
Concurrent per-tenant requests collapsed onto one leader's fetch, and
every follower received the leader's tenant's data — a cross-tenant
body swap. Storage never happened (pass-through route, `ttl_default:
0s`, and the §3.5 gate would have blocked it anyway); the leak was
entirely in-flight.

The threat model's posture ("headers participate in the cache key only
via Vary or an explicit per-route allow-list — never implicitly", T06/
T07) is enforced for *storage* keying but had no counterpart on the
*flight* key: the flight was keyed by the cache key alone.

## Decision

**A request carrying `Authorization` never shares a collapsing flight
with any other request.** `collapseDenied(ri)` is true exactly when
the request has an `Authorization` header; every flight site
(`fetchAndStore`'s inflight table, stayin-alive `collapsedFetch`,
`collapsedRevalidateBg`, background `collapsedFetchBg`, and
`doShedRefill`) fetches outside the shared flight when the gate trips.
Anonymous requests are untouched: their flight keys, collapse behavior,
and allocation profile are bit-for-bit identical to before.

The rule is deliberately stricter than identity-equality: it does not
attempt to prove two callers interchangeable by comparing credentials.

1. **"Same Authorization string ⇒ interchangeable" is unverifiable.**
   The production incident's shape was *identical* `Authorization`
   (one service-scoped JWT) with per-tenant callers selected by an
   undeclared custom header. A credential-equality partition would
   have merged those flights and re-leaked. Any equality scheme
   (Authorization alone, Authorization ∪ include_headers, ∪ more) is
   one undeclared selector away from the same bug class.
2. **The conservative side is cheap enough.** Splitting authorized
   flights costs one origin fetch per concurrent authorized caller —
   exactly what the origin would see if collapsing did not exist, and
   bounded by the fetch semaphore and shed machinery (issue #562).
   Collapsing is an optimization, not a correctness feature; when in
   doubt it must yield.
3. **It mirrors the storage doctrine at the right granularity.**
   Storage does not try to decide which *authorized* responses are
   safe to share by inspecting credentials — it refuses to store them
   unless the origin explicitly opts in (`public`/`s-maxage`). The
   flight gate now refuses to share them, full stop. There is no
   in-flight equivalent of the origin's explicit opt-in: the leader's
   response headers are unknown when a follower parks, and "the response
   turned out shareable" cannot retroactively un-share bytes already
   delivered.

Scope: `include_headers` are deliberately **not** part of the gate.
They already govern storage variants (ADR-0046); declaring one does not
make an authorized response shareable in-flight, and anonymous callers
on include_headers routes keep collapsing unchanged (pinned by test).
`Cookie` likewise stays out: storage does not key on it either (the
Set-Cookie storage gate is the control). Vary-declared dimensions are
not re-hashed: when a stored object exists they already participate via
the lookup key, and a first fill has no Vary to resolve.

## Consequences

### Positive
- The in-flight path is unconditionally leak-free for authorized
  traffic. No configuration, no operator declaration, no credential
  comparison can re-enable a cross-caller handoff.
- Anonymous collapse is unchanged — zero behavioral or performance cost
  for public routes (one `header.Map.Get` on the miss path, which
  `requestInfoFromCtx` already materialized).
- Trivially auditable: one predicate, applied at every flight site.

### Negative / trade-offs
- Same-credential fan-outs (one service account hammering one resource)
  no longer dedup on the miss path. Each concurrent authorized caller
  performs its own origin fetch. Mitigations: the fetch semaphore
  bounds concurrency and sheds excess (503 + Retry-After, or stale);
  authorized responses that the origin marks shareable are still
  served from *storage* on subsequent requests once cached (the §3.5
  gate permits it), so repeated authorized access converges to HITs.
- Per-user-auth traffic (API gateways with `Authorization` on every
  request) loses collapsing entirely. That is correct: those
  responses were never provably shareable, and Varnish deployments
  commonly accept the same posture for authorized traffic.
- A slight origin load increase during authorized burst windows.
  `fetch_shed_total` and origin request metrics surface it; runbook 55
  documents the expected shapes.

### Risks
- A stored authorized object (origin sent `public, s-maxage`) can still
  serve *another* authorized caller from storage on a HIT. That is the
  origin's explicit declaration, out of scope here — the flight gate
  covers only the in-flight window.
- Mixed-config clusters are unaffected: the gate reads only the
  request, not the route config, so nodes cannot disagree on it.

## Alternatives considered

- **Identity-equality partition (hash `Authorization`, optionally ∪
  `include_headers` values, into the flight key).** Preserves
  same-credential collapsing, and an earlier draft of this ADR chose
  it. Rejected for reason 1 above: the incident's identical-JWT shape
  defeats Authorization-only hashing, and Authorization ∪
  include_headers hashing still assumes the declared list is complete
  — the failure mode is silent cross-tenant delivery, and the
  assumption is unverifiable by the cache. The strict gate makes the
  assumption unnecessary.
- **Apply the §3.5 storability gate to flights** (collapse only when
  the response will be storable). Wrong layer: the gate's inputs (the
  response's Cache-Control) are unknown until the fetch completes, and
  followers park before that.
- **Hash every request header into the flight key.** Maximally safe
  against unknown selectors but ends collapsing for all traffic
  (Cookie, User-Agent, … differ per request). The Authorization gate
  captures the credential dimension, which is the one RFC 9111
  singles out for shared caches.
