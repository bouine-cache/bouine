# ADR-0057: Key the collapsing flight on the declared variation dimensions

- **Status**: Accepted
- **Date**: 2026-10-07
- **Deciders**: @bouine-core
- **Phase**: cache key policy
- **References**: RFC 9110 §12.5.5, RFC 9111 §3.5, §4.1; ADR-0046, ADR-0052

## Context

Request collapsing parks concurrent requests for the same cache key on
one leader's in-flight origin fetch and hands the leader's response to
all of them. ADR-0052/0054 removed requests that can be named
unconditionally (`Authorization`, `Cookie`, unsafe methods).

That leaves the **anonymous cold miss**: with no stored object, the
flight key was the primary cache key (`scheme|host|path|query|method`),
which carries neither a route's `include_headers` dimensions (those
govern stored variants only, ADR-0046) nor an unseen origin `Vary`.
Two requests differing only in a selector header — byte-identical URI
and Host — collapsed onto one leader, and every follower received the
leader's variant body. This happened in production; unlike the peer
gates, collapsing fails *unsafe* (it delivers the bytes).

## Decision

**A flight is shared only under a key that encodes every dimension the
response is known to vary on.** One helper, `collapseFlightKey`, derives
the flight key at every collapsing site and refuses the share (zero
key) when safety cannot be proven:

1. **Warm flight** (stored object exists, with or without Vary): the
   lookup key — a stored object is the origin's own declaration for
   this URL. A variant key with no object (evicted variant, surviving
   resolver) derives from a stored `VaryValue` and is treated the same.
2. **Cold flight, `include_headers` route**: primary key extended
   with the declared headers, hashed by the same `variantKeyCore`/
   `varyHeaderValue` path as the storage variant key — a flight
   collapses two requests exactly when their stored variants would be
   identical. Same-dimension callers keep deduping.
3. **Cold flight, include-free route**: refused. The origin's Vary is
   unknowable before the first response arrives. Each concurrent caller
   fetches its own copy; the next burst is warm and collapses — only
   the first burst per key pays.

## Consequences

- Warm traffic unchanged; the deploy/restart/purge herd still collapses
  per dimension value on declared routes.
- **Cold bursts on include-free routes lose collapsing** — one origin
  fetch per concurrent caller for the first fill per key, bounded by
  the fetch semaphore and shed. Pre-warm hot routes if the origin
  cannot take the spike.
- A mixed-version fleet splits flights (fail-safe: lost dedup, never a
  wrong body). Roll the fleet uniformly.
- A route whose origin varies on *undeclared* headers is still unsafe
  in storage as in flight — the operator's declaration to get right
  (ADR-0046).

**Invariants:**

- **Single hasher.** The flight key reuses `variantKeyCore` +
  `varyHeaderValue`; any future value-normalization must land there,
  never on one path only, or two callers whose values differ only in
  the folded class would share a flight whose stored variants are
  distinct. Pinned by `TestCollapseFlightKey` (flight/variant parity).
- **Warm trust is bounded by ADR-0058**: a revalidation observing a
  changed surface purges the stale resolver and its variants. The
  residual window is one TTL, not zero.

## Alternatives considered

- **Refuse every cold miss** — needlessly conservative where the
  declaration makes the share provable.
- **Hash every request header into the flight key** — ends collapsing
  for all traffic; rejected by ADR-0052.
- **A per-route `cache.collapse: false` knob** — pushes the footgun
  onto operators. Rejected.
