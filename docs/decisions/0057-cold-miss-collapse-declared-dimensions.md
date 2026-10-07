# ADR-0057: Key the collapsing flight on the declared variation dimensions

- **Status**: Accepted
- **Date**: 2026-10-07
- **Deciders**: @bouine-core
- **Phase**: cache key policy
- **References**: RFC 9110 §12.5.5, RFC 9111 §3.5, §4.1; threat-model T06, T07; ADR-0046 (include_headers union); ADR-0052 (credential/method flight gate); `internal/cache/collapse.go`, `internal/cache/keypolicy.go`, `internal/cache/handler.go`

## Context and Problem Statement

Request collapsing (the singleflight dedup behind `collapsedFetch`, the
`inflightStreams` table in `fetchAndStore`, `collapsedRevalidateBg`, and
the shed-refill) parks every concurrent request for the same cache key
on one leader's in-flight origin fetch and hands the leader's response
to all of them.

ADR-0052 closed the in-flight handoff for requests that could be named
unconditionally: carriers of `Authorization`, carriers of `Cookie`
(ADR-0054), and unsafe methods. That gate is deliberately blind to
credential equality — it refuses the share rather than trying to prove
two callers interchangeable.

That leaves a structurally identical shape it does not cover: an
**anonymous cold miss**. On a cold miss there is no stored object, so
the flight key is the **primary** cache key — `scheme|host|path|query|
method`. The primary key does not carry a route's declared
`cache.key.include_headers` dimensions (those govern *stored* variants
only, ADR-0046) and it cannot carry an origin `Vary` the cache has never
seen. Two requests that differ only in a selector header — byte-identical
URI and Host — therefore collapse onto one leader, and every follower
receives the leader's variant body. This was not hypothetical: a route
fronted under a single internal host and a selector-independent path used
`include_headers` to select a variant via request headers, and on the
first fill after a deploy or restart one caller's variant was handed to
a concurrent caller. The peer gates never see this failure because they
fail *safe* (reject the hit); collapsing fails *unsafe* (deliver the
bytes).

## Decision

**A collapsing flight is keyed on the dimensions the response is known
to vary on — never on the primary key alone when nothing establishes
those dimensions.** One helper, `collapseFlightKey`, derives the flight
key at every collapsing site and refuses the share (zero key) when it
cannot be proven safe:

1. **Warm flights — a stored object exists (with or without `Vary`)**:
   the flight key is the lookup key. A stored object is the origin's
   own declaration for this URL: a Vary-carrying object yields the
   variant key, which already hashes the declared dimensions and the
   origin `Vary`; a stored object *without* Vary is a positive statement
   that the origin returns one body for this URL regardless of request
   headers (RFC 9110 §12.5.5), so the primary key is a proven identity.
   **A variant key with no object** (the stored variant was evicted
   while the resolver survives) is treated the same: it was derived
   from a STORED `VaryValue`, so it already encodes the origin's full
   declared surface — keying such a flight on the include-only
   dimensions instead would re-open cross-dimension sharing the stored
   Vary distinguishes. Behavior unchanged.

2. **Cold flights on a route WITH `include_headers`**: the flight key
   is the primary key extended with the declared headers, hashed by the
   same `variantKeyCore`/`varyHeaderValue` path the storage variant key
   uses (bucketed Accept-Encoding/Accept-Language, legacy
   lowercase+sort otherwise). A flight therefore collapses two requests
   exactly when their stored variants would be identical — cold-miss
   dedup is *preserved* for same-dimension callers, and different-
   dimension callers never meet on one flight. The parity with the
   storage keying is the whole safety argument: any divergence between
   the flight key and the variant key would be a wrong-body handoff
   waiting to happen, so there is exactly one implementation of the
   pair-hash.

3. **Cold flights on a route WITHOUT `include_headers`**: refused. The
   origin's `Vary` is unknowable at flight time (the response has not
   arrived, nothing has ever declared the variation surface), and an
   undeclared selector header re-creates the cross-caller body swap.
   The request fetches its own copy; once the first fill stores the
   object, every subsequent concurrent miss is warm (case 1) and
   collapses again — only the first concurrent burst per key pays.

The rule is blind to header **values** except through the storage-key
normalization: it shares only what a declaration proves shareable, and
refuses when nothing does. It does not attempt value equality on
undeclared headers — an undeclared selector is the one failure this
cannot key on, so it refuses instead of guessing.

## Consequences

### Positive
- The in-flight path is leak-free for the undeclared-selector shape on
  every route, with no config knob that can be forgotten.
- Cold-miss dedup survives on declared-dimension routes — the thundering
  herd after a deploy, restart, or purge still collapses per dimension
  value instead of one origin fetch per concurrent caller.
- Warm traffic — the overwhelming majority — is unchanged in behavior
  and key derivation.
- One helper derives the flight key at every site; there is no second
  hashing implementation to drift.

### Negative / trade-offs
- **Cold bursts on include-free routes lose collapsing** (case 3). This
  is a deliberate behavior change: the first concurrent fill per key
  costs one origin fetch per caller, bounded by the fetch semaphore and
  the shed machinery — exactly what the origin saw before collapsing
  existed. Deploy/restart storms on high-traffic include-free routes
  will see a one-shot origin load spike per hot key; a proactive warmup
  or purge-then-warm before flipping a route onto the cache keeps the
  window short.
- **Cold flights on declared routes pay one `variantKeyCore` hash plus
  a small header snapshot per miss.** The snapshot is sized to the
  declared list (≤16 fields); the hit path is untouched.
- A route that declares `include_headers` whose origin varies on
  *undeclared* headers is still unsafe in storage as in flight — that
  is the operator's declaration to get right (ADR-0046), unchanged
  here.
- A mixed-version fleet (some nodes without the composite key) splits
  flights: a follower on an old node may park on an old-key flight.
  Same hazard class as `exclude_headers`/`include_headers` divergence
  (ADR-0046 §Risks); the gates fail safe, and a uniform rollout is
  expected anyway.

### Risks
- **Flight-key/variant-key parity drift.** `collapseFlightKey` reuses
  `variantKeyCore`+`varyHeaderValue` rather than a second hasher; any
  future change to one without the other reintroduces the bug class.
  Pinned by
  `TestCollapseFlightKey/flight_key_matches_the_storage_variant_key_for_the_same_dimensions`.
  The deeper invariant is SEMANTIC, not byte parity: declared-dimension
  equality is only safe because `varyHeaderValue` is the single
  value-normalization authority for both keys — two requests whose
  declared values differ only in a normalization equivalence class
  (bucket, case fold, a future language-fold rule) share a flight iff
  their stored variants also merge. A future normalization must land
  in `varyHeaderValue` itself, never as a flight-only or storage-only
  rule, or the equivalence classes diverge and a follower parks on a
  leader whose variant it does not hold.
- **Warm-path trust in origin honesty (closed by ADR-0058).** A
  Vary-forgetting origin poisons warm flights by design: the stored
  resolver keeps declaring the old surface, and every warm flight
  keys on variant keys the origin no longer produces. ADR-0058 closes
  the detectable half — a revalidation that observes the fresh
  response declaring a different surface purges the stale resolver
  and its variants (`bouine_vary_drift_total` is the operator
  signal). The undetectable half remains: between two revalidations,
  a silent origin is indistinguishable from an honest one; only the
  first request after each TTL window carries the evidence.
- **`exclude_headers` interplay**: a declared-then-excluded header
  contributes nothing to either key (both paths consult the same
  policy), so the pair stays consistent by construction. The config
  loader rejects `include_headers ∩ exclude_headers` overlap outright
  (validateIncludeHeaders), so the both-keys-gutted shape cannot be
  configured.
- **Object churn**: a key whose object is evicted between a leader's
  lookup and a follower's arrival returns the follower to the cold
  case — safe by construction (its flight key carries the dimensions;
  the follower just may not meet the leader's flight). No wrong body
  is possible, only a lost dedup.

## Alternatives considered

- **Refuse every cold miss (the storage-gate analogue only).** Safe, but
  it deletes the cold-burst dedup even on declared-dimension routes,
  where the declaration makes the share provable. Rejected as needlessly
  conservative: the dimension-extended key restores the dedup without
  weakening safety.
- **Hash every request header into the flight key.** Maximally safe
  against unknown selectors but ends collapsing for all traffic (Cookie,
  User-Agent differ per request). The Authorization gate already
  captures the credential dimension; ADR-0052 rejected this class.
- **A per-route `cache.collapse: false` knob.** Pushes the footgun onto
  operators — the same "you must remember to declare it" shape that
  caused the original bug. Rejected.
- **Key the flight on `include_headers` only when the route has them,
  and collapse bare primary keys otherwise** (the pre-composite
  behavior). This is exactly the hole this ADR closes: the bare primary
  key was never proven to encode what the body depends on.

## References

- ADR-0046 — fold `cache.key.include_headers` into the stored Vary.
- ADR-0052 — never collapse authorized, cookied, or unsafe-method requests.
- RFC 9110 §12.5.5 — an origin that varies on request headers MUST
  declare it in `Vary`.
- RFC 9111 §3.5 — storability of authorized responses (the storage gate
  collapsing bypasses); §4.1 — cache key calculation with Vary.
- Threat model T06/T07 — headers participate in the cache key only via
  Vary or an explicit per-route allow-list.
