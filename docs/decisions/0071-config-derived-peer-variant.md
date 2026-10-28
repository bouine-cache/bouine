# ADR-0071: Config-derived Vary resolution skips the bare ask

- **Status**: Accepted
- **Date**: 2026-10-28
- **Deciders**: @chrisdupin
- **Phase**: phase 3 (cache core)
- **See also**: ADR-0068 (metadata-only Vary resolver),
  `internal/cache/resolver.go`, `internal/cache/fastpath.go`,
  `internal/cache/handler.go`, issue #868 (the prod peer-fetch tail)

## Context

ADR-0068 closed the streamed-resolver body leak, which was the only way
a node without local Vary context converged on a Vary-keyed URL. The
legitimate replacement — the two-step resolution — costs **two
admin-port RPCs per Vary cold miss on a non-owner**: a bare primary-key
ask whose only yield is the resolver's metadata (the Vary list the
requester could not compute locally), then the derived variant-key ask
with a real assertion. Both legs land on the owner's admin server under
`tracing.FastHTTPMiddleware("bouine.admin")`.

In production this was measured as a sustained peer-fetch p99 tail that
scales with request volume (issue #868): the bare ask is network
round-trip whose information content is, on most affected routes,
already present in the requester's own configuration.

The Vary list of a stored entry is the union of the origin's declared
`Vary` and the route's `cache.key` dimensions
(`effectiveVary`, stream.go): `include_headers`, plus the synthetic
cookie-presence field. All three inputs of that union except the
origin's own `Vary` header are route config, and the KeyPolicy is
pinned identical across cluster nodes (keypolicy.go, AGENTS.md §3).
For the routes that drove the production tail — multi-locale,
origin-declares-no-Vary shapes where bouine synthesizes the variant
keying from `include_headers` alone — the policy **fully determines**
the stored Vary list.

## Decision

1. **`configDerivedVary(policy)`** returns the Vary list the KeyPolicy
   alone determines: the `include_headers` union with the synthetic
   cookie-presence field, normalized exactly as `effectiveVary`
   normalizes the stored union (sorted, deduplicated, ", "-joined), so
   a correct derivation hashes to the stored variant keys
   byte-for-byte. Empty when the policy keys no variants.
2. **On a route where the derivation is non-empty, the primary key only
   ever holds the metadata-only resolver** (every servable entry is
   stored under a derived variant key), so the bare ask can never
   return a body:
   - The H1 fast path (`tryPeerFetch`) **skips the bare ask** — no RPC,
     no owner hints: the owner was not asked, and the slow path must
     run its own question.
   - The slow path (`handleCacheMiss`, plain miss only: no stored
     object, no local resolver, after the transferred-resolver reuse)
     **derives the variant key locally and asks the variant's owner
     directly** with a real vary-key assertion
     (`serveDerivedPeerVariant`, the shared step-two body). One RPC
     instead of two.
3. **Wrong derivations fail open to the ADR-0068 two-step.** The
   derivation equals the stored Vary list iff the origin declared no
   Vary field beyond the policy's own (a declared field inside the
   include list dedups into the same union). An origin that declares
   extra fields makes the derived variant key hash elsewhere: the ask
   answers a miss and the bare ask runs — fetching the authoritative
   resolver metadata — followed by the real variant ask. The cost of a
   wrong derivation is **one extra ask, never a wrong body**: the
   owner-side variant-assertion gate (peerfetch.go) and
   `servePeerHit`'s response-side gate still bound what is served.
   Mixed-config fleets (an owner running a different route policy) hit
   the same fallback.
4. **Completion accounting is unchanged**: a config-derived resolution
   increments `bouine_peer_fetch_resolver_answers_total{side="consumer"}`
   exactly like the wire-resolved two-step, so fleet convergence stays
   observable and a mixed-version fleet still shows up as zero
   consumer-side answers.
5. **Routes without key headers keep today's two-step exactly** —
   origin-declared-Vary routes (no include list) cannot derive locally
   and were never targeted.

## Consequences

- A Vary cold miss on a non-owner costs one peer RPC (down from two) on
  every route whose key policy determines the Vary list — including the
  30s-TTL L1 routes and the `include_headers` L2 routes that drove the
  production tail. The fast path no longer mints the admin span for the
  skipped ask either.
- Per-request added work when the derivation is right: one local hash
  (`VariantKey` + `BuildVaryKey` are already the step-two cost). When it
  is wrong: one extra peer RPC per cold miss on the affected route until
  the origin stops declaring the extra field — bounded, observable as a
  three-ask sequence in `bouine_peer_fetch_resolver_answers_total` and
  the owner logs, and never a correctness hazard.
- No wire change: `ClusterProtocolVersion` stays "4"; the ask format is
  untouched (the derived ask is today's variant-key ask, sent to the
  variant's owner instead of the primary's). Rolling upgrades mix
  freely; old-version owners answer the derived ask as an ordinary
  variant-key ask.
- Pinned by `TestConfigDerivedVary` (normalization parity with
  `effectiveVary`), `TestResolverPeerVariant_ConfigDerivedSingleAsk`
  (one ask, real assertion), `TestResolverPeerVariant_ConfigDerived-
  FallsBackOnWrongDerivation` (three-ask fallback, correct variant
  served), and `TestFastPathHandler_ConfigDerivedSkipsBareAsk` (no RPC,
  no hints). The owner side is untouched — no owner tests change.

## Notes

The rejected stronger alternative — the ask carrying the request's
selecting header values so the owner derives the variant server-side —
was deferred: it needs a `ClusterProtocolVersion` bump, ships header
values over the wire, and only saves the round-trip when the primary's
owner and the variant's owner colocate on the ring (~1/3 on a 3-node
fleet), otherwise the owner proxies and the fleet-wide RPC count is
unchanged. Deriving locally needs none of that and works on every fleet
topology.
