# ADR-0045: PurgeEvent.VaryKey is metadata; receivers purge the primary key and all variants

- **Status**: Accepted
- **Date**: 2026-09-15
- **Deciders**: @theotime
- **Phase**: cluster hardening
- **Closes**: #631

## Context

`api.PurgeEvent.VaryKey` (pkg/api/cluster.go) was documented as
"if non-empty, targets only the variant", but every receive path —
the gossip applier, the HTTP peer-purge endpoint, and both batch
endpoints (ADR-0044) — funnels into `runState.purgeKey`, which calls
`cache.Handler.Purge`. `Handler.Purge` deletes the primary key **and**
every locally tracked variant (`variantSets`), which is what RFC 9111
§4.2.4 requires ("a cache MUST invalidate all stored response
instances... when it receives a non-error response" — i.e. invalidation
is resource-wide, not variant-scoped). All senders pass `""` today, so
nothing observable is wrong, but the first sender that populated the
field would over-purge silently. The issue asked to either honor the
field (variant-scoped delete) or repurpose it.

Three facts make variant-scoped delete wrong, not merely unnecessary:

1. **The variant store key cannot be reconstructed from the
   assertion hex.** `PurgeEvent.VaryKey` would carry the
   `BuildVaryKey` hex (the value stored in `Object.VaryKey` and
   asserted in peer fetch, RFC 9111 §4.1). The variant **store key**
   is composed by `VariantKey`/`VariantKeyFast` via
   `Key.WithVary(xxhash(variant-headers))`. The two canonicalizations
   differ: the store-key path lowercases header *values*
   (`normalizeHeaderValue`), the assertion path keeps case
   (`normaliseListHeader` preserves `BM-Market: FR` verbatim). Verified
   by direct hash comparison: for `Vary: BM-Market` with value `FR`
   the store-key hash and the assertion hash disagree. A receiver
   that XORed the assertion into `evt.Key` would build a key that
   matches no stored entry — the purge would silently no-op and stale
   variant bodies keep being served, a worse failure mode than
   over-purging.

2. **RFC 9111 §4.2.4 requires purging all variants.** Invalidation of
   a resource is resource-wide. A "purge just variant A" operation
   would be a non-RFC operation invented for the wire format, with no
   admin surface (`/v1/purge` takes a URL, and variant identity depends
   on request selecting-headers a URL cannot express).

3. **Symmetric support would be a wire-format change to
   `RefreshEvent` too.** A half-measure (scoped purge, unscoped
   soft-purge) makes cluster semantics depend on which event type a
   caller chose, a correctness trap.

## Decision

Receivers of `PurgeEvent` (gossip, HTTP peer-purge, batch endpoints)
MUST apply the purge to `evt.Key` and every locally tracked variant
under it, regardless of `VaryKey`. The field is repurposed as **issuer
metadata**: the `BuildVaryKey` assertion hex of the local object that
triggered the purge, useful for observability and future debugging,
never a purge target. Senders currently pass `""`.

`RefreshEvent` (soft purge) follows the same rule: the refresh
applies to the primary key and all variants. No `VaryKey` field is
added to `RefreshEvent` — none is needed and adding one would repeat
this ambiguity.

If a variant-scoped invalidation is ever needed, it must be a new,
explicit wire operation that carries the **variant store key** (a full
`api.Key`, not the assertion hex), with its own ADR.

## Consequences

### Positive
- Receive-path semantics stay RFC 9111 §4.2.4-conformant and identical
  for all event shapes (single gossip, HTTP, batch), eliminating the
  latent over-purge surprise the issue flagged.
- No wire-format change: `VaryKey` keeps its name, position, and
  `json:"vary_key,omitempty"` serialization (already round-tripped by
  the purge codec).
- The documented contract (`pkg/api/cluster.go`) now matches the
  implemented behavior; the misleading comment is gone.

### Negative / trade-offs
- The field cannot be used for variant-scoped invalidation without a
  future wire change (deliberate: see Decision).
- `PurgeEvent.VaryKey` is carried but unused at receive time — a few
  bytes per frame in batches.

### Risks
- A future contributor could read "VaryKey" in the struct and assume
  scoping. Mitigated by the doc comment citing this ADR and by the
  receive-path test pinning purge-all behavior.

## Alternatives considered

- **Honor `VaryKey` as a variant-scoped delete target** (compose
  `evt.Key.WithVary(hash(evt.VaryKey))`): rejected — the composition
  is provably not the stored variant key (different
  canonicalization), so the delete would silently no-op while stale
  bodies keep serving. It also contradicts RFC 9111 §4.2.4.
- **Carry the full variant store key (16 bytes) in the event instead
  of the assertion hex**: possible, but there is no legitimate sender
  (admin purge-by-URL cannot know variant identities), and it invites
  non-RFC partial invalidation. Rejected; revisit via a new ADR if a
  real use case appears.
- **Remove the field entirely**: rejected — `pkg/api` wire types are
  `// Stable.`; removal breaks the codec layout and any external SDK
  consumer for zero functional gain.

## References

- ADR-0030 (128-bit key, `WithVary` composition), ADR-0044 (batched
  invalidation delivery), ADR-0042 (Vary resolver/variant split).
- RFC 9111 §4.1 (Vary), §4.2.4 (invalidation removes all variants).
- docs/architecture.md §"Purge / Ban / Refresh semantics".
- internal/cache/handler.go (`Purge`, `variantSets`), cmd/bouine/cmd/engine.go
  (`purgeKey`, `peerPurgeApply`, gossip `Invalidator` wiring).
