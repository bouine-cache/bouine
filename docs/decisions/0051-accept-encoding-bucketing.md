# ADR-0051: Bucket Accept-Encoding in the variant key, paired with canonical origin forwarding

- **Status**: Accepted
- **Date**: 2026-09-27
- **Deciders**: @bouine-core
- **Phase**: cache key policy
- **References**: RFC 9110 §12.5.3, §5.2.1; RFC 9111 §4.1; ADR-0046 (include_headers union); ADR-0049 (variant metadata); `docs/plans/accept-encoding-bucketing.md`

## Context and Problem Statement

`docs/architecture.md` §3.3 has claimed since v1.0 that responses are
keyed by bucketing `Accept-Encoding` into `br | zstd | gzip | identity`.
The claim was false: what existed was token sorting, which collapses
ordering differences (`"gzip, deflate, br"` ≡ `"br, gzip, deflate"`) but
not token-set differences. Measured, eight realistic browser AE
strings produced six distinct cache variants for the same resource:

- `gzip, deflate, br` / `br, gzip, deflate` / `deflate, gzip, br` → one
  variant (the sort works within this set)
- `gzip, deflate, br, zstd` → another
- `gzip, deflate, br, zstd, identity` → another
- `gzip` / `gzip, br` / `identity` → three more

Each extra variant costs an origin fill, a `MaxVariants` (64) slot, and
hot-tier capacity for a body that is byte-identical to its siblings.
The win is not speculative: at `Vary: Accept-Encoding`, every browser
population that negotiates br collapses to one variant.

Two structural constraints shaped the decision:

1. **The hash-input construction must not fork.** ADR-0046's lesson:
   the variant-key hash input was built in several places, and missing
   one with a policy change is precisely the cross-variant body-swap
   bug class the peer gates exist to prevent. The live set is two
   normalizers (`normalizeHeaderValue` in vary.go, the
   `isListValuedVaryField`/`normaliseListHeader` pair in key.go)
   consumed by four construction paths (`variantKeyCore`,
   `variantKeySlow`, `buildVaryKeyInto`, and via `BuildVaryKey` the
   peer-gate assertions).
2. **Bucketing alone is incorrect.** The bucket is a claim about what
   the origin would send. If the key says `br` but the origin receives
   the client's raw `Accept-Encoding`, a deflate-only origin can store
   `Content-Encoding: deflate` bytes under the `gzip` bucket, and a
   later gzip-only client receives bytes it cannot decode. The bucket
   must therefore be paired with a canonical outbound header.

## Decision

1. **`encodingBucket(v string) string`** reduces an AE header value to
   one of `zstd | br | gzip | identity`: the highest-weight coding among
   the three compressions wins (ties broken zstd > br > gzip), `q=0`
   excludes (RFC 9110 §12.5.3), absent weight is 1 (§5.2.1), and every
   other token — `deflate`, `identity`, unknown — contributes nothing.
   No-acceptable-coding input yields `identity`; bouine never
   generates 406.
   `deflate` is deliberately outside the bucket set: the bucket set is
   exactly the codings bouine re-serves verbatim, and a deflate-only
   origin answering identity for the whole bucket is correct, just
   uncompressed.
2. **One dispatch point**: `varyHeaderValue(field, value, policy)` is
   the single normalization entry for every variant-key construction
   path; `accept-encoding` routes to `encodingBucket`, everything else
   keeps the legacy lowercase+sort normalization. The old
   `isListValuedVaryField`/`normaliseListHeader` pair was folded in and
   deleted. Cross-path parity is pinned by
   `TestVaryKeyEncodingBucket_ParityAcrossPaths`.
3. **Paired outbound rewrite**: `rewriteOutboundAE` sets the canonical
   bucket token on every origin-bound fetch from a cache-enabled route
   — miss fill, revalidate, background revalidate, background refresh,
   shed refill, and the streaming tee branch. Identity removes the
   header (a bare `identity` token reads as "client refuses
   compression" on some origins). The hit path never rewrites.
4. **Escape hatch**: `cache.key.verbatim_encoding: true` restores
   pre-bucketing behavior (lowercased+sorted raw value keyed, verbatim
   origin forwarding) for origins that genuinely vary bodies by the
   full AE string. Validation rejects any other value. Like
   `include_headers`, mixed settings across cluster nodes store/resolve
   under different keys — the peer gates fail safe (miss, never a wrong
   body), same hazard class as ADR-0046.

## Consequences

### Positive
- One stored variant per negotiated coding per resource: six AE
  dialects become two variants (br, zstd) where they were six.
- Cheaper than the old path on the hit-path key computation: 2
  allocs/200ns versus 4 allocs/275ns for the legacy sort (the
  `FastPath_PeerHitVary` gate benchmark's AE-bearing cousin,
  `VaryKey_AcceptEncodingBucket`, budgets 2; the verbatim sibling
  budgets 4 as the pinned legacy baseline).
- `docs/architecture.md` §3.3 is finally true.

### Negative / trade-offs
- All existing AE variant keys change on upgrade. Previously stored
  variants become unreachable and expire naturally by TTL/eviction; a
  mass purge would spike origin load (the ADR-0045 lesson). Operators
  should expect a transient miss-rate step on AE-varied routes.
- Mixed-version cluster windows cannot share AE variants: nodes on
  different sides of the upgrade compute different VaryKey hex, and
  peer fetch answers miss. Availability degradation only — the
  server-side assertion gate (peerfetch.go) rejects, never serves a
  wrong body. Complete the rolling restart.
- Origins that key anything on the literal AE string (rare, and
  indistinguishable from an origin bug) need `verbatim_encoding: true`.

## Alternatives considered

- **Sort-only normalization (status quo)**: leaves the zstd/identity
  fork and the six-dialect fragmentation; rejected — the whole point.
- **Bucket without the outbound rewrite**: rejected as incorrect, not
  merely suboptimal — the deflate-only-origin case stores a coding the
  bucket cannot honor.
- **Bucket on negotiation outcome including `deflate`**: rejected — a
  fourth bucket for a coding no browser prefers and bouine never
  serves doubles the variant space for no traffic.
- **Per-coding egress recompression (`normalize_identity`)**: rejected
  here; it breaks the zero-alloc egress path and needs its own ADR.
  Remains documented as backlog in §3.3.
- **Cluster-wide negotiated downgrade to the lowest common coding**:
  rejected — requires cross-node request coordination the protocol
  does not have; the canonical-token rewrite achieves consistency
  locally.
