# ADR-0068: Metadata-only Vary resolver

- **Status**: Accepted
- **Date**: 2026-10-25
- **Deciders**: @thylong
- **Phase**: phase 3 (cache core)

## Context

Every cacheable response with `Vary` was stored **twice** in the hot
tier: the variant under its variant key, and a full clone of it — same
body bytes — under the primary key as the "Vary resolver"
(#824). The fill-path comment claimed the two entries "share Header and
Body", but `HotStore.Put` deep-copies the body on every store
(pinned by `TestHotStore_PutOwnsCallerBody`; callers reuse their
buffers, so the copy is correct *per store*). Measured on `main`:
200 Vary pages filled 400 hot entries at **2.03x** the bytes of the
identical pages without `Vary` — half the hot budget spent on
duplicates for Vary-heavy workloads, plus a second disk copy whenever
the body crossed the warm threshold.

The resolver exists for three jobs: (1) answer `lookup()` with the
stored Vary list so a variant can be selected; (2) supply validators to
the resolver-304 optimization (`revalidateResolverMiss` →
`serveResolver304`); (3) forward to the cluster owner. Only (2) needs
bytes — and only to *serve* them after a validating 304.

A second defect rode along: the streaming fill path (the default
cold-miss route) stored the resolver **without** blanking `VaryKey`,
violating the "blank `VaryKey` by protocol" invariant the peer-fetch
and fast-path gates classify resolvers by
(fastpath.go, peerfetch.go). Streamed resolvers carried the variant's
vary-key hash and could leak through those gates.

## Decision

1. **`api.Object.PairedKey`** (additive, pkg/api semver) links the two
   entries of a Vary family: on a resolver it names the variant whose
   body the resolver mirrors; on a variant it names the primary the
   variant belongs to, so a node storing the variant via the
   write-to-owner RPC can register it in the primary's variant set.
   Resolver invariant:
   `VaryValue != "" && VaryKey == "" && Body == nil` (identity is the
   protocol shape, not PairedKey — variants carry it too).
2. **One constructor, three fill sites**: `buildResolver(variant,
   primaryKey)` replaces the ad-hoc clones in `writeAndMaybeStore` and
   both streaming store sites. It blanks `VaryKey` (fixing the streaming
   drift on all sites at once), drops the body, and sets
   `PairedKey = variant.Key`. Fill sites stamp variants with
   `PairedKey = primaryKey`. Headers stay identical to the variant's;
   the store's per-Put deep-clone is untouched.
3. **`serveResolver304` serves the mirror's body.** Legacy resolvers
   (pre-ADR warm blobs, `PairedKey` zero) keep serving their own body.
   Mirror missing from both tiers → forward the origin 304 when the
   client's conditional was validated, else fall through to the
   ordinary fill. `Content-Length` is set from the bytes actually
   served.
4. **Resolvers exit the refresh registry**: `storeObject` never
   registers a resolver, and `doBackgroundRevalidate` bails on one — a
   full-200 refresh of the primary key would resurrect the duplicate
   body. The resolver's validators therefore age with their store; a
   stale ETag still conditions the revalidation fetch, and a rotated
   representation answers full-200 → ordinary fill. One fewer origin
   fetch per Vary URL per TTL.
5. **Codec v7**: `PairedKey` as 16 fixed bytes after the flags byte;
   v6-and-older decode as zero (legacy shape). One chokepoint serves
   warm tier, WAL, and the peer wire.
6. **Resolvers always warm-back** (`TieredStore.Put` gate extended with
   `IsVaryResolver()`): `lookup()` reaches warm variants only through
   the Vary list carried by the entry at the primary key — the resolver
   IS routing metadata. A bodyless resolver that skipped the warm tier
   would strand every warm variant of that URL after a restart until
   re-fill. The metadata blob is a few hundred bytes.
7. **Two-step variant resolution for non-owners.** A node with no local
   Vary context peer-fetches the bare primary key; the owner answers
   with the metadata-only resolver (new peer answer class, blank
   assertion only), the requester derives its variant key from the
   learned Vary list and re-asks the variant's owner with a real
   vary-key assertion. The fast path hands resolver-shaped answers to
   the slow path (`RawRequest.OwnerResolver`, transferred by the h1parser
   under `OwnerResolverContextKey`) so the slow path runs the two-step
   WITHOUT repeating the identical bare ask — the same question-identity
   proof the boolean owner hints carry, but stronger: the transferred
   object carries its own `Key`, so the slow path reuses it only when
   `Key == lookupKey` (no nil-policy restriction needed). The answer
   class is observable: `bouine_peer_fetch_resolver_answers_total{side}`
   counts owner answers ("server") and completed two-step resolutions
   ("consumer"). On main this convergence path did not exist — streamed resolvers leaked through
   the owner's withhold gate (non-blank `VaryKey`) and were served as
   primary-key hits carrying the first variant's body; closing the leak
   made the legitimate path mandatory.
8. **Peer-put variants register in the owner's variant set.**
   `StoreFromPeer` calls `reserveVariantSlotFromPeer(PairedKey, Key)`
   for variants: the fill ran on another node, so the owner never
   tracked them, and a cluster-wide URL purge would have missed them
   (RFC 9111 §4.4) — reachable-but-unkillable once the two-step made
   them servable. Registration must not depend on refresh config:
   `buildPeerPutHandler` wires the owner's `onStore` callback
   unconditionally, whereas it previously wired `StoreFromPeer` only
   when some route enabled refresh-before-expiry — in a default config
   the callback stayed nil and peer-put variants were stored bare.
   The callback still calls `storeObject`, but peer-put objects never
   join the proactive refresh registry: the write-to-owner RPC carries
   an `api.Object` and the object codec has no URL field, and the
   refresh executor rebuilds its fetch from the registry entry's url —
   a URI-less registration would schedule a fetch that fails on every
   attempt and retries until the TTL lapses. Peer-put objects keep
   request-driven revalidation (SWR/SIE/stale-on-hit), the strong-mode
   refresh path (issue #509). Peer-put registration
   always succeeds, including at the `MaxVariants` cap: the write-to-owner
   RPC (`PeerPutHandler.Handle`) stores the object BEFORE the
   registration callback runs, so a refused registration would leave a
   stored-but-untracked variant — reachable via the two-step, invisible
   to the cluster-wide URL purge, i.e. the exact reachable-but-unkillable
   shape items 8 and 9 exist to close. Peer-put registrations may
   therefore exceed the cap in the set; growth is bounded by SIEVE
   evicting the stored variants (the dead-variant sweep prunes them on
   later registrations) and by `Purge` deleting the set. Local fills
   keep the cap as a storage refusal, where refusal is enforceable
   (the local fill has not stored yet).
9. **Peer-put registration skips the stale-incarnation primary
   probe.** `reserveVariantSlot`'s reconciliation probes for the local
   primary and wipes the variant set when absent — sound for local
   fills, wrong in cluster mode, where the variant's owner and the
   primary's owner are independent hash-ring assignments (the probe
   would wipe live registrations on a node that owns the variant but
   not the primary). `reserveVariantSlotMode(..., fromPeer)` skips the
   probe for peer-puts; the dead-variant sweep is retained and the cap
   remains a storage refusal on the local-fill path only (item 8).
   Caught by the integration test
   `TestStrong_PurgeVariantIsolation`.

## Consequences

- A Vary page costs ~1x its body in hot-tier RAM instead of ~2x, and one
  metadata copy in the warm tier (also no second disk copy).
- Under SIEVE pressure the tiny resolver outlives the fat variant; the
  resolver-304 path then degrades to 304-forward/full-fill more often
  than before (the mirror lookup falls back to the warm tier first).
  Offset: today's fat resolvers are themselves evicted under pressure,
  losing validator routing entirely, while the tiny resolver keeps
  `revalidateResolverMiss` alive.
- Strong-mode ownership: the original plan claimed "no behavior change
  in cluster mode" — wrong. Closing the streaming-`VaryKey` leak shut
  down the only (illegitimate) convergence path non-owners had, so the
  two-step resolution (item 7) and the peer-put variant registration
  (items 8 and 9) became part of this ADR. The
  `revalidateResolverMiss` path remains local-only: it runs where
  `lookup()` found a local resolver, which (via the owner gate) also
  stored the variant locally.
- The hit path is untouched (resolver construction is fill-path only;
  the fast path never reads resolvers beyond handing the metadata
  answer to the slow path).
- The per-handler variant-set split is real but closed: keys are
  URL-scoped and the store is shared across routes, while `variantSets`
  is per-handler and peer-put registrations land on handlers[0] — so a
  purge that stopped at the first handler reporting `owned` could
  leave a locally-tracked variant of another handler alive (the
  poisoning e2e suite caught exactly this, reachable-but-unkillable
  again). `purgeKey`/`softPurgeKey` now consult every handler; the
  shared store makes the deletes idempotent. The cluster-mode
  reconciliation also had to change shape: a node may store a primary's
  variants without the primary itself (independent ring assignments),
  so the stale-incarnation probe cannot wipe wholesale there — liveness
  is decided per tracked variant (pruneDeadVariants), and the wholesale
  wipe remains only in single-node mode, where an absent primary really
  does retire the whole set. Both fixed with the e2e suite
  (`TestStrong_PurgePeerPutVariantsPastCap`).

## Notes

Alternatives rejected: sharing one body buffer across both store
entries (refcounting through the hottest eviction/slab code for no
behavior win); storing the mirror pointer in `VaryKey` (the field is
protocol-load-bearing — blank means resolver); a handler-local
primary→variant map (dies on restart; the codec must carry the field
anyway). The `POST`-Location Vary ambiguity
(`maybeStorePostResponseFast`) is unchanged by this ADR and left
as-is.
