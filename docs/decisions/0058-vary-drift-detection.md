# ADR-0058: Purge and signal on Vary-declaration drift at revalidation

- **Status**: Accepted
- **Date**: 2026-10-07
- **Deciders**: @bouine-core
- **Phase**: cache key policy
- **References**: RFC 9110 §12.5.5, RFC 9111 §3.2, §4.1, §4.4; ADR-0046 (include_headers union); ADR-0057 (composite flight key); `internal/cache/varydrift.go`, `internal/cache/handler.go`

## Context and Problem Statement

ADR-0057 keyed the collapsing flight on the response's declared
variation dimensions. The warm half of that rule — and the storage
variant keying before it — trusts a stored object's VaryValue as the
origin's *current* declaration: the primary-key resolver entry carries
the surface the origin declared at fill time, and every subsequent
lookup, revalidate, and warm flight key is computed from it.

That trust has a lifetime problem. An origin that changes its Vary
under a live cache — a deploy that starts (or stops) varying on a
header — leaves every stored object declaring the *old* surface until
its TTL expires. Until then:

- Lookups compute variant keys under the abandoned surface: a request
  whose selector header the old surface ignores resolves onto a stored
  variant the *new* surface distinguishes — the storage-side twin of
  the cold-miss bug ADR-0057 closed, served as a wrong-body HIT, not
  just an in-flight handoff.
- Warm collapsing flights key on the old variant keys
  (`collapseFlightKey` case 1: "a stored object is the origin's own
  declaration"), so the in-flight sharing inherits the same stale
  trust.
- The 304-revalidation path preserves the stale declaration forever:
  a 304 that does not restate Vary merges the stored lines
  (RFC 9111 §3.2), and a 304 that does restate it only updates the
  revalidated variant entry — the resolver entry's surface stays
  whatever the *fill* declared when the fresh response is stored
  under a key computed from the old surface.

The 200-fill paths self-heal: `writeAndMaybeStore` re-stores the
primary resolver entry with the fresh declaration on the next full
fetch. But a route whose traffic revalidates (no-cache, SWR,
refresh-before-expiry) can carry a stale resolver far past the first
changed response — the exact window where ADR-0057's residual risk #1
lived: "a Vary-forgetting origin poisons warm flights by design".

## Decision

**Every revalidation compares the stored object's VaryValue against
the fresh response's effectiveVary union; on a field-set change the
handler purges the primary key (resolver and tracked variants,
RFC 9111 §4.4) and increments `bouine_vary_drift_total`.**

One helper, `detectVaryDrift`, runs at the three revalidation sites —
foreground `revalidate`, background SWR `doBackgroundRevalidate`,
and scheduled `doBackgroundRefresh` — on both their branches (304
merge via `refreshFrom304`'s recomputed pair, and cacheable 200). The
comparison is:

- **Field-set equality, not byte equality.** The stored value and the
  fresh union can differ cosmetically on include-free routes (the
  effectiveVary union normalization runs only when the route declares
  includes or cookie presence; the passthrough stores the origin's raw
  joined lines). Reordered or re-cased fields are the same surface; a
  field appearing or disappearing is drift.
- **Only a stored NON-EMPTY declaration can drift.** A stored empty
  VaryValue is either a genuine one-body-per-URL declaration (RFC 9110
  §12.5.5) or a pre-v4 warm-tier blob whose value re-derives on its
  next store — treating empty-vs-declared as drift would fire once
  per legacy blob, and the fresh fill re-keys the resolver anyway.
- **The include side never triggers drift.** `h.policy` is
  route-fixed, so a detection is always an origin-side change — the
  operator's config cannot move between two responses on the same
  Handler.

The purge uses the existing `Purge` primitive (resolver + tracked
variants + refresh-registry unregister), which is what an abandoned
surface demands: every surviving variant was selected under it. The
fresh response is then stored by the caller's normal store path under
keys derived from the *fresh* declaration, so the route re-fills
correctly on the very request that detected the drift.

### Where the signal runs, and does not

Only revalidations that observe a *good* fresh response (304 or
cacheable 2xx) declare a trustworthy surface. A 5xx or uncacheable
response never triggers the signal — the stale-fallback gates keep
serving the old object, as they should: an origin under duress is not
a declaration source.

Cold misses never run the comparison — there is no stored
declaration to compare against, and the first fill's store path
establishes the surface from scratch.

## Consequences

### Positive
- The ADR-0057 residual risk #1 ("warm flights trust origin honesty")
  is closed for the detectable half: the first revalidation after an
  origin changes its Vary now observes the change and removes the
  poisoned resolver, instead of propagating the stale surface until
  TTL.
- Wrong-body HITs from a stale storage surface are bounded to the
  window between the origin's change and the next revalidation of
  each key — the minimum any cache can achieve without trusting the
  origin less than its stored state.
- `bouine_vary_drift_total` gives operators the origin-change signal
  directly; the log line carries both surfaces for diffing.
- No new state, no new keying, no hot-path cost: the comparison runs
  once per revalidation (already a cold path) and is two string
  comparisons plus a split in the common no-drift case.

### Negative / trade-offs
- **The purge is a hit-ratio event.** A drift purge removes the whole
  variant set for the key; the route re-fills from origin. One-cycle
  hit-ratio dip per drifted key — the alternative (keeping the stale
  resolver) serves wrong bodies, which is not a trade.
- **The undetectable half remains.** Between two revalidations a
  silent origin is indistinguishable from an honest one; the first
  *serving* request after the origin change still resolves under the
  old surface. The detector bounds the window per key to one TTL, it
  does not eliminate it.
- **A 304-only origin never updates the resolver's surface by
  itself.** The merge updates the revalidated entry's pair; drift on
  the resolver entry is detected the same way on the next
  revalidation that reaches the primary-key entry. No additional
  action was taken for this shape: the primary entry is re-stored by
  the first 200 fill, and 304-only routes are rare enough that the
  one-TTL bound suffices.

## Alternatives considered

- **Re-key the resolver instead of purging.** Compute the fresh
  surface, store it, and let lookups transition. Rejected: the
  surviving variants were selected under the old surface and cannot
  be trusted under the new one (a variant stored for the old field
  set is a wrong-body candidate under the new). Purging is the only
  action that fails safe.
- **Alert-only (purge nothing).** Rejected: the detector exists
  because the stale state serves wrong bodies; observing without
  removing keeps the bug and adds the metric.
- **Compare at lookup time (every request against the stored
  surface).** Impossible in principle: at lookup time there is no
  fresh response to compare against — the declaration is only
  observable when the origin speaks. Rejected as category error.
- **TTL-shortening for Vary-carrying objects.** A blunt, global
  hit-ratio tax to bound a rare event; the drift detector bounds the
  same window per-key with zero steady-state cost.

## References
- RFC 9110 §12.5.5 — an origin that varies on request headers MUST
  declare it in Vary (the declaration the detector compares).
- RFC 9111 §3.2 — 304 header merging (why the comparison runs on the
  recomputed pair, not the raw 304).
- RFC 9111 §4.4 — invalidation of all stored variants (the Purge
  primitive the detector reuses).
- ADR-0046 — the include_headers union both sides of the comparison
  are computed from.
- ADR-0057 — the composite flight key whose residual risk #1 this
  closes.
