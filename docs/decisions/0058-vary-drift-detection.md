# ADR-0058: Purge and signal on Vary-declaration drift at revalidation

- **Status**: Accepted
- **Date**: 2026-10-07
- **Deciders**: @bouine-core
- **Phase**: cache key policy
- **References**: RFC 9111 §3.2, §4.4; ADR-0046, ADR-0057

## Context

A stored object's `VaryValue` is the origin's declaration as of fill
time; every subsequent lookup, revalidate, and warm flight key derives
from it (ADR-0057 case 1). An origin that changes its Vary under a
live cache leaves the *old* surface in place until TTL: lookups
resolve requests onto variants the new surface distinguishes
(wrong-body HITs — the storage-side twin of the ADR-0057 cold-miss
bug), and 304 merges preserve the stale resolver indefinitely. Routes
that mostly revalidate (no-cache, SWR) never self-heal via a 200 fill.

## Decision

**Every revalidation compares the stored VaryValue against the fresh
response's effectiveVary union; on a field-set change the handler purges
the primary key (resolver + tracked variants, RFC 9111 §4.4) and
increments `bouine_vary_drift_total`.**

`detectVaryDrift` runs at the three revalidation sites (foreground,
background SWR, refresh-before-expiry) on both the 304 branch (via
`refreshFrom304`'s recomputed pair) and the cacheable-200 branch:

- **Field-set equality, not byte equality** — reordering or re-casing
  is the same surface; a field appearing or disappearing is drift.
- **Only a non-empty stored declaration can drift.** An empty value is
  either a genuine Vary-less declaration or a pre-v4 warm-tier blob
  that re-derives on next store.
- **The include side never triggers drift** — `h.policy` is
  route-fixed, so a detection is always an origin-side change.

The purge uses the existing `Purge` primitive: every surviving variant
was selected under the abandoned surface. The caller's normal store
path then stores the fresh response under keys derived from the *fresh*
declaration — the route re-fills on the very request that detected the
drift.

Only a good fresh response (304 or cacheable 2xx) can trigger the
signal — an origin under duress is not a declaration source, and the
stale-fallback gates keep serving the old object. This table is
ENFORCED INSIDE `detectVaryDrift` (the status gate, so no call site
can reintroduce the hole), and on the foreground path the comparison
runs inside `writeAndMaybeStore`'s cacheability gate — an uncacheable
response must not purge a live surface it will not replace. Cold
misses never run the comparison.

## Consequences

- Wrong-body HITs from a stale surface are bounded to the window
  between the origin's change and the next revalidation per key — one
  TTL, not zero. Between two revalidations, a silent origin is
  indistinguishable from an honest one.
- **The purge is a hit-ratio event**: one-cycle dip per drifted key.
  Keeping the stale resolver instead serves wrong bodies — not a trade.
- No new state, no hot-path cost: one comparison per revalidation.

## Alternatives considered

- **Re-key the resolver instead of purging** — surviving variants were
  selected under the old surface and are wrong-body candidates under
  the new one. Purging is the only fail-safe action.
- **Alert-only** — observing without removing keeps the bug and adds
  the metric.
- **Compare at lookup time** — category error: there is no fresh
  response to compare against until the origin speaks.
- **TTL-shortening for Vary-carrying objects** — a global hit-ratio
  tax to bound a rare event.
