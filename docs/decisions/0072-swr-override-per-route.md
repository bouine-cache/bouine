# ADR-0072: Per-route stale-while-revalidate override

- **Status**: Accepted
- **Date**: 2026-10-30
- **Deciders**: @thylong
- **Phase**: phase 6
- **Closes**: issue #877

## Context

`RouteCache.StaleWhileRevalidate` (`stale_while_revalidate`, wired to
`HandlerConfig.DefaultSWR`) is a *default*, not an override: it is applied to
a stored object only when the origin response carries no
`stale-while-revalidate` directive (see `fillPolicy.defaultSWR` and the
store-time precedence in `buildObject`). When the origin *does* send one,
the origin's value wins and the operator knob is ignored.

That is the wrong default for origins fronted by a downstream CDN. A real
deployment hit exactly this (issue #877: a bouine tier fronting an SSR
storefront): product pages carry route meta `CDN-Cache-Control: public,
max-age=90000, stale-while-revalidate=90000` — 25h+25h, tuned for edge
caching at a downstream CDN. The operator pins storage freshness with
`ttl_override: 30s` and sets `stale_while_revalidate: 30s` expecting a
bounded stale window. Verified on a preprod deployment (v0.6.3): stale
serving was still active 120s after store — the origin's 25h directive
governed the stale window, and the route value was ignored. Steady state stays healthy (every STALE triggers a background
revalidation that restores a fresh HIT within seconds), but under a
sustained origin failure these price-bearing pages would serve stale
content for up to 25h — exactly the class of page for which the operator
wanted a 30s bound.

This is the same asymmetry `ttl_override` exists to solve for freshness
(ADR-0011): the origin's directive is deliberate (another team owns it,
and a downstream CDN relies on it), so the fix must be local to bouine.

## Decision

We add an `swr_override` field to `RouteCache`
(`internal/config/config.go`), wired to `HandlerConfig.SWROverride` and
carried on `fillPolicy.swrOverride`. When set to a non-zero value:

1. **It replaces the stale-while-revalidate window stamped onto the stored
   object**, regardless of what the origin sends. Precedence at store time
   (`buildObject`): `swr_override` > origin `stale-while-revalidate` >
   `defaultSWR` floor.
2. **Content responses only (status < 400)**, mirroring the TTL override's
   error gate: error objects keep the origin's directives and the
   negative-caching ladder (ADR-0059), so an override can never pin a
   dead error page into a wide grace window.
3. **It is NOT jittered**, unlike the TTL override. The value is an
   operator *bound* on the origin's swr; jittering it could serve past
   the configured window. There is no expiry stampede to prevent inside a
   grace window — the freshness TTL (which *is* jittered) already
   staggers the revalidation triggers.
4. **It survives revalidation**: `refreshFrom304` re-applies the override
   after merging the 304's headers, so a 304 carrying the origin's wide
   directive cannot revert the bounded window — the same guarantee
   ADR-0011 point 4 gives `ttl_override`. The full-200 revalidation paths
   rebuild the object through `buildObject` and pick the override up
   naturally.
5. **Forwarded headers are unaltered** (ADR-0011's rejected-alternative
   rationale applies): bouine never mutates the upstream
   `Cache-Control`. Under `downstream_freshness: sync` (ADR-0063), stale
   serves advertise `stale-while-revalidate=<window remaining>` computed
   from the stamped window, so the bounded value propagates downstream
   automatically.
6. **Zero (default) keeps today's semantics exactly**: origin swr wins
   when present, `stale_while_revalidate` applies when absent.

## Consequences

- Operators can bound the stale window on price-bearing (or otherwise
  correctness-sensitive) routes without touching the origin, keeping
  SWR's origin-failure resilience with a sanctioned cap.
- `stale_while_revalidate` and `swr_override` are both allowed; the
  override wins. The floor remains meaningful for origins that send no
  swr while the override is unset, and for routes where operators want a
  default but no hard bound.
- Wire types are untouched (`api.Object.StaleWhileRevalidate` already
  carries the window); peers, the fast path, the serving decisions, and
  the sync-mode advertisement all read the stamped value, so a
  store-time stamp (plus the 304 re-application) is the only change
  surface.
- Config is additive (yaml `swr_override`, validated `>= 0`); no
  compatibility break.
