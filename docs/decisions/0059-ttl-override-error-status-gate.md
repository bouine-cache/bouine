# ADR-0059: ttl_override exempts error responses (status gate)

- **Status**: Accepted
- **Date**: 2026-10-15
- **Deciders**: @thylong
- **Phase**: phase 6
- **Amends**: ADR-0011 (points 1 and 4)

## Context

ADR-0011 wrote the `ttl_override` contract for *content* responses — its
motivating example is an SSR page whose `max-age=60` a downstream CDN must
observe while bouine holds the object for `1h`. Error responses were never
considered.

Both TTL-stamping sites applied `overrideTTL` with no status check:

1. `internal/cache/handler.go` `buildObject` — unconditionally after
   `computeTTL`, discarding the ladder for every status.
2. `internal/cache/handler.go` `refreshFrom304` — re-applied on every 304
   refresh.

Meanwhile the sibling operator knob `ttl_default` is already protected:
`IsCacheableWithDefault` ends in `return isHeuristicStatus(status)` with the
stated intent "origin errors are never silently cached by an operator
default". `ttl_override` bypassed the same protection for every storable
error response — the exact class the negative-caching ladder exists to
control.

The trigger: chassis-libs-python PR #1208 added origin-side per-route
negative caching. Chassis origins emit `public, max-age=N, must-revalidate`
on opted-in error statuses and `no-store, private` elsewhere. bouine
routes configured with `ttl_override` (prod bouine-config `/content/`,
`/product-page/`: `30m`; rules-engine-board: `5s`) pinned those opted-in
errors fresh for the override duration — a 30-second negative-cache
window became 30 minutes.

## Decision

We gate the override application on status < 400. 1xx/2xx/3xx (in
practice 2xx and redirects) are "content": the override pins their
internal lifetime per ADR-0011. Status >= 400 follows the existing
`computeTTL` ladder unmodified: origin explicit freshness >
`negative_ttl` (for covered statuses) > heuristic (`Last-Modified`) >
`ttl_default`.

Both application sites gate on the response status: `buildObject` on
`res.StatusCode`, `refreshFrom304` on `refreshed.StatusCode` (the stale
object's status, preserved by `CloneForRefresh`). ADR-0011 point 4
("survives revalidation") is preserved for success responses; an error
object revalidated via 304 keeps the ladder TTL from the merged headers
or its previous TTL — never the override.

## Consequences

### Positive

- Opted-in origin negative caching (`max-age=30` on a 404) is honoured
  again: 30m → 30s on the affected prod routes.
- `ttl_default` and `ttl_override` now agree on the principle that
  operator knobs never extend error-response lifetimes beyond what the
  origin or the negative policy sanctioned.
- Zero hit-path cost: both sites run at store/refresh time only.

### Negative / trade-offs

- **Behavior change for existing operators.** Routes with both
  `ttl_override` and storable errors change stored TTLs for those errors
  (they follow the ladder instead of the override). Flagged in the
  CHANGELOG and the PR for bouine-config owners.
- **Heuristic errors can now be held longer than the override.** A 404 +
  `Last-Modified` with no explicit freshness and no `negative_ttl`
  coverage is stored for `HeuristicTTL` = 10% of (Date − Last-Modified),
  uncapped — an old Last-Modified (e.g. a year) yields ~36 days. Pre-fix
  the 30m override capped it. This is pre-existing behavior on every
  non-override route and RFC 9111 §4.2.2-permitted; accepted and
  documented, not capped.

### Risks

- `negative_ttl` vs explicit freshness precedence on errors is
  unchanged: an origin that opts in (`max-age=30` on a 404) still beats
  the route's `negative_ttl: 15s` (bouine's contract is
  origin-explicit-wins; Cloudflare differs). Non-goal of this ADR.

## Alternatives considered

- **`isHeuristicStatus(status)` as the gate** — wrong set: it includes
  404/405/410/414/501 (the bug being fixed) and excludes legitimate
  override targets such as 302/307.
- **Per-status override syntax** (nginx `proxy_cache_valid 404 1m`) — a
  real feature but a new design decision with its own precedence rules
  vs `negative_ttl`; separate ADR if ever wanted.
- **Gating inside `computeTTL`** — `overrideTTL` is not a `computeTTL`
  input; the gate is about override application, and `computeTTL`'s
  single production caller is `buildObject` anyway.
- **Capping `HeuristicTTL` on override routes** — over-engineering; see
  trade-offs.

## References

- ADR-0011 (per-route TTL override) — points 1 and 4 amended by this ADR.
- ADR-0013 (`ttl_default` no-freshness eligibility) — the `ttl_default`
  error-status protection this gate mirrors.
- RFC 9111 §4.2.2 (heuristic expiration), §4.2 (freshness).
- `internal/cache/handler.go` — `buildObject`, `refreshFrom304`.
- `internal/cache/override_test.go` — error-status gate coverage.
