# ADR-0074: `cache.enabled: false` honored on pool routes

- **Status**: Accepted
- **Date**: 2026-11-02
- **Deciders**: @chris.dupin
- **Phase**: phase 3 (cache core)
- **See also**: ADR-0054, ADR-0055, ADR-0063, ADR-0071; `serveDisabledRoute` in
  `internal/cache/handler.go`; `buildDisabledPoolRoute` in
  `cmd/bouine/cmd/builder.go`

## Context

`cache.enabled` existed in the schema but only static routes read it: the pool
builder built a full `cache.Handler` for every pool route, so `enabled: false`
validated and deployed but cached anyway — while validation advertised the knob
as the escape hatch ("set `cache.enabled: false` instead"). Live case: a
production per-user API endpoint whose origin declares `Vary: Cookie` spams
the ADR-0071 two-step-fallback warn; it must never be cached, and the
per-request knobs cannot express that.

## Decision

A pool route with `enabled: false` takes the existing per-request BYPASS path
(`serveCacheBypass` → `handleBypass`/`handleBypassFast`/`streamBypass`) with the
gate permanently open, decided once at boot (`HandlerConfig.Disabled` →
`serveDisabledRoute`). No store lookup, no store write, no peer RPC, no
in-flight sharing; `X-Cache: BYPASS` and the zero `cacheKey` are kept; origin
freshness headers pass through untouched. The route's policies (header
rewrites, strip_prefix/path_rewrite, forwarded), the SSE branch, and the fetch
knobs still apply because the bypass path already applies them. Unsafe methods
keep the RFC 9111 §4.4 invalidating dispatch (the POST-response store is
explicitly suppressed). The builder registers no fast path and skips
refresh/cluster/purge wiring; the handler is not in `rs.handlers`.

Validation rejects storage-side knobs as dead config on a disabled route
(bypass knobs, `key.*`, TTL/SWR/SIE, `negative_ttl`, `max_variants`,
`refresh_before_expiry` — so a disabled route never needs `ttl_default`); the
`route_defaults` merges for `bypass_on_user_agent`/`key.cookie_values` skip
disabled routes so inherited defaults can't trip the rejection. There is no
`route_defaults.cache.enabled`; the per-route field is the only form.

Why not the per-request knobs: they classify requests (cookie, UA, no-store) —
they can't express "never cache this route", and the prod warns persisted
through both workaround rounds. Pool/static parity: the knob now means the same
on both route kinds; nil/true keeps today's behavior bit-for-bit on both.

## Consequences

- Positive: flipping that endpoint is one config line; the ADR-0071 warns die
  at the source; BYPASS accounting is preserved, so dashboards keep working.
  Dead configs that were silently mis-deployed now fail at load.
- Trade-off: configs carrying `enabled: false` plus storage knobs must prune
  them in the same deploy. One bool read added at the top of the slow path;
  the H1 fast path registers nothing for disabled routes.

## Alternatives considered

- Per-request bypass knobs only: can't express route-level disable (rejected).
- A dedicated non-caching handler: would duplicate streamBypass/handleSSE/fetch
  shedding and drift from them (rejected).
- Diverting unsafe methods to bypass too: changes POST behavior and strands
  stored entries in the method-split-sibling shape (rejected).
- Rejecting the inert knobs too: `downstream_freshness` arrives via the
  every-route `route_defaults` merge, so rejecting it would block the flip
  (rejected).

## References

- `docs/architecture.md` §9; RFC 9111 §4.4; RFC 9213 / ADR-0063;
  the downstream config flip rides this PR's release.
