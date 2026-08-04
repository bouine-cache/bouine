# ADR-0013: ttl_default makes no-freshness responses cacheable

- **Status**: Accepted
- **Date**: 2026-06-18
- **Deciders**: @thylong
- **Phase**: phase 6

## Context

`RouteCache.TTLDefault` (`ttl_default`) is documented as the operator
fallback "used when the origin sends no explicit freshness (no max-age,
no Expires, no Last-Modified)" — see `HandlerConfig.DefaultTTL` godoc and
ADR-0011, which describes it as the fallback applied when the origin
provides nothing.

In practice the field had no effect for the very case it documents. A
real preprod deployment fronting an internal service (`internal-api-service`)
returned bare `200 OK` responses with no `Cache-Control`, `Expires`, or
`Last-Modified`. Every request returned `X-Cache: MISS`, despite
`ttl_default: 5s` (and `ttl_override: 5s`) being configured.

Root cause: storage eligibility is gated by `IsCacheable()`
(RFC 9111, `internal/cache/cacheable.go`) in the miss-fill paths
(`writeAndMaybeStore`, `fetchAndStoreStayinAlive`) *before* `buildObject`
/ `computeTTL` run. For a header-less `200`, `IsCacheable` returns false
(no explicit freshness, no `Last-Modified` for heuristic freshness, not
negative-cacheable). The `defaultTTL` fallback inside `computeTTL`
(`if !explicit && ttl == 0 && defaultTTL > 0 { ttl = defaultTTL }`) was
therefore unreachable — dead code for its documented purpose.

`ttl_override` is unaffected: its contract (ADR-0011) is narrowly to
replace the numeric lifetime of an already-eligible response and to
respect RFC boolean directives. It works as specified and is not a
"force-cache" knob.

## Decision

Introduce `IsCacheableWithDefault(status, reqHeader, respHeader,
negativeTTL, defaultTTL)` in `internal/cache/cacheable.go` and use it in
the two GET miss-fill paths. When the strict RFC decision is "not
cacheable" *and* `defaultTTL > 0`, a response becomes eligible iff:

1. it is not blocked by `isCacheBlocked` — i.e. no `no-store`, no
   `private`, no `Pragma: no-cache`, no `Vary: *`, no `Set-Cookie`
   without explicit freshness, and no `Authorization` without
   `public`/`must-revalidate`/`s-maxage`; and
2. its status is heuristically cacheable (`isHeuristicStatus`) — 5xx and
   other error statuses are excluded so origin errors are never silently
   cached by an operator default.

The stored object's TTL is then `defaultTTL` (via the existing
`computeTTL` fallback), subject to jitter and origin-age adjustment.

### Scope boundaries

- The POST→GET store path (`invalidateAndProxy`) keeps strict
  `IsCacheable`: RFC 9111 requires explicit freshness to cache a POST
  response; a default TTL must not relax that.
- `ttl_override` semantics are unchanged.
- Default behaviour without `ttl_default` is unchanged: a header-less
  response remains a perpetual MISS (regression-guarded by test).

### Rejected alternative: overload `ttl_override`

`ttl_override` is documented to honour eligibility and only replace the
numeric lifetime. Making it force-cache uncacheable responses would
break its CDN-decoupling contract (ADR-0011) and silently change
behaviour for existing operators.

### Rejected alternative: relax `IsCacheable` globally

Caching no-freshness responses by default would violate RFC 9111's
conservative stance and regress the cache-tests conformance score. The
new behaviour is strictly opt-in via `ttl_default > 0`.

## Consequences

### Positive
- `ttl_default` now does what it documents: origins that emit no cache
  headers can be cached for an operator-chosen lifetime (nginx
  `proxy_cache_valid` parity).
- Opt-in and blocking-directive-safe; no behaviour change when
  `ttl_default` is unset.

### Negative / trade-offs
- Operators enabling `ttl_default` on routes whose origin intentionally
  omits headers to signal "do not cache" (relying on the previous
  accidental behaviour) will now cache. This is the documented contract,
  and `no-store`/`private`/`no-cache` remain the correct signals to
  prevent caching.

### Conformance
- `http-tests/cache-tests` is unaffected: the harness exercises explicit
  RFC directives, and the fallback only engages when `defaultTTL > 0`,
  which the conformance config does not set.

## References

- `internal/cache/cacheable.go` — `IsCacheableWithDefault`, `isCacheBlocked`.
- `internal/cache/handler.go` — `writeAndMaybeStore`,
  `fetchAndStoreStayinAlive`, `computeTTL`.
- `internal/cache/default_ttl_test.go` — eligibility + end-to-end tests.
- `internal/config/config.go` — `RouteCache.TTLDefault` godoc.
- ADR-0011 (per-route `ttl_override`), ADR-0009 (state-machine hardening).
- RFC 9111 §4.2 (freshness), §5.2 (Cache-Control directives).
