# ADR-0011: Per-route TTL override decoupled from upstream Cache-Control

- **Status**: Accepted
- **Date**: 2026-06-09
- **Deciders**: @thylong
- **Phase**: phase 6

## Context

Operators deploying bouine in front of a downstream CDN (e.g. Cloudflare)
need independent control over two distinct lifetimes:

1. **bouine's internal cache lifetime** — how long bouine holds an object
   before revalidating with the origin.
2. **The CDN's cache lifetime** — how long the CDN (or the browser) caches
   the response, governed by the `Cache-Control` header forwarded from bouine.

The existing `ttl_default` field (`AGENTS.md §3.1`, L3 `internal/cache`)
is a *fallback* applied only when the origin sends no explicit freshness
headers. It cannot override an explicit `max-age` or `Expires` from the
origin.

The requested feature: when an origin emits `Cache-Control: max-age=60`
and the operator wants bouine to cache for `1h` (so Cloudflare still sees
`max-age=60` and caches for 60 s on its edge), neither `ttl_default` nor
any existing directive achieves this.

## Decision

We add a `ttl_override` field to `RouteCache` (`internal/config/config.go`)
that, when set to a non-zero value:

1. **Replaces the numeric freshness lifetime** bouine stores on the cached
   object (`api.Object.TTL`) regardless of what `max-age`, `s-maxage`, or
   `Expires` the origin sends.
2. **Forwards the upstream's response headers unaltered** to downstream
   clients. The stored `api.Object.Header` retains the original
   `Cache-Control` header verbatim; only `api.Object.TTL` changes.
3. **Preserves RFC 9111 boolean semantics**: `no-store` prevents caching
   entirely (checked by `IsCacheable` before `buildObject` is called);
   `no-cache` still causes revalidation on every request; `must-revalidate`
   and `proxy-revalidate` are still enforced when bouine's (now overridden)
   TTL expires.
4. **Survives revalidation**: after a 304 Not Modified response in both the
   synchronous revalidation path (`Handler.revalidate`) and the background
   stale-while-revalidate path (`Handler.doBackgroundRevalidate`), the
   override is re-applied so the stored TTL does not revert to the origin's
   shorter `max-age`.
5. **Respects jitter**: `JitterTTL(overrideTTL, jitterPercent)` is applied
   to the override value so expiry stampedes are still prevented.

### Rejected alternative: mutating the forwarded `Cache-Control` header

Rewriting the forwarded header to match the override TTL would break the
use case: Cloudflare must see the origin's `max-age=60` so it caches at
the edge for 60 s while bouine caches for 1 h. Mutating the header would
also change the observable semantics for browsers and other downstream
caches, which is a correctness violation.

### Rejected alternative: reusing `ttl_default`

`ttl_default` semantics are "use this when the origin provides nothing".
Overloading it to also override explicit freshness would be a breaking
change and would silently alter behaviour for operators relying on
`ttl_default` as a pure fallback.

## Consequences

### Positive
- Operators can decouple bouine's storage lifetime from the CDN/browser
  lifetime with a single YAML field.
- The feature composes cleanly with `jitter_percent`, `stale_while_revalidate`,
  `stale_if_error`, and `stayin_alive`.
- Forwarded headers remain byte-for-byte identical to the origin's response,
  preserving RFC compliance for downstream caches.
- Zero cost on the hit path: override only applies in `buildObject` (miss
  fill) and in the two 304-revalidation paths; `isFresh` uses the already-
  stored `obj.TTL` with no extra branching.

### Negative / trade-offs
- An operator who sets `ttl_override` must understand that bouine and the
  downstream CDN will have different effective TTLs. The dashboard surfaces
  the override in the route config panel to make this visible.
- `no-cache` from the origin effectively bypasses the override because
  `evalNoCache` runs before `isFresh`. Operators using `ttl_override` on
  routes where the origin sends `no-cache` will see no caching benefit.
  This is documented in the field's godoc.

### Risks
- A misconfigured large `ttl_override` (e.g. `ttl_override: 365d`) will
  serve stale content for a very long time. Mitigated by: the field is opt-in
  (default 0 = disabled), jitter distributes expiry, and `make purge` / ban
  are available for emergency invalidation.

## References

- `internal/cache/handler.go` — `buildObject`, `computeTTL`, revalidation paths.
- `internal/config/config.go` — `RouteCache.TTLOverride`.
- `cmd/bouine/cmd/builder.go` — wiring `RouteCache → HandlerConfig`.
- `internal/cache/override_test.go` — test coverage for all stated invariants.
- RFC 9111 §4.2 (freshness), §5.2.2.3 (no-store), §5.2.1.4 (no-cache).
- ADR-0009 (cache state-machine hardening).
