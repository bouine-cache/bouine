# ADR-0012: Block caching of Set-Cookie responses by default

- **Status**: Accepted
- **Date**: 2026-06-09
- **Deciders**: @thylong
- **Phase**: phase 6

## Context

bouine follows RFC 9111 literally: a shared cache MAY store responses
containing `Set-Cookie` when explicit freshness (`max-age` or `s-maxage`)
is present. On a subsequent cache HIT, `serveObject` replays ALL stored
headers — including `Set-Cookie` — to every client. This is a **session
fixation / data leak** vector: one user's session cookie is served to
every subsequent user.

nginx's `proxy_cache` takes the opposite, safe default: `Set-Cookie` in the
response blocks caching unconditionally, regardless of `Cache-Control`. The
operator must explicitly opt in with `proxy_ignore_headers Set-Cookie` (and
pair it with `proxy_hide_header Set-Cookie` to strip it from cached
responses). Varnish operators achieve the same safety via
`unset beresp.http.Set-Cookie` in `vcl_backend_response`.

Operators deploying bouine as a shared cache (CDN, reverse proxy in front of
an application) overwhelmingly expect the nginx-like default.

## Decision

We add `allow_set_cookie` (boolean, pointer for nil-defaults-to-false) to
`RouteCache`:

1. **Default (`nil` / `false`)**: if the origin response contains a
   `Set-Cookie` header, the response is **proxied to the current client**
   (who receives the cookie) but **not stored in the cache**. This matches
   nginx's safe default.

2. **Explicit `true`**: caching is permitted per RFC 9111 (`Set-Cookie` +
   explicit freshness), but `Set-Cookie` is **stripped from `obj.Header`
   at storage time** — after the initial MISS response has already been
   written to the first client with the full headers. Subsequent cache
   HITs do not contain `Set-Cookie`.

The check is applied in the handler (after `IsCacheable` returns true),
not in `IsCacheable` itself, to keep the pure-RFC function unchanged and
avoid breaking its existing callers.

## Consequences

### Positive
- Eliminates the session-cookie replay vulnerability by default.
- First client always receives the full origin response (including
  `Set-Cookie`) — login flows are unaffected.
- When opted in, operators get safe caching: the cookie is delivered once
  and never replayed.
- Zero cost on the cache-hit path: the check runs only at storage time
  (miss / fill), and `obj.Header.Del("Set-Cookie")` is a no-op when the
  header is absent.

### Negative / trade-offs
- **Breaking change**: operators who previously cached `Set-Cookie`
  responses (intentionally or not) will see those responses bypassing the
  cache after upgrade. They must add `allow_set_cookie: true` to restore
  the previous behaviour. The CHANGELOG must call this out prominently.
- Deviates from the RFC's permissive default (`MAY store`). Justified
  because nginx, Varnish, and CDN best-practice guides all recommend
  stripping `Set-Cookie` from cached responses; the RFC's `MAY` is a
  permission, not a recommendation.

### Risks
- An operator who sets `allow_set_cookie: true` without understanding the
  implications could still cache personalised content. Mitigated by the
  field name, godoc, and documentation explicitly warning about the
  behaviour.

## Alternatives considered

- **Modify `isBlockedBySetCookie` / `IsCacheable` directly**: rejected
  because it would change the signature of a function used by admin purge
  and conformance tests. The handler-level check is cleaner.
- **Always strip `Set-Cookie` from stored objects without a flag**: rejected
  because some operators (e.g. caching a login redirect) may intentionally
  want `Set-Cookie` in the stored response. The flag gives them control.
- **Default to `true` (RFC-compliant) and let operators opt out**: rejected
  because the vulnerability is silent and the RFC's `MAY` does not imply
  `SHOULD`. The safe default prevents harm for the 99% who don't want
  cookies cached.

## References

- `internal/cache/handler.go` — three storage paths guarded.
- `internal/config/config.go` — `RouteCache.AllowSetCookie`.
- `internal/cache/setcookie_test.go` — test coverage for all scenarios.
- nginx documentation: `proxy_ignore_headers`, `proxy_hide_header`.
- RFC 9111 §3.2 (storing responses with Set-Cookie).
- ADR-0009 (cache state-machine hardening).
