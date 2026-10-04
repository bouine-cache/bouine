# ADR-0054: Per-route cookie bypass (cache.bypass_on_cookie)

- **Status**: Accepted
- **Date**: 2026-10-04
- **Deciders**: @bouine-core
- **Phase**: cache request gating
- **References**: RFC 9111 §5.2, §4; RFC 6265 §4.2; ADR-0012 (Set-Cookie storage block); ADR-0052 (collapse identity); issue #762; `http-tests/cache-tests` `other-cookie`

## Context and Problem Statement

bouine's first deployment in *front of* an SSR service (Nuxt, behind
the ingress controller) exposes a gap its previous position (caching
upstream API responses) never exercised: SSR responses are rendered
per-user from the request's session cookie, and the request side of
the cache has no cookie-awareness.

The existing guards do not compose into safety for that shape:

- **Response-side** (`Set-Cookie` blocking, ADR-0012) fails because an
  SSR origin typically sets the session cookie once at login; every
  subsequent render *reads* `Cookie` and emits **no** `Set-Cookie`.
  The blocking gate never fires, so a `Cache-Control: public, max-age=60`
  (or operator `ttl_default`) personalized page is stored.
- **Key-side** (`cache.key.include_headers`, ADR-0046) is the wrong
  tool: it keys a variant per cookie *value* — per-session variants
  blow past `max_variants` (1024) into silent non-caching — and it
  does not gate in-flight sharing: request collapsing keys on the
  primary/lookup key before variant selection, so two users'
  concurrent misses still share one leader body.
- **In-flight-side** (ADR-0052) refuses only `Authorization`, because
  credential *equality* cannot prove identity interchangeability. A
  session cookie is the same shape: "same cookie string ⇒ same user"
  is exactly the assumption a shared cache cannot verify.

The result is a two-vector leak:

1. **Stored cross-user**: user A's personalized HTML stored under a
   shared key, served to anonymous or other-user requests until TTL.
2. **In-flight cross-user**: user B parked as a singleflight follower
   on user A's fetch receives A's body.

## Decision Drivers

- **Conformance**: `http-tests/cache-tests` `other-cookie` (kind:
  `optimal`) asserts a cache *should* serve a stored fresh response
  to a `Cookie: a=b` request. RFC 9111 does not *require* refusing
  cookied requests — a shared cache MAY store and serve when the
  response directives permit. bouine passes `other-cookie` today
  (Varnish fails it); the score must not regress (AGENTS.md §2.5).
  Therefore: the guard is **per-route opt-in**, default off.
- **Conservatism**: for personalized HTML the failure mode (one user
  sees another's page) is worse than the cost (origin render per
  cookied request). Between a storage-only gate and a full pass, pass
  is the only semantics where the operator does not depend on origin
  headers being right: a cookied user can never receive *any* cached
  body, anonymous or otherwise.
- **Trigger breadth**: Cookie-header *presence*, not a name list.
  A name list is operator-maintained and silently breaks when the
  origin adds a cookie; presence is blunt (an A/B or analytics cookie
  also bypasses) but fails closed. Routes that care can strip
  irrelevant cookies at the ingress or accept the hit-ratio cost.

## Decision

Add `cache.bypass_on_cookie` (`*bool`, default nil/off) to `RouteCache`.
When enabled on a route, a request carrying any non-empty `Cookie`
header:

1. **never serves from cache** — the branch runs before `lookup`, so
   no store read occurs and no stored body (anonymous or otherwise)
   can reach the request;
2. **never stores** — the response proxies via `handleBypass`
   (streamBypass), which does not write the store;
3. **never shares an in-flight fetch** — the branch runs before the
   miss/collapse machinery, so no singleflight participation (the
   ADR-0052 refusal generalized: presence, not identity);
4. **preserves SSE semantics** — SSE-intent requests keep
   `handleSSE` (live stream, idle-bounded reads, never cached, never
   collapsed), which already satisfies the same contract;
5. **is attributed** `X-Cache: BYPASS` / `cache_result="BYPASS"` so
   the cookied share is measurable in existing metrics.

Invalidating methods (POST/PUT/DELETE) are **not** affected: they run
`serveInvalidating` first and must keep invalidating the shared GET
key regardless of cookies.

### Placement

`ServeRequest`: after `rewriteRequestCtx` (so `request.header_remove`
can strip a cookie before the check — an explicit operator escape
hatch), before `lookup` (no store read), before the decision switch
(no collapse). The flag is a plain bool field resolved at handler
build time; flag-off routes pay one `Peek` of a header fasthttp
already parsed.

`FastPathHandler.TryHit`: declines (nil, false) when the route flag
is set and `req.Header(Cookie)` is non-empty, so the h1parser falls
through to the slow path's bypass branch. Flag-off routes pay one
bool read; the header scan runs only on flag-on routes.

Cluster: no new flows — bypass happens before lookup and peer-fetch,
so a cookied request never moves cookie-bearing data across nodes.

## Consequences

- **Positive**: the leak vectors are closed for opted-in routes with
  zero origin-cooperation; anonymous traffic keeps full cache
  benefit; default routes are bit-identical (conformance preserved).
- **Negative**: hit ratio on opted-in routes drops by the cookied
  request share — the metric (`cache_result="BYPASS"`) makes it
  visible before rollout. Cookie presence is conservative: analytics
  cookies bypass too.
- **Neutral**: `include_headers` remains available for routes that
  *do* want cookie-keyed variants (multi-user-safe content); the two
  knobs are independent.
- Varnish operators map `return (pass)` on `req.http.Cookie` to this
  flag (docs/migration/varnish.md updated).

## Notes

The alternative "store+collapse gate only" (anonymous cached HTML
still served to cookied users, matching `other-cookie` more
aggressively) was rejected for this phase: it is correct only when
every personalized render carries `private`/`no-store`, an origin
invariant bouine cannot verify. It can be added later as a separate
knob without breaking this one (config is additive per
AGENTS.md §13).
