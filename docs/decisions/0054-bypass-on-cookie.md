# ADR-0054: Cookie request gating — unconditional in-flight refusal, per-route cache bypass (cache.bypass_on_cookie)

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

The cookie contract has two halves with different scopes:

**In-flight refusal is unconditional** (every route, default
behavior): `collapseDenied` refuses requests carrying a Cookie header
exactly as it refuses Authorization (ADR-0052). A cookied request
never parks on — or leads — a shared singleflight: concurrent cookied
misses on one URL each perform their own origin fetch. The
ADR-0052 rationale transfers directly: "same cookie string ⇒ same
user" is an assumption a shared cache cannot verify, and an SSR
origin renders per-user content from the cookie, so a follower parked
on another user's fetch receives that user's body. This does not
affect conformance: `other-cookie` is a *sequential serve-from-store*
assertion — the cookied request there never fetches at all — and
collapsing is by definition a *concurrent fetch* behavior that the
conformance suite does not exercise. Anonymous requests keep
collapsing bit-for-bit; the cost (one fetch per concurrent cookied
caller) is bounded by the fetch semaphore and shed machinery, same
as the Authorization refusal.

**Serving/storage refusal is per-route opt-in**: add
`cache.bypass_on_cookie` (`*bool`, default nil/off) to `RouteCache`.
When enabled on a route, a request carrying any non-empty `Cookie`
header:

1. **never serves from cache** — the branch runs before `lookup`, so
   no store read occurs and no stored body (anonymous or otherwise)
   can reach the request;
2. **never stores** — the response proxies via `handleBypass`
   (streamBypass), which does not write the store;
3. **never shares an in-flight fetch** — guaranteed unconditionally
   by the in-flight half above (and doubly by this branch running
   before the miss/collapse machinery on opted-in routes);
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

- **Positive**: the in-flight leak vector is closed on **every**
  route, zero configuration, zero origin-cooperation; on opted-in
  routes the stored/served vectors close too; anonymous traffic
  keeps full cache benefit (collapsing included); the cache-tests
  `other-cookie` optimal case keeps passing (sequential
  serve-from-store, untouched by the in-flight rule).
- **Negative**: hit ratio on opted-in routes drops by the cookied
  request share — the metric (`cache_result="BYPASS"`) makes it
  visible before rollout. Cookie presence is conservative: analytics
  cookies bypass too. Unconditionally, same-URL concurrent cookied
  bursts cost one origin fetch per caller (the ADR-0052 trade,
  applied to cookies).
- **Insight safety net**: the dashboard fires
  `config-cookie-bypass-missing` when a route stores responses
  (ttl_default/ttl_override > 0) while ≥5% of its measured traffic
  carries a Cookie header and the flag is off — the personalized-SSR
  footgun becomes visible without breaking the default. The signal is
  a per-route cookied counter in the dashboard route ring
  (`RouteStat.Cookied`), not a new Prometheus label (cardinality
  rules §9).
- **Neutral**: `include_headers` remains available for routes that
  *do* want cookie-keyed variants (multi-user-safe content); the two
  knobs are independent.
- Varnish operators map `return (pass)` on `req.http.Cookie` to this
  flag (docs/migration/varnish.md updated).

## Notes

The collapse-refusal half landed **unconditional** rather than
flag-gated deliberately: the in-flight leak requires no operator
misconfiguration (any two concurrent cookied users on one URL
trigger it), the fix has no observable cost for correct traffic
(anonymous collapsing untouched, cookied requests were never
*correctly* shareable), and conformance does not exercise concurrent
fetches — so gating it per route would sell safety à la carte for a
risk that exists by default. The serving/storage half remains
per-route opt-in because refusing the *cache* for cookied requests
IS observable (hit ratio, `other-cookie` is adjacent) and trades off
against routes that genuinely cache cookie-agnostic content.

The alternative "store+collapse gate only" (anonymous cached HTML
still served to cookied users) was rejected for the flag half: it is
correct only when every personalized render carries
`private`/`no-store`, an origin invariant bouine cannot verify. It
can be added later as a separate knob without breaking this one
(config is additive per AGENTS.md §13).
