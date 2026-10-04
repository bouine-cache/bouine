# 56 — Cookie bypass: cookied requests never touch the cache on opted-in routes

**Audience**: operators deploying bouine in front of personalized SSR
(Nuxt, behind the ingress) and operators seeing hit-ratio or origin
load changes after enabling `cache.bypass_on_cookie` (ADR-0054, issue
#762).

## What changed

On routes with `cache.bypass_on_cookie: true`, a request carrying any
non-empty `Cookie` header never touches the cache:

- **never served from cache** — no store lookup; a cookied user cannot
  receive any stored body, anonymous or otherwise;
- **never stored** — the response is proxied via the bypass path,
  which does not write the store;
- **never shares an in-flight fetch** — no singleflight participation;
  each cookied request pays its own origin render.

Anonymous requests on the same route are unchanged (full MISS/HIT
semantics), and POST/PUT/DELETE still invalidates the shared GET key
(RFC 9111 §4.4) regardless of cookies. Default-off routes are
bit-identical — the cache-tests `other-cookie` case keeps passing.

## When to enable it

Enable on routes whose origin renders per-user HTML from the request
cookie (login sessions, personalized pages). The guard exists because
the other knobs do not compose into safety for that shape:

- `Set-Cookie` blocking does not fire (SSR origins read the cookie;
  they emit `Set-Cookie` once at login, not per render).
- `cache.key.include_headers: [Cookie]` keys a variant per session
  (blows past `max_variants` into silent non-caching) and does not
  stop in-flight sharing between users.
- Stale-if-error/SWR would serve one user's stored body to another.

## Cost and how to see it

- Cookied requests render at origin every time: the `BYPASS` share of
  `bouine_requests_total{cache_result="BYPASS"}` is the cost. Check it
  before/after enabling — analytics and A/B cookies count as "cookied";
  strip them at the ingress or accept the cost.
- The trigger is Cookie *presence*, not a name list (fails closed). A
  per-name trigger may be added later; the flag is additive.

## Known-good states

- Anonymous traffic: full cache benefit, one extra Cookie Peek.
- Personalized SSR: logged-in users always get their own render;
  anonymous users get cached HTML.
- SSE (`Accept: text/event-stream`) requests: unchanged — live
  stream, never cached, never collapsed.
- Dashboard insight `config-cookie-bypass-missing` gone: the route
  either has the flag on, no longer stores (`ttl_default`/
  `ttl_override` removed), or the cookied share dropped below 5%.

## Failure modes

| Symptom | Cause | Action |
|---|---|---|
| Origin render load equals cookied request share | The guard working as designed | Accept, or reduce cookie breadth at the edge (strip analytics cookies at the ingress) |
| Hit ratio dropped after enabling | Cookied share of traffic is bypassing | Check `cache_result="BYPASS"` before rollout; scope the flag to personalization-relevant path prefixes |
| Logged-in user still sees another user's page | Flag not on the matching route (router matches first host+prefix entry), or the page is served by an origin cache | `bouine cachecheck` the URL; verify the route that matched |
| `X-Cache: BYPASS` on anonymous requests | An upstream proxy/ingress injects a Cookie header | Inspect request headers at bouine; strip injected cookies at the ingress |
| Insight `config-cookie-bypass-missing` fires | The route stores responses (ttl_default/ttl_override) while ≥5% of its traffic carries cookies — the personalized-SSR leak shape | Verify the origin truly renders per-user; if yes set `bypass_on_cookie: true`, if no raise the threshold or remove ttl_default |
