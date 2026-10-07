# 55 — Request collapsing: identity-split flights and dimension-keyed cold flights

**Audience**: operators seeing origin load or hit-ratio changes after
ADR-0052 (requests carrying `Authorization` were removed from the
request-collapsing space; unsafe methods were added to the gate as
defense-in-depth), ADR-0054 (Cookie-carrying requests were added
to the gate), and ADR-0057 (cold flights are now keyed on the declared
variation dimensions, and undeclared cold misses are refused).

## What changed

Request collapsing (the singleflight dedup on the miss, revalidate,
background-refresh and shed-refill paths) parks concurrent requests for
the same cache key on one leader's in-flight origin fetch and hands the
leader's response to all of them. **A request carrying `Authorization`
or `Cookie` no longer participates**: it always performs its own origin
fetch. Anonymous requests are bit-for-bit unchanged.

The gate also denies unsafe methods (POST/PUT/DELETE, anything outside
GET/HEAD/OPTIONS). This is defense-in-depth with no behavior change:
unsafe methods never reached the collapsing paths anyway (the
dispatcher proxies them directly), but the gate now enforces that
locally, so a future dispatcher refactor cannot silently coalesce
concurrent mutations onto one origin fetch.

**Cold flights are keyed on the declared variation dimensions**
(ADR-0057). A shared flight hands the leader's response *body* to
followers, so the flight key must encode every dimension the body varies
on. On a cold miss the primary key (scheme, host, path, query, method)
alone cannot: it carries neither the route's `include_headers` dimensions
nor an origin `Vary` the cache has not yet seen. One helper now derives
the flight key at every collapsing site:

- **Warm flights** (a stored object exists, with or without Vary): the
  flight key is the lookup key, unchanged. A stored object is the
  origin's own declaration for this URL — Vary-carrying objects yield
  the variant key, and a stored object without Vary positively declares
  one body per URL (RFC 9110 §12.5.5).
- **Cold flights on an `include_headers` route**: the flight key is the
  primary key extended with the declared headers, hashed by the same
  path as the storage variant key. Same-dimension cold misses still
  collapse (the deploy/restart thundering herd keeps its dedup);
  different-dimension callers never meet on one flight.
- **Cold flights on an include-free route**: refused — one origin fetch
  per concurrent caller for the first fill of each key. The origin's
  Vary is unknowable before the first response arrives, so no shared
  key can be proven safe. The next concurrent miss is warm and collapses
  again; only the first burst per key pays.

Why the `Authorization` half exists: storage of an authorized response
is gated by RFC 9111 §3.5 (not stored unless the response is
`public`/`s-maxage`/`must-revalidate`), but collapsing is not storage —
nothing stopped one authorized caller from receiving another authorized
caller's in-flight response. This leaked cross-tenant data in
production (identical shared service JWT, tenant selected by an
undeclared custom header), and no credential-equality scheme can prove
that shape safe — so authorized flights are refused outright (ADR-0052).

Cookies carry the same argument (ADR-0054): an SSR origin renders
per-user content from the request's session cookie, so a cookied
follower parked on another user's fetch receives that user's page
in-flight. Cookied requests still *use* the cache — a stored
response is served to them per RFC 9111, and the cache-tests
`other-cookie` case keeps passing — they just never *share a live
fetch*. The cost is the same ADR-0052 trade: concurrent cookied
callers on one URL pay one origin fetch each.

## When origin load increases — and when it is expected

- **Authenticated traffic on one URL** (API gateways, service fan-outs):
  each concurrent authorized caller now costs one origin fetch.
  Previously they merged onto one. This is the intended behavior —
  those responses were never provably shareable.
- **Cookied traffic on one URL** (SSR with login sessions, A/B
  buckets): same — each concurrent cookied caller costs one origin
  fetch. Expected under the ADR-0054 extension of the gate.
- **Cold start of an include-free route** (a deploy, fleet restart, or
  ban reclaim): the first concurrent fill of each key no longer
  collapses, so a burst of concurrent cold-miss callers costs one origin
  fetch each. Expected under ADR-0057 — nothing has declared the
  variation surface yet. The next burst is warm and collapses; keep the
  window short with a proactive warmup or a purge-then-warm before
  flipping a route onto the cache.
- **Cold start of an `include_headers` route**: same-dimension callers
  still collapse onto the dimension-extended key; only the declared
  dimensions split the herd, which is exactly what the route asked for.
- The fetch semaphore bounds concurrent fetches and sheds excess
  (503 + Retry-After, or stale); a rise in `fetch_shed_total` during
  authorized, cookied, or cold-start bursts is the bound working.

## Rolling out the ADR-0057 change itself

The composite flight key is a per-process rule, not a cluster
protocol: during a rolling deploy, nodes running the old build still
key cold flights on the bare primary key. The failure mode is
fail-safe — the two builds never hand a wrong body to a follower
(their gates just use different keys, so mixed-fleet traffic splits
flights and loses dedup until the fleet converges) — but a
mixed fleet during a cold-start burst pays the ADR-0052/0054/0057
origin-load cost twice: old nodes collapse cold misses onto one
flight, new nodes collapse the same traffic per dimension (or refuse
it on include-free routes). Roll the fleet uniformly — do not run a
long-lived mixed fleet across a deploy/restart window on high-traffic
routes — and expect the one-shot cold-burst origin spike from the
section above to overlap the rollout window.

## Mitigations for authorized routes that genuinely share responses

- Have the origin mark the response shareable (`Cache-Control:
  public, s-maxage=...`). Storage then serves it to every subsequent
  request — authorized or not — as a HIT. Only the *first* concurrent
  burst misses; repeated authorized access converges to HITs.
- Front genuinely public content on routes without per-request auth so
  anonymous collapsing applies.

## Diagnosing

1. `bouine_requests_total` with `cache_result="MISS"` rises on an
   authorized route → expected under the gate; check convergence to
   HITs via storage once the origin's directives allow caching.
2. Hit ratio drop after adding `include_headers` → separate mechanism
   (storage variants), unchanged by the flight key: warm paths still
   collapse, and cold misses collapse per dimension value. A drop that
   persists past the first fill cycle is not the flight key.
3. One-shot MISS burst on an include-free route right after a deploy or
   restart → the refused cold fill, expected once per key; it converges
   on the next burst. If it persists, something is evicting the resolver
   objects — check eviction metrics, not the flight gate.

## Known-good states

- Anonymous warm traffic: unchanged, zero new cost (the lookup key is
  the flight key).
- Anonymous cold traffic on include-free routes: one origin fetch per
  concurrent caller for the first fill per key, then warm collapsing.
- Cold traffic on `include_headers` routes: collapses per declared
  dimension value — same-dimension callers share one flight.
- Repeated authorized access to an origin-shareable resource: converges
  to stored HITs; only concurrent cold bursts pay the extra fetches.

## Failure modes

| Symptom | Cause | Action |
|---|---|---|
| Origin fetches ≈ concurrent authorized callers on one URL | The gate working as designed | Accept; use origin `public, s-maxage` for convergence to HITs |
| Origin fetches ≈ concurrent cookied callers on one URL | The ADR-0054 extension of the gate working | Accept; anonymous traffic still collapses; cookied users each get their own render |
| `fetch_shed_total` rising during authorized bursts | Fetch semaphore saturated by un-collapsed authorized fetches | Raise `max_fetch_concurrency` on the route; shedded callers retry |
| `fetch_shed_total` rising during cookied bursts (SSR login storms, synchronized cookie drops) | Same bound, cookie side | Raise `max_fetch_concurrency`; consider `bypass_on_cookie` for clarity of attribution |
| One-shot cold-burst origin spike on an include-free route after deploy/restart/purge | The refused cold fill (ADR-0057) | Accept — it converges on the next burst; warm the route first if the origin cannot take the burst |
| `bouine_vary_drift_total` non-zero | An origin changed its Vary declaration under a live cache (ADR-0058); the stale resolver and its variants were purged | Confirm the origin's new declaration is intended; expect a one-cycle hit-ratio dip while the route re-fills under the new surface |
| Cross-caller response leakage still suspected in-flight | Impossible on authorized, cookied, or undeclared-cold traffic after these gates; suspect storage keying instead | Check the route's `include_headers`/Vary for an undeclared selector — storage variants share by declared dimensions |
