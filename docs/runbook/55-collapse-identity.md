# 55 — Request collapsing: authorized and unsafe-method requests never collapse

**Audience**: operators seeing origin load or hit-ratio changes after
ADR-0052 (requests carrying `Authorization` were removed from the
request-collapsing space; unsafe methods were added to the gate as
defense-in-depth).

## What changed

Request collapsing (the singleflight dedup on the miss, revalidate,
background-refresh and shed-refill paths) parks concurrent requests for
the same cache key on one leader's in-flight origin fetch and hands the
leader's response to all of them. **A request carrying `Authorization`
no longer participates**: it always performs its own origin fetch.
Anonymous requests are bit-for-bit unchanged.

The gate also denies unsafe methods (POST/PUT/DELETE, anything outside
GET/HEAD/OPTIONS). This is defense-in-depth with no behavior change:
unsafe methods never reached the collapsing paths anyway (the
dispatcher proxies them directly), but the gate now enforces that
locally, so a future dispatcher refactor cannot silently coalesce
concurrent mutations onto one origin fetch.

Why the `Authorization` half exists: storage of an authorized response
is gated by RFC 9111 §3.5 (not stored unless the response is
`public`/`s-maxage`/`must-revalidate`), but collapsing is not storage —
nothing stopped one authorized caller from receiving another authorized
caller's in-flight response. This leaked cross-tenant data in
production (identical shared service JWT, tenant selected by an
undeclared custom header), and no credential-equality scheme can prove
that shape safe — so authorized flights are refused outright (ADR-0052).

## When origin load increases — and when it is expected

- **Authenticated traffic on one URL** (API gateways, service fan-outs):
  each concurrent authorized caller now costs one origin fetch.
  Previously they merged onto one. This is the intended behavior —
  those responses were never provably shareable.
- The fetch semaphore bounds concurrent fetches and sheds excess
  (503 + Retry-After, or stale); a rise in `fetch_shed_total` during
  authorized bursts is the bound working.

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
   (storage variants), unchanged by this gate: anonymous callers on
   include_headers routes still collapse (pinned by test).

## Known-good states

- Anonymous traffic: unchanged, zero overhead (one header lookup on
  the already-materialized miss-path header map).
- Repeated authorized access to an origin-shareable resource: converges
  to stored HITs; only concurrent cold bursts pay the extra fetches.

## Failure modes

| Symptom | Cause | Action |
|---|---|---|
| Origin fetches ≈ concurrent authorized callers on one URL | The gate working as designed | Accept; use origin `public, s-maxage` for convergence to HITs |
| `fetch_shed_total` rising during authorized bursts | Fetch semaphore saturated by un-collapsed authorized fetches | Raise `max_fetch_concurrency` on the route; shedded callers retry |
| Cross-caller response leakage still suspected in-flight | Impossible on authorized traffic after this gate; suspect storage keying instead | Check the route's `include_headers`/Vary for an undeclared selector — storage variants share by declared dimensions |
