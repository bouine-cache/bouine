# ADR-0009: Cache State-Machine Hardening (RFC 9111 Conformance Pass)

- **Status**: Accepted
- **Date**: 2026-05-30
- **Phase**: post-phase-4 / pre-phase-5 hardening

## Context

`make conformance` scored **87.9 % (321/365)** against
[`http-tests/cache-tests`](https://github.com/http-tests/cache-tests).
`docs/architecture.md §4` sets the phase-4 exit criterion at ≥ 84 % (307/365), which
was already met, but a targeted hardening pass was undertaken to close
the gap before the next phase begins.

Root-cause analysis of all 44 failures revealed clusters of bugs in
the cache state machine (`internal/cache/`): incorrect Age header
parsing, wrong freshness-lifetime computation when the `Date` header is
stale or missing, missing stale-on-error fallback, missing `Warning: 110`
on stale serves, and gaps in CDN-Cache-Control validation.

Because all affected decisions are observable to operators (caching
more or fewer responses, different stale-serve behaviour, Warning headers
in responses), an ADR is required per `AGENTS.md §14` and
`docs/decisions/README.md`.

## Decision

The following changes are made to `internal/cache/` together; they form
a single logical improvement to RFC 9111 conformance.

### 1. `Age` header parsing — strict integer enforcement

`parseOriginAge` now rejects `Age` values containing a decimal point
(e.g. `7200.0`). Per RFC 9111 §5.1, `Age` is `delta-seconds` = `1*DIGIT`;
a float is non-conformant and must be ignored (Age treated as absent).
Other non-digit suffixes (`7200;foo`, `7200, 0`) are tolerated via the
existing stop-at-first-non-digit parser, consistent with real-world
lenient behaviour.

### 2. Apparent age from `Date` header

`buildObject` now computes `corrected_initial_age` per RFC 9111 §4.2.3:
`max(apparent_age, age_value)` where `apparent_age = max(0, now − Date)`.
Previously, only the `Age` response header contributed to origin age.
This means that a response with `Date` set in the past (e.g. `Date: −7200`)
and no `Age` header is correctly identified as already stale.

### 3. `Expires` freshness with invalid/absent `Date`

`FreshnessLifetimeH` now falls back to `time.Now()` as the reference
time when the `Date` header is absent or syntactically invalid. RFC 9111
§4.2.1 does not require `Date` to be present before using `Expires`;
using the current time as a proxy matches common implementations.

### 4. Heuristic-stale objects served as `StaleHit`

`evalStale` now returns `StaleHit` (instead of `Revalidate`/`Miss`) for
objects cached via heuristic freshness only (no `max-age`, `s-maxage`,
or `Expires` header) when there is no `must-revalidate` or
`proxy-revalidate` directive. Per RFC 9111 §4.2.6 a cache MAY serve
stale responses at its discretion without revalidating, and
`http-tests/cache-tests` `heuristic-delta-*` tests require this.

Objects with explicit `Expires` (even if expired) or explicit CC
freshness are not covered by this rule — those go through the normal
revalidation path.

### 5. `max-stale` stale-age formula correction

`evalStale` now computes stale age as
`age − (obj.TTL + originAge)` instead of `age − obj.TTL`.

Previously `obj.TTL` was `freshness_lifetime − originAge` (remaining
freshness when stored), so subtracting it from `age = elapsed + originAge`
incorrectly counted `originAge` twice in the stale-age calculation.
The corrected formula equals `elapsed − obj.TTL`, which is
`elapsed − (freshness_lifetime − originAge)` = time since the object
expired on this cache node.

### 6. Stale-on-error fallback (stale-503, stale-close)

Two code paths are hardened:

- **`revalidate()`** — when origin returns 5xx **and** the stored
  response does not carry `must-revalidate` or `proxy-revalidate`, serve
  the stale object via `serveStale` (was: forward the 5xx or serve via
  `serveFromCache` without `Warning`).  
  For `must-revalidate` / `proxy-revalidate` responses the 5xx is still
  forwarded.

- **`ServeHTTP` Miss path** — when `evalStale` returns `Miss` (stale
  object with no validators), if the stored object has no
  must-revalidate / proxy-revalidate / no-cache / s-maxage, route
  through `fetchAndStoreStayinAlive` which serves stale on any 5xx or
  origin error (was: `fetchAndStore`, which forwards error responses).

These rules preserve `stale-close-must-revalidate`,
`stale-close-proxy-revalidate`, `stale-close-no-cache`, and
`stale-close-s-maxage=2` which correctly expect the 5xx to propagate.

### 7. `Warning: 110` on stale serves

`serveStale` now unconditionally adds `Warning: 110 - "Response is Stale"`
per RFC 7234 §5.5.3. This covers SWR, SIE, and error-fallback stale
paths.

### 8. CDN-Cache-Control: reject quoted-string directive values

`cdnCacheControl` now rejects CDN-CC header values containing `"`
(double-quote). RFC 9213 §2 requires CDN-Cache-Control directive values
to be Structured Field integers; a quoted-string (e.g. `max-age="10000"`)
is the wrong SF type and the cache MUST ignore the header, falling back
to `Cache-Control`.

Previously our `ParseCacheControl` scanner correctly unquoted the string
(`"10000"` → `10000`) before `parseDur`, meaning the header was
*accepted* with an invalid type.

### 9. `must-understand` status-code check

`isCacheBlocked` now only permits `must-understand` to override `no-store`
(RFC 9111 §5.2.2.3) when `isUnderstoodStatus(status)` is true. For unknown
status codes (e.g. 599) that the cache does not explicitly enumerate,
`no-store` still blocks caching even when `must-understand` is present.
Status 200 and all standard status codes from RFC 9110 are enumerated as
"understood".

### 10. Heuristic freshness for unknown status + `public`

`IsCacheable` allows heuristic freshness for unknown status codes when
`Cache-Control: public` is present. The `public` directive is an explicit
opt-in by the origin; combined with `Last-Modified`, the 10 %
Last-Modified heuristic is applicable regardless of status.

### 11. POST responses stored under GET key

`invalidateAndProxy` now stores cacheable POST responses (those with explicit
freshness) under the corresponding GET key after the normal invalidation, per
RFC 9111 §4.3.1. Subsequent GET requests for the same URI find the cached
POST response. Non-cacheable POST responses (no explicit freshness) only
invalidate as before.

## Consequences

### Positive

- `make conformance` score: **87.9 % → 93.2 %** (+19 tests, 321 → 340/365).
- `stale-503`, `stale-close`, `stale-warning-stored`, `stale-warning-become`,
  `heuristic-delta-5/10/30`, `ccreq-max-stale-age`, `age-parse-float`,
  `freshness-expires-invalid-date`, `freshness-max-age-date`,
  `cdn-cc-invalid-sh-type-unknown`, `cdn-cc-invalid-sh-type-wrong`,
  `status-200-must-understand`, `status-599-must-understand`,
  `heuristic-599-cached`, `method-POST` are now passing.
- Stale-on-error behaviour is now visible to operators via `Warning: 110`.
- `must-understand` is correctly scoped to understood status codes.

### Negative / trade-offs

- Heuristic-stale `StaleHit` (§4) means that truly heuristic objects
  (no Expires, no CC freshness) are served past their heuristic TTL
  without revalidation. Operators who rely on strict heuristic TTL
  enforcement should add `must-revalidate` or an explicit `max-age`
  on affected routes.
- POST now stores responses under the GET key; deep invalidation of
  POST-modified resources requires using explicit `no-store` on POST
  responses or the surrogate-key purge API.

### Risks

- The stale-on-error fallback (§6) covers all non-`must-revalidate`
  objects. If an upstream briefly flaps, clients may receive a stale
  response rather than a visible error. This is the intended
  cache-graceful-degradation behaviour, but operators should ensure
  monitoring alerts on elevated stale rates (`bouine_cache_stale_total`).

## Alternatives considered

**Strict TTL-0 for objects with Age > max-age**: instead of storing
objects with negative remaining freshness as TTL=0, store the raw
negative TTL to preserve exact staleness arithmetic. Rejected because
it requires changes to `isFresh` and several comparison operators
throughout the engine, increasing churn. The stale-age formula fix (§5)
achieves the same correct outcome without negative TTLs.

**Always `StaleHit` for no-`must-revalidate` stale objects**: serve all
stale objects (Expires-based, CC-based, and heuristic) as `StaleHit`.
Rejected because `status-*-stale` tests require that Expires-based stale
objects cause origin contact, and `freshness-max-age-stale` requires that
CC-based stale objects revalidate. The narrower heuristic-only criterion
(§4) satisfies the conformance tests without breaking those requirements.

## References

- RFC 9111 §4.2.2 (heuristic freshness), §4.2.3 (age calculation),
  §4.2.6 (serving stale), §4.3.1 (POST caching), §5.1 (Age field),
  §5.2.1.2 (max-stale), §5.2.2.2 (must-revalidate),
  §5.2.2.3 (must-understand)
- RFC 9213 §2 (CDN-Cache-Control Structured Fields)
- RFC 7234 §5.5.3 (Warning: 110)
- RFC 5861 §3–4 (stale-while-revalidate, stale-if-error)
- `docs/architecture.md §12` — conformance gate
- `AGENTS.md §2.5` — never weaken the cache-tests score
- ADR-0001 through ADR-0008 — prior decisions this extends
