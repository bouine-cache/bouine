# 59 — Cookie-value keying: cache.key.cookie_values (Cloudflare cookie.include parity)

**Audience**: operators deploying bouine in front of value-personalized
SSR (per-locale / per-currency / per-experiment renders) and operators
seeing hit-ratio or `vary_cap_hits_total` changes after enabling
`cache.key.cookie_values` (ADR-0072, issue #870).

## What changed

On routes with `cache.key.cookie_values: [name1, …]` (capped at 4
names), each listed cookie's **value** joins the Vary variant key: the
same URL + the same listed values resolves to one stored variant;
any listed-value difference resolves to its own variant with its own
body. Unlisted cookies never fragment the variant. The semantics are
the Cloudflare cache-rule `cache_key.custom_key.cookie.include`
equivalent — `cookie_presence` is `check_presence` (runbook-level
sibling of this feature, ADR-0072 has the mapping table).

```yaml
routes:
  - name: ssr-app
    cache:
      key:
        cookie_values: [locale, currency, exp_variant]
        cookie_presence: [consent]        # composes: an independent bit
```

A `route_defaults.cache.key.cookie_values` default merges like the
other list knobs: unset routes inherit it wholesale, a route's own
list replaces it (never a union), and `cookie_values: []` opts out.

## What did NOT change

- **In-flight sharing stays refused** for every cookied request on
  every route (ADR-0054): value keying stores per-value variants, it
  does not make cookied users shareable.
- **`Set-Cookie` blocking, Authorization storage rules, bypass knobs**
  are untouched.
- **Non-keyed routes pay zero** — the synthetic Vary field is absent
  from their keying (proven by the `VaryKey_NoCookieKeyingZeroCost`
  bench gate).

## Privacy

The cookie value is PII (threat-model T53): it exists only inside the
transient variant-key hash input — never stored (the stored Vary
metadata carries only the synthetic field *name*), never logged,
never in metrics labels, never in admin/API output.

## Diagnosing cardinality (the hit-ratio collapse)

Distinct values are unbounded, so a high-cardinality value (a session
ID, a tracking ID) turns the route into near-zero hit ratio with
unbounded *lookups* and refused *stores*:

1. **`bouine_vary_cap_hits_total` rising on the route** — stores past
   `cache.max_variants` (default 1024) are refused; every request for
   a past-cap value re-fetches from origin. This is the primary
   signal: the route's value cardinality exceeds its cache budget.
2. **Dashboard "too many variants" insight** — the existing Vary
   blow-up insight fires on sustained cap refusals.
3. **Origin fetch rate up, HIT ratio down** on exactly the keyed
   route — compare `cache_result` by route.

**Fix**: move the high-cardinality cookie to `cookie_presence` (a bit,
not a value), drop it from the list, or bypass on it
(`bypass_on_cookie_names`) if the content truly is per-user. A
session-ID-valued cookie in `cookie_values` is an operator error, not
a bouine bug.

## Cluster consistency (the same hazard as include_headers)

`cookie_values` must be identical on every node serving the route. A
node with a different list computes different variant keys — its peer
gates then fail **safe**: rejected exchanges surface as misses (never
a wrong body), visible as peer-fetch variant-mismatch counters. This
is the same class as `include_headers` divergence; rolling deploys
must treat the list as atomic.

## Rollback

Config reload: remove the list (or the route_defaults entry). Stored
variants key on the synthetic field and simply stop being looked up;
they age out via TTL/eviction. No data migration either way.
