# Plan: Host-agnostic cache keys (`cache.key.include_host: false`)

**Status:** Implemented (Phase 1 of §5 rollout — core feature, dark until a
route opts in; config flip and measurement are a bouine-config follow-up)
**Date:** 2026-09-26
**Scope:** Route-level opt-out of the Host segment in the primary cache
key, so the same URL+query serves one entry regardless of the Host the
request arrives with.
Motivated by product-page in prod-eu, where 963 URLs (20.3% of
requests) are requested under both the internal SSR host
(`doorman.doorman-prod-eu.svc.cluster.local`) and a public host
(`www.backmarket.*`), and product-page never reads the request Host —
same bytes, two entries, each fed by only part of the URL's traffic.
Depends on: ADR-0030 (128-bit key), ADR-0046 (`include_headers` union),
ADR-0049 (purge event VaryKey metadata)
Related: `docs/plans/language-normalization.md` (same route, same
metric, orthogonal axis)

---

## 1. Problem statement

bouine's primary key is `scheme|host|path|query|method`
(`internal/cache/key.go:49-71`, `BuildKey`/`BuildKeyFast`/
`buildKeyFromRaw`). Host is unconditional: `RouteKey`
(`internal/config/config.go:694-746`) has seven fields, none touch it.

On the `/product-page/` route in prod-eu, doorman forwards both
front-apps SSR calls (internal DNS host) and public-host traffic, and
its `$product_page_upstream` map (`doorman/config/maps.conf:239-243`)
is host-independent — both classes reach bouine. Measured
(40k-request sample of the doorman access log, 2026-09-26):

- 963 URL+query strings are requested under ≥2 hosts; for 20.3% of
  product-page requests the URL's traffic is split across the
  internal/public boundary.
- The minority fragment carries a median 33% of its URL's traffic.
- Within-window first-sight repeat rate on the affected URL set:
  74.2% with Host in the key vs 88.2% without — **+14.0pp on that
  set, +2.85pp route-wide** (first-sight proxy for the
  population-probability mechanism; it is a floor, not a forecast).
- Distinct keys: 16,919 → 15,691 (−7.3%) on the same sample.

The origin is provably host-blind: product-page contains zero reads
of the request `Host` (all `base_url` uses derive from `BM-Market`),
and no CORS/referrer logic references it. The duplication is pure
waste.

**Why a cache-side feature and not an upstream fix:** front-apps must
keep the internal DNS name (service discovery) and browsers must keep
the public name (CORS, cookies). No caller can make the two agree.

## 2. Design

### 2.1 Config surface

One boolean on `RouteKey`, default `true` (today's behaviour):

```yaml
- match: {path_prefix: /product-page/}
  request: {strip_prefix: /product-page}
  pool: product-page
  cache:
    ttl_override: 12h
    stale_while_revalidate: 4h
    key:
      include_host: false
```

- `include_host` is additive to the strict YAML decoder; absent =
  today's key bytes, byte-for-byte. No existing config changes keys.
- Same cluster-consistency hazard class as `include_headers`: the
  flag must be identical on every node serving the route (a mixed
  fleet splits ownership across the ring for the same logical URL;
  the ring hashes the computed key). Validation does not enforce
  this — same as `include_headers` — but the rollout gate (§5)
  requires uniform config.
- No interaction constraints with `keep_query_params`/`strip_*`/etc.
  Host is orthogonal to query policy.

### 2.2 Where it plugs in

`KeyPolicy` (`internal/cache/keypolicy.go:16-29`) is the existing
per-route, read-only, zero-alloc-on-hit carrier that already reaches
every key site. Add one field:

```go
type KeyPolicy struct {
    ...
    includeHost bool // default true; false ⇒ omit the host segment
}
```

- `NewKeyPolicy` gains the flag as a leading parameter
  (`buildKeyPolicy` in `cmd/bouine/cmd/builder.go:606` is the only
  caller in production paths). `NewKeyPolicy(nil,...)` callers in
  `cmd/bouine/cmd/engine.go` (`cacheCheck`, purge/ban/refresh ops)
  pass `true` today — see §3.3 for why admin paths keep the flag.
- `hasKeyPolicy` (`builder.go:632`) gains `!rk.IncludeHost` as a
  trigger so `include_host: false` actually allocates a policy.

Host append sites to gate (all four, plus their heap-overflow twins —
6 call sites, 3 functions):

| site | file:line |
|---|---|
| `BuildKey` stack + heap | `key.go:60`, `key.go:98` |
| `BuildKeyFast` stack + heap | `key.go:135`, `key.go:168` |
| `buildKeyFromRaw` stack + heap | `fastpath.go:755`, `fastpath.go:782` |

When `policy != nil && !policy.includeHost`, skip the
`appendCanonicalHost` call **and its following `|` delimiter**,
emitting `scheme||path|query|method`. Emitting the empty segment
(not removing the delimiter) keeps the canonical form unambiguous
against paths containing `|` and keeps the overflow-detection length
arithmetic unchanged.

The variant (Vary) key is untouched: `VariantKey*` derive from the
primary key and Vary header values only — host never enters them.
`BuildVaryKey` (`key.go:388`) is unaffected.

### 2.3 What is *not* keyed

`X-Bouine-Host` (`pkg/header/header.go:195`) is stored per-object at
`handler.go:3065` from the first fill's request and used by ban
predicates (`hot.go:938-942`) and lazy-ban path lookups
(`storage/bans.go:144`). After this change the stored value is the
host of *whichever request filled the entry*. Consequences and
decisions:

- **Ban by host regex still works** — an entry filled via the public
  host is still banable by matching that host. But a ban targeting
  `www.backmarket.fr` will not touch the entry if it was filled via
  the internal host. Mitigation: `cache-lifecycle` already bans by
  **path/URL regex and surrogate keys** for this route (surrogate
  `Cache-Tag`), which are host-independent — verify its product-page
  ban expressions cover both hosts or drop host conditions there.
  This is the one real behavioural coupling; it gets a dedicated
  test (§6.1).
- Purge-by-URL (`/v1/purge`) takes a raw URL and rebuilds the key
  with `BuildKeyFromURL(url, nil)` (`admin/server.go:530`,
  `engine.go:711`, `:734`, `:853`; `/v1/refresh` likewise;
  `/v1/cachecheck` at `engine.go:620-621` builds a default
  all-off policy instead). `nil` policy keeps the host segment, so a purge
  of `http://doorman.../product-page/...` only purges keys stored
  *with* that host — with `include_host: false` the stored keys have
  no host segment and the purge misses. **Required**: thread the
  route's policy into the purge/refresh/cachecheck paths —
  `admin.CacheCheckFn`/`PurgeFn` already close over `rs`, so the fix
  is to resolve the route matching the URL and pass its compiled
  policy (fall back to a default `include_host: true` policy for
  unmatched hosts). Alternative (cheaper, rejected for v1): document
  that host-agnostic routes must be purged via `/v1/ban` with a path
  regex. Pick the policy-threading; ban-by-path as the escape hatch
  is fine to keep.

### 2.4 Validation

- Boolean only; strict-decoder failure surfaces on typos already.
- Add one loader rule to mirror existing footguns:
  `include_host: false` requires `match.host` to be empty — a route
  that matches on host and then ignores host in the key is almost
  certainly a config error (two routes could shadow).
- Config snapshot (`/v1/config`) gains the field; `sanitizedConfig`
  (`engine.go:605`) needs no change.

## 3. Correctness traps

Ranked.

### 3.1 Wrong-body hazard is bounded by the operator, not the cache

Unlike `vary_normalize` (wrong-body on mis-mapped value collapse),
host collapse is safe **iff the upstream is host-blind**. That is a
property the operator must verify per route, and it can change
silently when the origin adds Host-based behaviour. Mitigations:

- Keep the flag **off by default**; docs must state the
  verification contract ("origin must not vary by Host: no
  redirects, no absolute URLs, no Host-keyed feature flags").
- For product-page this is verified today (§1); re-verify at rollout
  with the differential test (§6.2).
- Do **not** offer a wildcard `include_host: false` default at the
  config root. Route-level only.

### 3.2 Scheme stays in the key

`include_host: false` removes **host only**. The internal doorman
hop is plain HTTP and the public edge terminates TLS before
doorman, so both classes arrive at bouine over `http://` — scheme
already agrees on this route. Keep scheme keyed; if a future route
mixes schemes over one origin, that's a separate flag
(`include_scheme`, not in scope).

### 3.3 Admin/cluster surfaces that rebuild keys

- `/v1/cachecheck`, `/v1/purge`, `/v1/refresh` rebuild keys from raw
  URLs with `nil` policy (§2.3) — must route-resolve and use the
  route policy, else they report/act on a key that never matches a
  stored object.
- Peer fetch (`cluster/peerfetch.go`) and ownership
  (`ownerFn(lookupKey)`, `handler.go:1413-1415`) use the *computed*
  lookup key — automatically consistent across nodes as long as
  config is uniform (§2.1). The peer Vary gate
  (`handler.go:1330`) compares `BuildVaryKey` against stored
  `VaryKey` — host-independent, unaffected.
- `StoreFromPeer` (`handler.go:2413`) rebuilds nothing; stores under
  the incoming object's key — consistent by construction.

### 3.4 Cold start / config change

Changing `include_host` re-keys the route: old entries keep their
host-ful keys until TTL or purge; the new key space dual-populates
for one TTL window. Identical to ADR-0046 §Risks. Rollout order in
§5 handles it (uniform config, then ban-by-path to reclaim).

### 3.5 Ban/purge metadata (X-Bouine-Host)

Covered in §2.3. One additional wrinkle: `compileBanPredicate`
skips objects stored after the ban (`hot.go:933-935`) using
`obj.StoredAt` — unaffected by host collapse.

### 3.6 Refresh registry

Background refresh schedules against the computed key
(`triggerBgRefresh`, `handler.go:970`), so a refresh scheduled by an
internal-host request can be served/filled by a public-host request
and vice versa — exactly the desired sharing, no divergence risk
(the fetch rebuilds from the stored object's headers).

## 4. Estimate — product-page, prod-eu

Same two mechanisms as language-normalization §5, smaller magnitude:

- **Mechanism A (population):** +2.85pp route-wide first-sight
  repeats in the sample. Under the hit-probability model this is the
  dominant term; the sample under-counts long-tail URLs whose two
  fragments each individually fall below the
  one-hit-per-TTL threshold — the merged stream crosses it.
- **Mechanism B (capacity):** −7.3% distinct keys on the route,
  directly proportional store entries on a store pinned at its
  budget (prod-eu hot store, ADR/language-normalization §5.2).
  Overlaps with the PR #260 memory rebalance the same way.
- **Cost:** none measurable. `includeHost` is a bool check on a
  policy already dereferenced at every key site; the hit path is
  unchanged (policy pointer read).

**Confidence: high on direction, medium on magnitude.** The 963-URL
set and its 20.3% share are measured, not modelled; the pp estimate
is a lower bound from a 1h49m sample.

## 5. Rollout

1. **PR 1 (bouine core):** `KeyPolicy.includeHost` + 6 key-site
   gates + `NewKeyPolicy`/`hasKeyPolicy`/`buildKeyPolicy` plumbing +
   `BuildKeyFromURL`-with-policy threading in admin/engine + loader
   validation (`match.host` conflict) + full test coverage (§6.1).
   No config flips — feature dark.
2. **Chart bump** (bouine-chart): schema + `values.yaml` comment for
   `cache.key.include_host`.
3. **Config PR (this repo):** flip `include_host: false` on
   `/product-page/` in prod-eu **only**, one continent, uniform
   across its 3 pods. Expect one 12h TTL window of dual
   population; optionally `POST /v1/ban {"path_regex":
   "^/product-page/"}` after ~12h to reclaim host-ful orphans.
4. **Measure:** `bouine_request_duration_seconds_count{cache_result}`
   split on `upstream_pool=product-page`, prod-eu vs prod-us/ap
   control, 24h before/after; `bouine_hot_store_entries` delta.
   Success gate: product-page eu hit ratio ≥ +2pp, no p99
   regression on `bouine_request_duration_seconds` HIT.
5. **Fleet:** replicate to prod-us/prod-ap and preprods if green.
6. **Follow-up (separate):** re-evaluate whether
   `language-normalization` Phase 0 instrumentation
   (`bouine_vary_distinct_values`) should also sample host to keep
   this axis observable.

## 6. Testing

### 6.1 Unit / parity

- `BuildKey` vs `BuildKeyFast` vs `buildKeyFromRaw` with
  `includeHost: false`: byte-identical keys for the same
  (scheme,host,path,query,method) across differing hosts — extend
  the existing key parity tests (`key_test.go`,
  `fastpath_test.go:141`, `TestPeerVaryGateHeaderParity`
  `fastpath_test.go:2003` analog: add a host-parity case).
- Two requests differing only in Host → one stored object, both
  HIT; `X-Bouine-Host` equals the filler's host.
- Purge-by-URL resolves the route policy: purging either host form
  removes the single shared entry (guards §2.3).
- Ban-by-host-regex does **not** match an entry whose stored host
  differs from the ban target; ban-by-path and surrogate-key do.
- Loader: `include_host: false` + `match.host` set → startup error.
- Fuzz: extend `FuzzEffectiveVary`-style fuzz on `BuildKey` with
  `includeHost` both ways over the same URL corpus; assert only the
  host-segment bytes differ between the two policies.

### 6.2 Differential test (rollout gate)

In-process: same URL, two requests with the two real host values,
assert byte-identical bodies across a corpus of product-page URLs
harvested from the §1 sample (tech-specs, vr-carousel, pickers,
mobile-plan-pickers, parent-products, main product — each with the
observed query forms). Plus the origin-side re-verification: grep
product-page for Host reads (mechanical, §1) repeated at rollout
time.

### 6.3 Load

`BenchmarkBuildKey` with/without policy on the hot path; expect
noise-level delta. The flag must not allocate (bool field, not a
map/set).

## 7. Alternatives considered

- **Normalize the host instead of dropping it** (map public →
  internal): strictly more config, same wrong-body surface, and the
  mapping lives far from the origin's actual Host usage. Dropped.
- **`header_set: Host doorman...` on the route** (rewrite requests
  so the origin sees one host): changes what the origin receives —
  rejected; this plan is key-only, origin traffic stays untouched
  (also: `header_set` applies to origin-bound fetches and the key
  is built *before* the rewrite lands — it would not even work).
- **Fix at doorman** (normalize Host before proxying to bouine):
  `proxy_set_header Host $http_host` is load-bearing for other
  upstreams; a per-location override for product-page only would
  work but couples the cache-key concern to the router config and
  dies the day another cache is in front. Rejected as fragile
  layering, though noted as a viable fallback if the bouine feature
  stalls.
- **Do nothing:** −7.3% effective capacity on a capacity-bound
  store plus a permanent ~3pp hit-ratio tax on the route.
