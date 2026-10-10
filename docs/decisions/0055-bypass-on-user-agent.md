# ADR-0055: User-Agent-conditioned cache bypass (cache.bypass_on_user_agent)

- **Status**: Accepted
- **Date**: 2026-10-07
- **Deciders**: @bouine-core
- **Phase**: cache request gating
- **References**: issue #771; ADR-0054 (bypass_on_cookie — the contract this knob mirrors); threat-model T52; `http-tests/cache-tests` (no impact: default off)

## Context and Problem Statement

bouine is deployed as a second cache layer behind a CDN edge:
`client → edge → bouine → origin`. Edge caching products commonly ship a
rule that bypasses the *edge's* cache for a specific verified-crawler
User-Agent — e.g. a merchant-center shopping-feed crawler that must
always see current product data, with spoofed UAs blocked at the edge by
bot-management + WAF rules.

The rule's intent is silently defeated by the second layer: the bypass
only stops the edge from serving its copy; the request still proxies to
bouine, which serves **its own** stored copy — stale from the crawler's
perspective. bouine had no UA-conditioned behavior at all: `RouteMatch`
supports host + path-prefix + methods, and the H1 parser exposed the UA
only for observability.

## Decision Drivers

- **Contract parity**: the natural model already exists —
  `cache.bypass_on_cookie` (ADR-0054). A UA-triggered bypass should have
  the identical never-touch-the-cache contract: no lookup, no storage,
  no in-flight sharing, `X-Cache: BYPASS` /
  `cache_result="BYPASS"` attribution.
- **Conformance**: default off; nothing changes for routes without
  patterns, so the `cache-tests` score is untouched (AGENTS.md §2.5).
- **Performance**: flag-less routes must pay effectively nothing; the
  zero-alloc hit-path gates must not move (AGENTS.md §2.4, §7).
- **Trust model**: the UA is client-controlled. A spoofable header can
  only safely *route* (fetch vs serve), never *invalidate* — bypass ≠
  purge (threat-model T52).

## Decision

Add `cache.bypass_on_user_agent: [patterns]` (`[]string`, default
empty/off) to `RouteCache`. When a request's `User-Agent` matches any
pattern:

1. **never serves from cache** — the branch runs before `lookup`, so no
   store read occurs;
2. **never stores** — the response proxies via `handleBypass`
   (streamBypass), which does not write the store;
3. **never shares an in-flight fetch** — the branch runs before the
   miss/collapse machinery (doubly: bypassed requests never reach
   singleflight at all);
4. **SSE intent wins the dispatch** — same as the cookie bypass;
5. **is attributed** `X-Cache: BYPASS` / `cache_result="BYPASS"`.

Invalidating methods (POST/PUT/DELETE) are not affected:
`serveInvalidating` runs first and keeps invalidating the shared GET
key regardless of a UA match (same ordering rationale as ADR-0054).

### Matching model

Glob/exact patterns only, no regex: `*` is the single wildcard and
matches any run of bytes **including `/`** (deliberately unlike
`path.Match`, whose `*` stops at `/` — UA strings are full of `/`).
Patterns are matched against the full UA string, ASCII-case-
insensitively; an exact pattern matches the whole string, not a
substring (`*ShoppingFeedBot*` for substring semantics). Non-ASCII
bytes compare byte-exact on both sides. Globs keep config reviewable
and mirror the edge rule's plain-substring semantics; RE2 compilation
would add cold-path cost and regex-shaped footguns for no operator
benefit.

`config.Validate` rejects: > 16 patterns, patterns > 256 bytes, empty
entries, a lone `*` (matches every request — an accidental
route-wide cache kill; operators wanting that should set
`cache.enabled: false`), adjacent `**`, unsupported glob metacharacters
(`?`, `[`, `]`, `\` — they would read as literals and silently never
match), bytes outside graphic ASCII 0x21-0x7E (space included, on
purpose: real User-Agent strings contain spaces, so an exact pattern
with one could never match — the `*Pattern*` substring form is the
supported way to match a spaced UA), and duplicates
(case-insensitive). Patterns are compiled once at handler build
(`internal/cache/uabypass.go`); the compiled matcher is nil for
pattern-less routes, and compile defensively drops any
wildcard-only pattern (lone `*`, `**`) that bypassed validation.

### Placement

`ServeRequest`: after `rewriteRequestCtx` (so `request.header_set` /
`header_remove` can adjust the UA first — the same operator escape
hatch the cookie bypass has), after the cookie-bypass branch, before
`lookup`. `FastPathHandler.TryHit`: declines (nil, false) on a match,
so the h1parser falls through to the slow path's bypass branch.
Pattern-less routes pay one nil check — no header Peek at all, so the
zero-alloc hit-path gates are unchanged (pinned by
`TestUABypass_FlagOffHitPathZeroAllocs` and the existing
`BenchmarkGate_Handler_CacheHit_ReusableWriter` budget of 0).

Cluster: no new flows — bypass happens before lookup and peer-fetch.

### route_defaults inheritance

`route_defaults.cache.bypass_on_user_agent` declares the pattern list
once for every pool route, instead of repeating it per route (the same
declare-once purpose `route_defaults.request.forwarded` serves, issue
#769). Precedence:

- a route with no list of its own inherits the default wholesale;
- a route's own list **replaces** the default (never a union) — a
  route's list is the complete pattern set, mirroring the forwarded
  token-list form;
- an explicit empty list (`bypass_on_user_agent: []`, which yaml
  decodes to a non-nil empty slice) opts the route out;
- static routes inherit nothing (the knob is pool-route-wired).

The default's patterns are validated at `route_defaults`' own path
before the merge, so an invalid default is reported once instead of
surfacing as an error on every route; the merge is skipped when the
default is invalid, and is idempotent across repeated `Validate`
calls. `route_defaults.cache` accepts only `bypass_on_user_agent`
(distinct `RouteDefaultsCache` type) — other cache fields fail strict
decoding until their merge semantics are designed, same rule as the
request half.

### Trust model (threat-model T52)

A spoofed UA merely costs an origin fetch — the same as the
`Cache-Control: no-cache` request any client can already send. Nothing
is invalidated, evicted, or poisoned; no cross-user data can be stored
or served. Origin load is the residual, bounded by the existing fetch
semaphore and shed machinery (`max_fetch_concurrency`,
`fetch_wait_timeout`). The issue's optional "trust an edge-verified
header" idea was deliberately **not** adopted: bouine cannot
distinguish an edge-injected header from a client-injected one at its
trust boundary, so a stronger signal would be spoofable by exactly the
attackers it exists to filter. A future `bypass_on_header`
generalization may follow the same per-route opt-in shape if a
deployment can arrange a genuinely unforgeable signal (e.g. mTLS
between edge and bouine).

## Consequences

- **Positive**: layered deployments can mirror edge verified-bot
  bypass rules on the inner cache, restoring the rule's freshness
  intent; default-off means zero risk to existing deployments; BYPASS
  attribution makes the crawler share of traffic measurable.
- **Negative**: matching-UA traffic on opted-in routes is always an
  origin fetch — a crawler hammering a route becomes direct origin
  load (bounded by the fetch semaphore; watch
  `cache_result="BYPASS"` share after enabling).
- **Neutral**: independent of `bypass_on_cookie`; both flags can be
  set on one route (either trigger bypasses). Varnish operators map
  `return (pass)` on `req.http.User-Agent` to this knob
  (docs/migration/varnish.md updated).
