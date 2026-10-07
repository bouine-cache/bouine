# User-Agent cache bypass (layered deployments)

Runbook for `cache.bypass_on_user_agent` (ADR-0055, issue #771). Read
[`56-cookie-bypass.md`](56-cookie-bypass.md) first — the bypass contract
(attribution, metrics, invalidating methods) is identical; only the
trigger differs (User-Agent glob match instead of Cookie presence).

## What it does

On routes with patterns configured, a request whose `User-Agent`
matches is routed entirely around the cache: no lookup, no storage, no
in-flight sharing. The response is proxied from origin with headers
preserved and attributed `X-Cache: BYPASS` / `cache_result="BYPASS"`.

```yaml
routes:
  - name: shopping-feed
    match: { path_prefix: /feed }
    pool: origin
    cache:
      bypass_on_user_agent: ["*ShoppingFeedBot*"]
```

When every route should share the same patterns, declare them once
under `route_defaults` instead of repeating them per route:

```yaml
route_defaults:
  cache:
    bypass_on_user_agent: ["*ShoppingFeedBot*"]
routes:
  - name: feed          # inherits the default list wholesale
    pool: origin
  - name: api           # replaces the default with its own list
    pool: origin
    cache:
      bypass_on_user_agent: ["*Bingbot"]
  - name: private-app   # opts out: normal cache semantics
    pool: origin
    cache:
      bypass_on_user_agent: []
```

A route's own list always **replaces** the default (never a union);
`bypass_on_user_agent: []` is the explicit opt-out; static routes
inherit nothing.

Pattern rules (enforced at config load): `*` is the only wildcard and
matches any run of bytes including `/`; an exact pattern matches the
whole UA string — use `*Pattern*` for substring semantics; matching is
case-insensitive; max 16 patterns, 256 bytes each; no lone `*`, no
`**`, no `?`/`[`/`]`/`\`, no duplicates, and only graphic ASCII bytes
0x21-0x7E — **spaces are rejected on purpose**: real User-Agent
strings contain spaces (`Mozilla/5.0 (…) et al.`), so an exact
pattern could never match one; match a spaced UA with a `*Pattern*`
substring glob instead.

## Why: edge bypass rules must be mirrored on inner caches

bouine deployed behind a CDN edge (`client → edge → bouine → origin`)
cannot see the edge's bot-management decisions. An edge rule that
bypasses the *edge's* cache for a verified crawler only stops the edge
from serving its copy; the request still reaches bouine, which serves
its own stored copy — stale from the crawler's perspective. **Any
bypass rule configured at the edge must be mirrored on every cache
layer behind it** or its freshness intent is silently defeated.

## Threat model

The User-Agent is client-controlled and spoofable (threat-model T52).
A spoofed UA merely costs an origin fetch — the same as the
`Cache-Control: no-cache` request any client can already send. Bypass
never invalidates or evicts anything (bypass ≠ purge) and never stores
or serves cross-user data. Residual: origin load from crawler-pattern
traffic, bounded by `max_fetch_concurrency` / `fetch_wait_timeout`.
Do not try to make the rule "stronger" by trusting an edge-injected
verified-bot header: at bouine's trust boundary that header is as
spoofable as the UA itself.

## Operating

| Symptom | Diagnosis | Action |
|---------|-----------|--------|
| `cache_result="BYPASS"` share spikes on a route after enabling the knob | Crawler-pattern traffic is higher than expected, or a bot is spoofing a matching UA | Check the route's BYPASS share in the dashboard; verify with the edge's bot-management logs whether the traffic is the verified crawler. Spoofed traffic costs origin fetches only — nothing is poisoned or evicted. |
| Origin load rises after enabling | Every matching request is an origin fetch by design | Confirm the crawler's fetch rate is acceptable; tune `max_fetch_concurrency` / `fetch_wait_timeout` on the route; the fetch semaphore bounds the blast radius. |
| Crawler still receives stale content | The UA does not match any pattern (exact pattern vs substring: `ShoppingFeedBot` does not match `ShoppingFeedBot/1.0` — use `ShoppingFeedBot*` or `*ShoppingFeedBot*`), or the request is served by an upper cache layer that still stores | Re-check the pattern against the crawler's actual UA string; mirror the rule on every layer between the client and bouine. |
| Config rejected at load | Pattern validation: > 16 entries, > 256 bytes, empty, lone `*`, `**`, `?`/`[`/`]`/`\`, a byte outside graphic ASCII 0x21-0x7E (space included — match a spaced UA with `*Pattern*` instead), or duplicate entries | Fix the pattern per the error message; a lone `*` is rejected on purpose — to disable caching for a route use `cache.enabled: false`. |
