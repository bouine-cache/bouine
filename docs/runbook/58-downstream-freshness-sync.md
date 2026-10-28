# 58 — Downstream freshness sync (`downstream_freshness: sync`, ADR-0063)

How to cap the freshness a downstream CDN (typically Cloudflare) trusts
when bouine sits in front of it, what the caveats are per CDN, and how
to verify the cap is live.

---

## When to use this

You run `bouine → Cloudflare → users` (bouine in front of CF — e.g. CF
as an internal edge for a sub-path, or a dual-tier setup where CF fronts
bouine) and end-user content age is bounded by `bouine TTL + SWR + CF
TTL` instead of `bouine TTL + SWR + one revalidation cycle`. The
motivating case (ADR-0063): `api-stats` with TTL 1h + SWR 10m served
end users content up to ~2h10 old because CF discards bouine's `Age` on
admit and re-ages a full CF TTL.

Set per route:

```yaml
routes:
  - match: { host: api-stats.example.com }
    pool: app
    cache:
      ttl_default: 1h
      stale_while_revalidate: 10m
      downstream_freshness: sync
```

Or declare it once for every route — a route without its own
`cache.downstream_freshness` inherits the default, a route's own
value replaces it:

```yaml
route_defaults:
  cache:
    downstream_freshness: sync
```

`Cache-Control` is **never mutated** (ADR-0011's browser contract
holds); bouine synthesizes `CDN-Cache-Control` (RFC 9213), which
Cloudflare honors in preference to `Cache-Control`.

## What it does

| Serve | Synthesized `CDN-Cache-Control` |
|---|---|
| Fresh (HIT / REVALIDATED) | `public, max-age=<seconds until bouine's wall>` |
| Stored `no-cache` object, REVALIDATED | `public, no-cache` (never a numeric clock) |
| Stale, within SWR or SIE window (incl. soft purge) | `public, max-age=0, stale-while-revalidate=<window remaining>` — CF serves `UPDATING`, no visitor latency |
| Stale via stayin-alive grace / `max-stale` beyond the window / heuristic past the SWR window | `public, max-age=0` (strict) |
| Fill / MISS | `public, max-age=min(origin effective max-age, bouine final TTL)` + origin qualitative directives |

The origin's own `CDN-Cache-Control` / `Cloudflare-CDN-Cache-Control` /
`Surrogate-Control` lines are removed on sync routes (the synthesis is
authoritative). Ceiling math and the full behavior matrix: ADR-0063.

## Cloudflare caveats (the cap is void if these apply)

- **Zone Cache Rules with Edge Cache TTL** ("ignore origin cache
  control") or `set_cache_control` **override** the synthesized
  header — check zone rules before blaming bouine.
- **Always Online** zones ignore `stale-while-revalidate` /
  `stale-if-error`: the mirror degrades to strict (`max-age=0`), which
  is never unsafe — CF revalidates per request.
- Browser Cache TTL rules govern the eyeball TTL independently; the
  synthesized header targets CF's own entry, and `Cache-Control` to the
  browser is untouched.
- A 304 answering a CF revalidation carries the same cap (RFC 9111
  §4.3.4 — a 304's headers update the downstream stored entry), and a
  stale-sourced 304 also carries `Warning: 110` and triggers bouine's
  background revalidation.

## Verification

1. `curl -sD - https://api-stats.example.com/... | grep -i cdn-cache-control`
   — expect `Cdn-Cache-Control: public, max-age=<TTL − Age>` on a HIT
   and the mirror line during the SWR window.
2. Watch `CF-Cache-Status`: expect `HIT` while fresh and `UPDATING`
   during bouine's SWR window (CF serves its entry and revalidates
   asynchronously — visitor latency flat).
3. Confirm the ceiling empirically: change the origin content and
   measure the worst-case time until the last end user sees the new
   bytes — bounded by TTL + SWR + one revalidation cycle (~1h10 on
   `api-stats`), not TTL + SWR + CF TTL (~2h10).
4. `Cache-Control` must be byte-identical to the passthrough route.

## Other downstream CDNs (ADR-0063 "Downstream portability")

- **Fastly**: CDC is not in Fastly's documented TTL precedence, so the
  synthesized line is inert; the cap instead rides Fastly's native
  upstream `Age` subtraction — Fastly's clock is the **origin's**, not
  bouine's. With `ttl_override` tighter than the origin, Fastly
  overshoots by the difference. Stale serves degrade to blocking
  revalidation, never unsafe.
- **bunny.net**: CDC-family headers are not honored at all — `sync` is
  inert. Capping at bunny requires zone-side controls ("Override Cache
  Time" Edge Rules) or `Cache-Control` mutation (rejected, ADR-0011).
- Net: `sync` is never *wrong* downstream — worst case inert. A chain
  with a second CDN behind Cloudflare should treat CF as the capped hop
  and document the second hop separately.

## Rolling out / back

- Default is `passthrough` — enabling per route changes only
  client-facing headers, never status, body, `Age`, `X-Cache`, or the
  cache key. Rollout and rollback are a config reload; entries do not
  need purging.
- A soft purge under `sync` lets already-admitted CF entries survive at
  most one revalidation cycle; immediate invalidation of admitted CF
  entries requires the Cloudflare API purge integration (future work,
  ADR-0063 §9).
