# 60 — Experimental peer near-cache (ADR-0071)

Operating the `experimental.near_cache` flag: the node-local store of
peer-fetched objects on non-owner nodes.

---

## What it does

In strong cluster mode, a request landing on a node that does not own
its key pays a peer-fetch RPC to the owner on every miss. With the
flag on, a non-owner keeps a small local copy of peer answers for
keys that prove popular (a second peer fetch within the 30 s
admission window) and serves it fresh-only, at local-hit latency,
until the copy's TTL lapses or an invalidation deletes it.

- Served responses carry `X-Cache: HIT` and `X-Cache-Source: near`.
- Near entries never serve stale and never trigger revalidation —
  revalidation stays the owner's job.
- Variant (Vary-bearing) objects are never admitted (v1).

## Configuration

```yaml
experimental:
  near_cache:
    enabled: true
    routes: ["/api", "/ssr"]        # allowlist; empty = nowhere (fail-closed)
    max_bytes: 64MiB   # per node, default 16MiB, validation cap 256MiB
    max_ttl: 10s       # staleness bound, default 10s, validation cap 5m
```

`routes` is the allowlist of route names the feature may act on
(explicit `name:` or the derived host:path / path label — the same
names as the `route` metric label). Only listed routes get the near
branch; every other route keeps paying peer RPCs exactly as before.
An empty list is valid and selects **nowhere** — the engine logs a
warning — and a name that matches no route fails config validation, so
a typo cannot silently de-scope a selected route. Keep routes whose
cache key omits request-identity headers out of the allowlist: a near
copy pins one peer answer locally for `max_ttl`.

`max_ttl` is the bound that holds **even if invalidation propagation
fails entirely**: a near copy can never outlive it. The default is
10 s — on a measured production fleet's key distribution a 5–10 s bound keeps
~2/3 of the absorption a 30 s bound buys, at a third of the staleness
exposure. Size it to the staleness the rest of the fleet may accept
from this feature.

The feature only acts in strong cluster mode: an enabled-but-inert
config (no cluster) logs a warning at startup.

## Metrics

| Metric | Meaning |
|---|---|
| `bouine_near_cache_hits_total` | near serves (fresh entries only) |
| `bouine_near_cache_admissions_total` | objects stored after the guards |
| `bouine_near_cache_invalidations_total` | refresh/ban-driven removals (purges delete silently, like every other surface) |

Near hits also appear in `bouine_requests_total` /
`bouine_response_bytes_total` with `source="near"`.

## Operator actions

- **Sustained non-zero `invalidations` rate**: invalidation traffic
  is fighting near-cache reuse (each refresh/ban deletes the copies it
  touches). Expected during invalidation storms; investigate only if
  it stays elevated at steady state.
- **Suspected stale content from a near copy**: check
  `source="near"` requests in the window, then `max_ttl` — every near
  copy dies by itself at the latest at its TTL bound, so a restart of
  the pod also clears all near state (it is memory-only).
- **Memory pressure**: near bytes are bounded by `max_bytes`; the
  store is SIEVE-evicted, so it self-shrinks under pressure. Check
  RSS against `max_bytes` — it should be a rounding error next to the
  hot tier.
- **Disabling the flag**: remove the `experimental.near_cache` block
  and restart; existing near entries are memory-only and vanish.

## Known limitations (v1)

- Variant (Vary) routes keep paying peer RPCs; near admission
  refuses them by design (ADR-0071).
- Near entries are keyed by the route's policy key: a purge built
  under a different route's key space (URL purge on a
  header-policied route) misses them; they expire at `max_ttl`
  instead. Surrogate-key, host and path bans do reach them (bans
  evaluate on Get against the object's own metadata).
- A route NOT in the `routes` allowlist has no near branch at all —
  including its fast path.
