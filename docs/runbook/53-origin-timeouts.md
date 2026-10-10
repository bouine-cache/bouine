# Origin timeouts (per route)

Which knob bounds the origin wait, how they compose, and how to give
one slow route more time than the rest of the pool. Design: ADR-0043.

## Resolution order

A route's effective origin-fetch timeout (header + body) is:

1. `routes[].cache.fetch_timeout` — explicit, per route, authoritative.
2. Else `upstream_pools[].connect.response_header_timeout` — the
   pool-wide fallback (default 30s).

There is no third fallback for pool-backed routes: the origin client
carries no client-level read cap anymore (fasthttp composes
`read deadline = min(per-request deadline, client.ReadTimeout)`, so a
client-level value silently truncated every route's `fetch_timeout` at
the pool knob).

## Knobs

| Knob | Scope | Default | Notes |
|---|---|---|---|
| `cache.fetch_timeout` | route | inherit pool knob | Bound on the total fetch; must be < 5m (data-plane safety net). Both directions honored: can be below or above the pool knob. |
| `connect.response_header_timeout` | pool | 30s | Fallback origin wait for routes without `fetch_timeout`; also bounds the never-cached `FastHandler` passthrough and active health probes use their own `health.active.timeout`. Must be < 5m. |
| `fetch_wait_timeout` | route | 100 ms | Queue wait for a `max_fetch_concurrency` slot; independent of the fetch itself (issue #562). |

## Tuning

- **One slow route** (AI generation, report export): set
  `cache.fetch_timeout: 120s` on that route only. Other routes on the
  same pool keep the pool-wide wait — no global relaxation.

```yaml
upstream_pools:
  - name: app
    targets: [app:8080]
    connect:
      response_header_timeout: 30s   # fleet default
routes:
  - name: reports
    match: { host: example.com, path_prefix: /reports }
    pool: app
    cache:
      fetch_timeout: 180s            # this route waits longer
```

- **Fleet-wide slow origins**: raise
  `connect.response_header_timeout` on the pool instead of copying the
  same `fetch_timeout` into every route.
- **SSE routes**: hinted requests (`Accept: text/event-stream`) are
  bounded by the 10-minute per-read idle budget, not these knobs (see
  runbook 52); non-hinted SSE stays bounded by the route's resolved
  fetch timeout.

## Failure modes

- **502 on a route configured with `fetch_timeout` above 30s (older
  versions)**: the pre-ADR-0043 client cap silently truncated the wait
  at `response_header_timeout`; upgrade.
- **502 at exactly the configured `fetch_timeout`**: the origin is
  slower than the operator-assigned budget — raise the route knob, or
  fix the origin.
- **Client connection reset before the fetch gives up**: a timeout was
  configured at or above the 5-minute data-plane safety net; config
  validation now rejects that for both `fetch_timeout` and
  `response_header_timeout`.
- **503 + Retry-After under load**: unrelated to the fetch bound — the
  fetch queue (`fetch_wait_timeout`) shed the request (issue #562).
