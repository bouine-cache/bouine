# ADR-0043: Per-route origin timeout

- **Status**: Accepted
- **Date**: 2026-09-07
- **Deciders**: @theotime
- **Phase**: origin / config (extends ADR-0042's deadline work)
- **See also**: ADR-0042 (SSE idle deadlines), `internal/origin/pool.go`
  (`newOriginClient`), `cmd/bouine/cmd/builder.go`
  (`resolveRouteFetchTimeout`)

## Context

Operators asked for a per-route origin timeout: one slow endpoint (AI
generation, report exports) must be able to wait longer on its origin
than the rest of the fleet, without relaxing the wait for every other
route on the same upstream pool.

The route-level knob already existed — `routes[].cache.fetch_timeout`,
enforced verbatim on every foreground fetch via kernel connection
deadlines (`FastClient.DoDeadline`, PR #556 / v0.5.2) — but it did not
work in the upward direction. `origin.newOriginClient` copied the
pool-wide `connect.response_header_timeout` (default 30s) into the
client's `ReadTimeout`, and fasthttp composes the effective read
deadline as:

```
readDeadline = min(per-request deadline, client.ReadTimeout)
```

so any route configured with `fetch_timeout > response_header_timeout`
was silently cut at the pool-wide knob. ADR-0042 already documented
this composition for the SSE path (`min(fetch_timeout,
response_header_timeout)`); the same truncation applied to every
ordinary fetch.

## Decision

1. **Remove the client-level cap.** `newOriginClient` sets
   `ReadTimeout: 0` (unlimited) on the pool's general-purpose client.
   The per-request deadline — resolved per route — is the sole,
   authoritative origin-wait bound on every fetch. The
   `FastHandler` passthrough (never-cached requests) already passes its
   own explicit `DoTimeout(responseHeaderTimeout)` and is unaffected.

2. **Inherit, don't re-default.** `resolveRouteFetchTimeout`
   (`cmd/bouine/cmd/builder.go`) resolves each route's effective
   timeout: an explicit `cache.fetch_timeout` wins; otherwise the route
   inherits the pool's `connect.response_header_timeout` (with its
   built-in 30s default applied by `origin.NewPool`). This preserves
   the pre-existing effective bound for routes without a knob. It must
   NOT fall through to `cache.defaultFetchTimeout` (60s), which would
   silently double the historical origin wait the moment the client
   cap was removed.

3. **Validate the inherited default.** `connect.response_header_timeout`
   must now stay strictly below the 5-minute data-plane safety-net
   WriteTimeout (`config.maxFetchTimeout`) — the same rule
   `fetch_timeout` already follows — because an inherited value can no
   longer ride below a second, lower cap.

SSE fetches are unaffected: hinted requests take the pool's stream
client, whose per-read idle deadlines (ADR-0042) do not depend on
`ReadTimeout` (it is already 0 there).

## Consequences

- `fetch_timeout` becomes meaningful in both directions: shorter and
  longer than the pool knob.
- Slow-origin resource exhaustion stays bounded: every foreground
  fetch is deadline-armed by `doFastFetch`; background refreshes carry a
  ctx deadline; `max_fetch_concurrency` + `fetch_wait_timeout`
  (issue #562) cap queueing. An unbounded `Do` only remains on the
  direct `FastClient.Do` path when a handler is built without a
  resolved timeout, which the builder's inheritance now prevents for
  every pool-backed route.
- `TestPoolClient_UsesConfiguredSettings` pins that the client
  ReadTimeout stays 0 — the regression that would silently re-introduce
  the cap.
- `TestPoolFastClient_PerRequestDeadlineBeatsPoolHeaderTimeout`
  (internal/origin) and `TestResolveRouteFetchTimeout` (cmd) pin the
  capability end to end.
