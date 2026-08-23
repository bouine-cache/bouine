# ADR-0039: HTTP/1.1 pipelining for peer fetch

Date: 2026-08-23

## Status

Accepted — implemented in Phase 6.4 (issue #521, PR #524).

## Context

The fasthttp migration (ADR-0034) replaced `net/http` with `fasthttp`
for the data plane. The peer fetch layer (`internal/cluster/peerfetch.go`)
used `fasthttp.Client` with `MaxConnsPerHost=256` to handle concurrent
fetches to each peer. This provided high throughput but at a significant
memory cost: 256 connections per peer at ~50 KiB each = ~12.8 MiB per
peer in connection buffers alone.

HTTP/2 multiplexing was the original plan for peer fetch (multiple
requests over a single connection), but fasthttp does not support HTTP/2
(ADR-0034). HTTP/1.1 pipelining is the fasthttp-native alternative:
`fasthttp.PipelineClient` sends multiple requests over a small number
of persistent connections without waiting for each response.

The issue (#521) estimated pipelining would reduce per-peer connection
memory from ~12.8 MiB (256 connections × 50 KiB) to ~400 KiB (8
connections × 50 KiB), an ~97% reduction.

## Decision

Replace `fasthttp.Client` with `fasthttp.PipelineClient` for peer
fetch. Each peer address gets its own `*fasthttp.PipelineClient` cached
in a `sync.Map`, created lazily via `LoadOrStore`.

### Configuration

Two new config fields under `cluster:`:

- `peer_max_conns_per_host` (default 8) — maximum pipelined connections
  per peer address.
- `peer_max_idle_conn_duration` (default 120s) — idle timeout for
  peer connections.

### Pipeline depth

`fasthttp.PipelineClient` uses `MaxPendingRequests=16` (hardcoded as
`peerMaxPendingRequests`). With 8 connections × 16 pending = 128
concurrent fetches per peer, matching the effective concurrency of the
previous HTTP/2 plan.

### Broadcast separation

The `Broadcaster` (fire-and-forget invalidation notifications) creates
its own standalone `fasthttp.Client` instead of sharing the fetcher's
client. This is correct because broadcast sends one request per peer
(no pipelining benefit) and the fetcher no longer owns a single shared
client. TLS configuration is inherited from the fetcher.

### Transport helper

`transport.PipelineDo(ctx, c *fasthttp.PipelineClient, req, resp)` mirrors
`transport.Client.Do` but for `PipelineClient`. It uses `DoDeadline`
when the context has a deadline, `DoTimeout` with a 60s default
otherwise.

## Consequences

- Per-peer connection memory reduced from ~12.8 MiB (256 conns × 50 KiB)
  to ~400 KiB (8 conns × 50 KiB), a ~97% reduction.
- 128 concurrent fetches per peer (8 conns × 16 pending) matches the
  previous HTTP/2 capacity target.
- `PipelineClient` has no `CloseIdleConnections()` method — cleanup is
  dropping references. The `Close` method iterates the `sync.Map` and
  nils out entries.
- `fasthttp.Server.Shutdown()` waits for keep-alive connections to go
  idle. Test servers must set `IdleTimeout` (e.g., 50 ms) to avoid
  120-second hangs in test teardown.
- Broadcast traffic uses a separate `fasthttp.Client` with its own
  connection pool, isolated from the pipelined fetch connections.
- Config validation enforces `PeerMaxConnsPerHost >= 0` and
  `PeerMaxIdleConnDuration >= 0`.
