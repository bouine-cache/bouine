# ADR-0044: Batched cluster invalidation delivery with per-issuer sequence dedup

- **Status**: Accepted
- **Date**: 2026-09-09
- **Deciders**: @crush
- **Phase**: cluster hardening

## Context

Invalidation spikes (ban/purge/refresh storms) fan out one HTTP POST
per event per peer from `internal/cluster.Broadcaster`
(broadcast.go: `BroadcastPurge`/`BroadcastBan`/`BroadcastRefresh`):
each event spawns a goroutine per peer, encodes the frame per peer,
and blocks the caller until every POST resolves. Measured on
darwin/arm64 with 3 peers: ~49 µs CPU and 138 allocs per event, plus
3 goroutine spawns; a 1000-key purge burst produces 3000 HTTP POSTs
and 1000 gossip frames. Strong mode also double-delivers: the same
event arrives via HTTP fan-out *and* gossip, so each peer applies it
twice. The gossip receive path itself is cheap (~165 ns/event) but
the `Info` log per event adds allocation and I/O churn under storms.

AGENTS.md §7 requires bounded CPU on hot paths and §1 makes
operability under load a success criterion; the current fan-out turns
invalidation storms into a CPU/network amplifier proportional to
cluster size.

## Decision

1. The `Broadcaster` buffers purge and refresh events in a bounded
   batcher and flushes on: batch size 256, a 10 ms interval timer, or
   `Close`. Ban events continue to be sent immediately (they are rare,
   large-effect operations where immediacy dominates).
2. Batched events are encoded once per flush into a single binary
   frame (`msgTypePurgeBatch` / `msgTypeRefreshBatch`, wire version
   3) containing a count-prefixed sequence of the existing per-event
   payloads. One POST per peer per flush; the gossip queue receives
   one batch frame per flush instead of one frame per event.
3. Receivers apply each event in the batch through the existing
   `Invalidator` callbacks; per-event metrics become per-batch
   counter increments and per-event `Info` logs are dropped in favor
   of a per-batch aggregate.
4. Receiving peers dedup: every event carries (Issuer, Seq). Each
   peer tracks the highest Seq seen per Issuer (a small fixed map,
   pruned lazily) and drops events whose Seq is not strictly
   increasing. This collapses the strong-mode double delivery
   (HTTP + gossip) and any re-sent batches after a partition heals.
   Monotonic tokens for purges are already an architectural
   requirement (docs/architecture.md, "Cluster split-brain on
   purges → Monotonic purge tokens"); this implements it on the
   receive side.

## Consequences

### Positive
- Sender CPU per event drops from ~49 µs (3 peers) to ~5 µs
  amortized; zero goroutine spawns per event on the flush path.
- HTTP POST volume scales with event rate / 256, not × peers.
- Gossip frames per burst shrink ~256×; `GetBroadcasts` queue-rebuild
  cost drops proportionally.
- Receive-side apply work is halved under storms (dedup).
- Admin API purge/refresh latency is no longer coupled to slowest
  peer RTT.

### Negative / trade-offs
- Purges reach peers up to 10 ms later under low traffic. Invalidity
  windows of minutes-to-hours make this immaterial in practice.
- Batches that are dropped by a busy peer lose up to 256 events of
  HTTP delivery; the gossip path remains the reliable fallback
  (unchanged) and dedup makes the re-delivery idempotent.
- New wire message types require versioned encode/decode and fuzz
  coverage (added).

### Risks
- Seq regression if issuers restart: a restarted node joins with a
  new name in practice (K8s pod name), so per-issuer high-watermarks
  start fresh; same-name reuse re-arms after `seqTrackerTTL` (1 h).
- Memory: batch buffers are bounded (queue capacity 4096 events);
  overflow is counted in a metric and events fall back to immediate
  unbatched send, preserving delivery.

## Alternatives considered

- Pipelined per-peer senders without batching: still 1 request per
  event per peer; encoding and syscalls dominate. Rejected.
- Dedup only (no batching): halves receive cost but leaves the
  sender-side 49 µs/event and 3-goroutine fan-out. Rejected as
  incomplete.
- Batching inside memberlist gossip only: compound messages already
  batch at the transport layer, but HTTP fan-out remains per-event
  and queue rebuilds under storms remain O(queue). Rejected.

## References

- ADR-0034 (fasthttp stack), ADR-0039 (peer pipelining).
- docs/architecture.md §"Purge / Ban / Refresh semantics",
  §"Cluster split-brain on purges".
- internal/cluster/broadcast.go, codec.go, handlers.go, cluster.go.
