# Plan: Fix cluster `full` mode gossip overflow

## Problem

At 150 RPS with 3 bouine pods in `full` cluster mode, memberlist logs
flood with:

```
handler queue full, dropping message (8) from=10.42.0.X:8443
```

Message type 8 is `userMsg` — gossip user messages (replication events).
The queue overflow causes:

1. Replication events dropped → peers miss cached objects → cold misses
   on nodes that should have full replicas.
2. The single `packetHandler` goroutine in memberlist falls behind,
   delaying SWIM protocol messages (suspect/alive/dead), causing false
   failure detection → liveness probe timeouts → pod restarts.

## Root cause

Two compounding bugs in `QueueBroadcast` (`internal/cluster/cluster.go:347`):

### Bug 1: Dual delivery — every message sent twice

`QueueBroadcast` does both:

```go
func (c *Cluster) QueueBroadcast(msg []byte) {
    c.gossipMu.Lock()
    c.gossipQueue = append(c.gossipQueue, gossipBroadcast{data: msg})
    c.gossipMu.Unlock()
    // AND immediately sends to every peer:
    if c.ml != nil {
        for _, n := range c.ml.Members() {
            _ = c.ml.SendBestEffort(n, msg)
        }
    }
}
```

- **Path A**: `SendBestEffort` fires individual UDP messages to every
  peer immediately.
- **Path B**: `GetBroadcasts` drains `gossipQueue` on every gossip round
  and compound-messages them piggybacked on heartbeats.

Both paths deliver the same payload to the same `packetHandler` goroutine
on the receiver via the same `lowPriorityMsgQueue`. This doubles the
message volume for zero benefit — the gossip compound path already
provides reliable delivery.

### Bug 2: Single-goroutine processing bottleneck

memberlist's `packetHandler()` (`memberlist/net.go:509`) is a **single
goroutine** that processes all incoming user messages sequentially:

```go
func (m *Memberlist) packetHandler() {
    for {
        case <-m.handoffCh:
            for {
                msg, ok := m.getNextMessage()
                msgType := msg.msgType
                switch msgType {
                case userMsg:
                    m.handleUser(buf, from) // calls NotifyMsg synchronously
                }
            }
    }
}
```

`handleUser` calls `Delegate.NotifyMsg(buf)` synchronously. In bouine,
`NotifyMsg` → `handleJSONGossip` → `json.Unmarshal` + `store.Put` with
a 100ms timeout. If `store.Put` takes 5ms, the goroutine can process
~200 msgs/s.

At 150 RPS with 3 nodes in `full` mode:
- Each node handles ~50 writes/s → 50 replication events/s
- Each event is sent to 2 peers via `SendBestEffort` = 100 msgs/s received
- Each event is also delivered via gossip compound = another ~100 msgs/s
- Total: ~200 msgs/s per receiver, right at the processing ceiling

The `HandoffQueueDepth` is 1024 (memberlist default). At 200 msgs/s
with a 5ms processing time, the queue fills in ~5 seconds of sustained
traffic. Bursty fills (e.g., cache cold-start) overflow it instantly.

### Why `strong` mode is unaffected

In `strong` mode, `replicateFn` is nil — no replication events are
gossiped. Cache misses use HTTP peer-fetch (TCP, parallel, backpressured)
instead of gossip. Purge/ban events are infrequent and small (~100 bytes),
so even with dual delivery they never overflow the queue.

## Evidence

Logs from the 150 RPS load test (v0.2.1, `full` mode, 3 pods):

```
handler queue full, dropping message (8) from=10.42.0.200:8443
handler queue full, dropping message (8) from=10.42.0.201:8443
handler queue full, dropping message (8) from=10.42.0.202:8443
```

All dropped messages are type 8 (`userMsg`) from bouine peers (port 8443
= memberlist gossip port). No type-8 messages from non-bouine sources.

After switching to `strong` mode: zero "handler queue full" warnings,
zero pod restarts, stable for 3+ minutes at 150 RPS.

## Proposed solution: HTTP replication for `full` mode

### Rationale

The ADR-0008 design doc lists two mitigations for `full` mode bandwidth:
"batching replication events via memberlist compound messages" and
"backpressure: if the gossip queue exceeds a soft limit, replication
events are dropped." Neither was implemented. Even if both were
implemented, gossip is the wrong transport for full-object replication:

| Property | Gossip (UDP) | HTTP (TCP) |
|----------|-------------|------------|
| Max payload | ~1400 bytes (UDP MTU) | Unlimited (streaming) |
| Processing | Single goroutine, sequential | One goroutine per request |
| Backpressure | None (drop on queue full) | TCP flow control |
| Reliability | Best-effort | Ordered, retransmitted |
| Object size | Must fit in UDP packet or fragment | Any size |
| Existing infra | Yes | Yes (`/v1/peer/purge`, `/v1/peer/fetch`) |

Replication events carry full HTTP response bodies (1KB–1MB). Sending
these via UDP gossip is a protocol mismatch — gossip was designed for
small, infrequent membership and invalidation events, not bulk data
transfer.

The codebase already has the HTTP peer endpoint infrastructure:
`PeerPurgeHandler`, `PeerFetchHandler`, `postBinary`, mTLS peer
client. Adding a replication endpoint reuses all of this.

### Design

**Wire format**: Binary, using the existing `storage.EncodeObject` /
`storage.DecodeObject` codec — the same format already used by peer-fetch
(`PeerFetchHandler.ServeHTTP` writes `storage.EncodeObject(obj)`,
`PeerFetcher.Fetch` decodes with `storage.DecodeObject`). This avoids
JSON's overhead for large payloads:

| Issue | JSON | Binary codec |
|-------|------|-------------|
| `Body []byte` | base64-encoded (33% larger) | Raw bytes, zero-copy decode |
| `time.Time` | RFC3339 string (~35 bytes) | 15-byte fixed encoding |
| `header.Map` | Reflection-based marshal | Varint-framed key-value pairs |
| CPU | Reflection + base64 encode/decode | Direct binary read/write |

The event metadata (Issuer, Seq, IssuedAt, Method) is sent as HTTP
headers, not in the body. This keeps the body as a pure
`storage.EncodeObject` blob and allows the receiver to decode with
`storage.DecodeObject` directly — no `ReplicationEvent` JSON wrapper.

**New endpoint**: `POST /v1/peer/replicate` on the admin server.

**Sender** (`Broadcaster.BroadcastReplicate`):
- Instead of calling `cluster.QueueBroadcast(jsonBody)`, fire HTTP POSTs
  to each peer **asynchronously** — do NOT use `sync.WaitGroup`.
  `BroadcastReplicate` is called from `storeAndReplicate` on the data
  path (every cache fill). Blocking on peer ACKs would add up to 2s
  latency to cache responses. ADR-0014 explicitly rejected synchronous
  replication for this reason.
- Acquire the semaphore **non-blockingly** (via `select` with `default`)
  before launching each goroutine. If acquisition fails, drop + log +
  metric immediately without spawning a goroutine. If acquired, launch
  a goroutine that releases on completion. This avoids spawning
  goroutines that immediately block on the semaphore.
  Semaphore size: 64 (channel of `struct{}`). At 150 RPS with 3 nodes,
  average concurrent goroutines is ~5 (100 POSTs/s × 50ms each);
  64 provides headroom for slow peers without unbounded goroutine growth.
- Each goroutine:
  1. Calls `storage.EncodeObject(obj)` to produce the binary body.
  2. Sets HTTP headers: `X-Bouine-Issuer`, `X-Bouine-Seq`,
     `X-Bouine-Issued-At`, `X-Bouine-Method` (same pattern as peer-fetch's
     `X-Bouine-Hop` header).
  3. POSTs to `http://<peer.AdminAddr>/v1/peer/replicate` with
     `Content-Type: application/octet-stream`.
  4. Uses a detached `context.Background()` with a 5s timeout (not the
     request context — the request may have already returned to the client).
- Defensive copy of body/headers/surrogate keys is retained (the copy
  must outlive the request that triggered it). The copy happens before
  `EncodeObject` so the encoder reads stable data.
- Dedicated `http.Client` with `Timeout: 5s` (longer than purge's 2s —
  replication payloads can be up to 1MB).
- Metrics: `cluster_replications_sent_total` increments once per
  `BroadcastReplicate` call (preserves existing per-event semantics —
  dashboards unchanged). `cluster_replication_bytes_total{direction="sent"}`
  increments by the encoded body length per attempt (regardless of POST
  result), matching the per-attempt semantics of `sent_total`.
  `cluster_replications_dropped_total` (new) increments when the
  semaphore is full or a peer POST fails.

**Receiver** (`PeerReplicateHandler`):
- `POST /v1/peer/replicate` on the admin server.
- Wrap the request body in `http.MaxBytesReader(w, r.Body, maxReplicateBodySize)`
  where `maxReplicateBodySize` = 10MB to prevent OOM from oversized payloads.
- Read the body and decode with `storage.DecodeObject(body)` — same
  codec as peer-fetch. The decoded `Object.Body` aliases the read buffer
  (zero-copy), so `store.Put` must copy or persist it before the buffer
  is reused.
- Read event metadata from HTTP headers (`X-Bouine-Issuer`, `X-Bouine-Seq`,
  `X-Bouine-Issued-At`, `X-Bouine-Method`) for logging and metrics.
- Call `store.Put(r.Context(), obj)`. Use the request context — the
  sender's 5s HTTP timeout bounds it.
- Go's HTTP server handles concurrency (one goroutine per request), so
  50 concurrent replication POSTs/s is fine. No shared queue, no
  head-of-line blocking.
- Metrics: `cluster_replications_received_total` and
  `cluster_replication_bytes_total{direction="received"}` increment.
- Auth: same bearer token as other peer endpoints.

**Remove `SendBestEffort` from replication path**:
- `BroadcastReplicate` no longer calls `QueueBroadcast` at all.
- The `gossipQueue` path for replication is removed.
- Purge/ban keep `QueueBroadcast` (they are small and infrequent).

**Keep gossip for purge/ban in `full` mode**:
- Purge/ban events are ~100 bytes, infrequent (only on explicit
  invalidation), and benefit from gossip's epidemic propagation.
- The dual-delivery in `QueueBroadcast` is still wasteful for purge/ban,
  but the volume is low enough that it doesn't cause overflow.
- A separate cleanup of `QueueBroadcast` (removing `SendBestEffort`
  for all message types) can be done later.

### What does NOT change

- `strong` mode: no replication, unaffected.
- `eventual` mode: no replication, unaffected.
- `full` mode anti-entropy: still uses HTTP for key-set exchange and
  backfill (`GET /v1/peer/keys`, `POST /v1/peer/fetch`). Unaffected.
- `full` mode purge/ban: still uses HTTP fan-out + gossip (dual path,
  same as `strong` mode). Unaffected by this change.
- memberlist membership: still gossip-based. Unaffected.
- Ring management: still gossip-based. Unaffected.

## Implementation steps

1. **Add `PeerReplicateHandler`** in `internal/cluster/handlers.go`:
   - Read body, decode with `storage.DecodeObject(body)`.
   - Read event metadata from HTTP headers (`X-Bouine-Issuer`, etc.).
   - Call `replicator.StoreObject(ctx, obj)`.
   - Return 204 on success, 500 on store error.
   - Same bearer token auth as `PeerPurgeHandler`.
   - `MaxBytesReader` limit: 10MB.

2. **Add `sendReplicate` method** in `internal/cluster/broadcast.go`:
   - Call `storage.EncodeObject(obj)` to produce the binary body.
   - Set HTTP headers: `X-Bouine-Issuer`, `X-Bouine-Seq`,
     `X-Bouine-Issued-At`, `X-Bouine-Method`.
   - POST to `http://<peer.AdminAddr>/v1/peer/replicate` with
     `Content-Type: application/octet-stream`.
   - 5s timeout (replication payloads can be up to 1MB, longer than
     purge's ~100 bytes).
   - Same binary wire format as peer-fetch (`storage.EncodeObject`).

3. **Rewrite `BroadcastReplicate`**:
   - Remove `cluster.QueueBroadcast(body)` call.
   - Add async HTTP fan-out: one goroutine per peer, bounded by a
     semaphore (channel of `struct{}`, size 64). No `sync.WaitGroup` —
     replication must not block the data path (`storeAndReplicate`).
   - Each goroutine uses `context.Background()` with a 5s timeout (not
     the request context, which may be cancelled before the POST completes).
   - Drop + log + metric when semaphore is full or POST fails.
   - Keep defensive copy of body/headers/surrogate keys (must outlive
     the request goroutine).
   - Add `cluster_replications_dropped_total` counter metric.
   - Keep existing metrics calls (`sent_total`, `bytes_total`).

4. **Mount the handler** in `cmd/bouine/cmd/builder.go`:
   - Register `POST /v1/peer/replicate` on the admin mux.
   - Wire the `Replicator` callback (already set in `engine.go`).

5. **Remove `handleJSONGossip` replication path** in `cluster.go`:
   - Replace the `case api.GossipTypeReplication` with a warning log
     (for backward compat during rolling upgrades — old pods still
     send replication gossip).
   - After this change, `handleJSONGossip` only has the `default:` case.
     Keep the method as a clean extension point for future JSON gossip
     types; do not remove it.
   - Keep `handleBinaryGossip` for purge/ban.

6. **Update metrics**:
   - `cluster_replications_sent_total`: increment once per
     `BroadcastReplicate` call (preserves existing per-event semantics).
   - `cluster_replications_received_total`: increment in handler.
   - `cluster_replication_bytes_total`: use HTTP body length, not gossip
     message length.
   - `cluster_replications_dropped_total` (new): increment when semaphore
     is full or peer POST fails.

7. **Update docs**:
   - ADR-0008: note that replication moved from gossip to HTTP, using
     the binary `storage.EncodeObject` codec (same as peer-fetch).
   - ADR-0015: note that replication no longer uses JSON; it now uses
     the binary object codec. Purge/ban remain binary gossip; peer-fetch
     remains binary HTTP.
   - Runbook `10-cluster-modes.md`: update `full` mode section.
   - Migration guide: note that rolling upgrade from gossip-replication
     to HTTP-replication is safe (anti-entropy heals any missed objects).

## Testing strategy

### Unit tests

- `handlers_test.go`: `PeerReplicateHandler` decodes binary body with
  `storage.DecodeObject`, reads metadata from headers, calls
  `StoreObject`, returns correct status codes.
- `broadcast_test.go`: `BroadcastReplicate` sends HTTP POST to all
  peers, does not call `QueueBroadcast`, increments metrics.
- `broadcast_test.go`: `BroadcastReplicate` with nil `replicator` is a
  no-op (backward compat).
- `broadcast_test.go`: `BroadcastReplicate` sends binary body
  (`storage.EncodeObject`), not JSON. Assert `Content-Type` is
  `application/octet-stream`.
- `broadcast_test.go`: HTTP failure does not crash, logs warning,
  increments `cluster_replications_dropped_total`.
- `broadcast_test.go`: Rewrite existing `TestBroadcastReplicate_Full_EnqueuesGossip`
  to `TestBroadcastReplicate_Full_SendsHTTP` — assert HTTP POST is sent
  to all peers, `QueueBroadcast` is NOT called.

### Integration tests

- `TestFull_ReplicationViaHTTP`: Fill cache on node 0, verify object
  appears on node 1 and node 2 via HTTP replication (not gossip).
  Assert `replications_received_total` increments on peers.
- `TestFull_ReplicationBandwidthMetric`: Verify byte counters match
  HTTP body sizes, not gossip message sizes.
- `TestFull_ReplicationFailureIsolation`: One peer's admin port is
  unreachable. Verify replication to other peers succeeds, failed peer
  logs warning, no panic.
- `TestFull_GossipQueueNoOverflow`: 150 RPS sustained for 60s in `full`
  mode with 3 nodes. Assert zero "handler queue full" warnings.
  Assert `replications_received_total` ≈ `replications_sent_total` (no
  drops).

### Load test (this cluster)

- Deploy the fix to the 3-node k3s cluster.
- Switch `bouine-values.yaml` back to `mode: full`.
- Run k6 loadgen at 150 RPS for 10 minutes.
- Verify: zero pod restarts, zero "handler queue full" warnings,
  replication counters converge, cache hit rate stable.
- Compare Grafana dashboard metrics before (strong mode) and after
  (fixed full mode).

## Trade-offs

### What we lose

- **Gossip's epidemic propagation**: With HTTP fan-out, replication goes
  directly from sender to each peer (N-1 HTTP POSTs per fill). Gossip
  would propagate via epidemic fan-out (each peer forwards to others),
  reducing sender load. But with 3 nodes, N-1 = 2 — the difference is
  negligible. For larger clusters (10+ nodes), HTTP fan-out scales
  linearly with cluster size, while gossip scales logarithmically. This
  is acceptable because ADR-0008 targets `full` mode at "small clusters
  (2–5 nodes)."

- **Eventual delivery guarantee**: Gossip compound messages provide
  redundant delivery — even if one gossip round misses a peer, the next
  round catches up. HTTP fan-out is fire-and-forget (like purge in strong
  mode). If a peer's admin port is down, the replication is lost. But
  the anti-entropy reconciler (30s interval) heals any gaps, and the
  node will cache on its own miss anyway.

### What we gain

- **No handler queue overflow**: HTTP requests are handled by Go's HTTP
  server (one goroutine per request), not memberlist's single
  `packetHandler` goroutine. No shared queue, no head-of-line blocking.
- **Bounded concurrency via semaphore**: The semaphore (size 64) drops
  replications under load instead of overflowing queues. Gossip has no
  backpressure — it drops on handler-queue full, which also delays
  SWIM protocol messages and causes false failure detection.
- **No payload size limit**: HTTP can send any-size replication events.
  Gossip is limited by UDP packet size (~1400 bytes); larger messages
  need fragmentation or TCP fallback, both of which go through the same
  single-goroutine handler.
- **Simpler code**: Removes the dual-delivery complexity for replication.
  `QueueBroadcast` is only used for purge/ban, which are small and
  infrequent.

## Alternative considered: Remove `SendBestEffort` only

The simplest fix is removing the `SendBestEffort` loop from
`QueueBroadcast`, keeping gossip compound messages as the sole delivery
path.

**Pros**: One-line change, halves message volume, uses memberlist's
built-in batching.

**Cons**:
- Still processes messages in a single goroutine on the receiver. At
  higher RPS or with larger objects, the queue will still overflow.
- Adds replication latency (gossip rounds are periodic, ~500ms–1s).
- Doesn't address the payload size limitation for large objects.
- Doesn't address the lack of backpressure.

**Verdict**: Good enough as a quick mitigation, but doesn't solve the
fundamental protocol mismatch. The HTTP approach is the right fix.

## Alternative considered: Async `NotifyMsg` with worker pool

Dispatch replication events from `NotifyMsg` to a bounded worker pool
instead of processing inline.

**Pros**: Parallel processing on receiver, keeps gossip transport.

**Cons**:
- Still has UDP packet buffer as bottleneck for large objects.
- Still has dual-delivery waste.
- Adds complexity (worker pool lifecycle, ordering, error handling).
- Doesn't address the protocol mismatch.

**Verdict**: Overcomplicated half-measure. If we're adding complexity,
HTTP is the better target.

## Backward compatibility

- Rolling upgrade from gossip-replication to HTTP-replication is safe.
  During the upgrade window, expect dropped messages and 404s — these
  are fully repaired by the anti-entropy reconciler (30s interval).
  - **Old→New (gossip)**: old pods send replication via gossip. New pods
    have removed the `handleJSONGossip` replication handler and log a
    warning on receipt. The replication is dropped — anti-entropy heals.
  - **New→Old (HTTP)**: new pods send HTTP POST to `/v1/peer/replicate`.
    Old pods don't have this endpoint and return 404. New pods log a
    warning and move on. Anti-entropy heals.
  - **New→New (HTTP)**: works normally.
- Config schema: no change. `cluster.mode: full` still works.
- Metrics: same names, different transport. No dashboard changes needed.
