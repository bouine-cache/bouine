# ADR-0031: Cap the gossip broadcast queue

- **Status**: Accepted
- **Date**: 2026-08-15
- **Deciders**: @thylong

## Context

`Cluster.QueueBroadcast` appended unconditionally to `gossipQueue` — a
plain slice with no cap. Under a purge storm (thousands of keys purged
via a ban that fans out as individual gossip broadcasts) or a slow
`GetBroadcasts` drain, the slice grew without limit. ADR-0025 cites
"production pod restarts at 150 RPS due to gossip queue overflow" as the
reason `full` mode was removed, but the unbounded queue that caused it
remained, fed by a different producer. The OOM killer would eventually
fire (issue #297).

There was no `bouine_cluster_gossip_queue_dropped_total` counter and no
queue-depth gauge, so operators had no visibility into the overflow.

## Decision

Cap `gossipQueue` at `GossipQueueDepth` (default 4096, matching
`defaultHandoffQueueDepth`). When the cap is reached, drop the incoming
message (drop-newest) and increment
`bouine_cluster_gossip_queue_dropped_total`. Expose
`bouine_cluster_gossip_queue_depth` as a gauge, updated in
`GetBroadcasts` after each drain (not on every enqueue, to avoid
lock-held gauge updates on the hot path).

### Why drop-newest, not drop-oldest

`GetBroadcasts` drains the queue FIFO. The oldest messages have been
waiting the longest and are the most likely to have already been
delivered by a previous gossip round. Discarding the newcomer is O(1)
(no memmove, no ring buffer) and preserves the messages most likely to
not yet have been delivered. Drop-oldest would require `copy(queue,
queue[1:])` — an O(n) memmove of ~96 KiB on every overflow with a 4096
cap.

### Why no per-message dedup

Each `BroadcastPurge` and `BroadcastBan` call generates a globally unique
`Seq` via `b.seq.Add(1)`. There is no re-enqueue path. A dedup map
(`map[uint64]struct{}`) would never hit and would itself grow without
bound if `GetBroadcasts` is never called (e.g. memberlist stopped, node
partitioned) — recreating the OOM bug in a different data structure.

## Consequences

- `cluster.gossip_queue_depth` config field added (default 0 = use 4096).
- `cluster.MaxGossipQueueDepth` = 1<<20; config validation mirrors it.
- A dropped-gossip counter makes queue overflow observable. In strong
  mode, HTTP fan-out is the primary invalidation path, so dropped gossip
  is a degraded-but-not-broken state. Anti-entropy repairs missed gossip.
- In eventual mode, dropped gossip means the invalidation may not reach
  peers until the next anti-entropy round. The `SendBestEffort` direct
  send still fires even when the queue is full, providing a second
  delivery attempt.
