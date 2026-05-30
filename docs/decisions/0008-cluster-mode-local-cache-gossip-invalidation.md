# ADR-0008: Cluster consistency modes — strong, eventual, full

- **Status**: Accepted
- **Date**: 2026-05-29
- **Deciders**: @thylong
- **Phase**: phase 4.5 (hardening)

## Context

The current cluster mode shards cache keys across nodes using a consistent
hash ring. Each key has exactly one owner node; non-owners forward misses
to the owner via peer-fetch before falling through to origin. This
maximises memory efficiency (one copy per key in the cluster) but
introduces:

- Latency on peer-fetch misses (extra RTT before origin fallback).
- A single point of failure per key set (owner down → cold miss).
- Complexity: ring digest reconciliation, bounded-load balancing,
  hop-limit guards.

Some deployments prefer **eventual consistency** with **lower miss
latency** and **higher availability** at the cost of **redundant
storage**: every node caches whatever it receives from origin, and
only invalidation signals (ban/purge/refresh) are propagated via
gossip. This is the model used by CDN edge nodes where each PoP is
independent.

Others need **full replication**: every node holds a copy of every
cached object, maximising hit rate and resilience at the cost of N×
memory. Invalidations still propagate via gossip for consistency.

## Decision

**Introduce a `cluster.mode` configuration field with three values:**

| Mode | YAML value | Key routing | Replication | Invalidation propagation | Consistency |
|------|-----------|-------------|-------------|--------------------------|-------------|
| Strong (current) | `strong` | Ring owns key → peer fetch on miss | 1 copy (owner only) | HTTP fan-out + gossip (dual path) | Strong after invalidation ACK |
| Eventual | `eventual` | Every node stores locally; no peer fetch | N copies (each node caches independently) | Gossip only | Eventual (convergence window 1–5 s) |
| Full | `full` | Every node stores locally; no peer fetch | N copies (active replication on fill) | Gossip only | Eventual (same converence as `eventual`) |

Default: `strong` (preserves existing behaviour).

### What changes per mode

#### `strong` (unchanged from current "consistent_hash")
- `ownerFn` and `peerFetch` wired into `cache.Handler` as today.
- `Broadcaster` sends HTTP POST to peer admin + gossip fallback.
- Ring SVG, peer-fetch stats, ring digest anti-entropy remain active.
- `PeerFetchHandler` remains mounted on admin server.

#### `eventual`
- `ownerFn = nil`, `peerFetch = nil` — every node looks up its local
  store first; on miss goes straight to origin. No peer-fetch RPC.
- `Broadcaster` sends invalidation events **only via gossip** (no HTTP
  fan-out). Gossip is the sole delivery path. This avoids N-1 HTTP
  fan-out per invalidation.
- `NotifyMsg` in `cluster.go` processes incoming purge/ban gossip
  events and applies them locally (was a stub).
- The ring is still maintained for membership + dashboard display, but
  key ownership is irrelevant for routing. `Owner()` and `IsLocal()` are
  never called by the cache handler in this mode.
- No replication: each node only stores what it has fetched from origin.

#### `full`
- Same routing as `eventual`: `ownerFn = nil`, `peerFetch = nil`. Every
  node serves from local store; miss → origin.
- **Active replication**: when a node stores a cacheable response, it
  broadcasts the full `Object` to all peers via a new gossip message
  type (`ReplicationEvent`). Peers receive it and store it in their
  local hot tier, achieving full replication without each node making
  its own origin request.
- `Broadcaster` sends **both** invalidation events (purge/ban via
  gossip) and **replication events** (full object gossip on fill).
- `NotifyMsg` processes three message types: purge, ban, and replicate.
- The ring is used for membership only; `Owner()` is never called for
  routing. Dashboard shows per-node fill rates instead of key ownership.
- Memory: each node holds the entire working set. `hot_max_bytes` must
  be sized accordingly (at least the full working set).
- Convergence: replication gossip arrives within the memberlist flush
  interval (~1 s); invalidations same as `eventual`.

### Config schema change

```yaml
cluster:
  enabled: true
  mode: strong          # "strong" (default) | "eventual" | "full"
  join: [...]
  hop_limit: 2         # only used in strong mode
  tls: ...
```

`mode` defaults to `strong` when omitted — **zero breaking change**
for existing deployments.

### Mode comparison by operator concern

| Concern | `strong` | `eventual` | `full` |
|---------|----------|------------|--------|
| Hit rate on warm cluster | High (key concentrated) | Medium (each node cold-starts) | Highest (all keys everywhere) |
| Miss latency (peer fetch) | 1 RTT to owner + fallback | 0 (direct to origin) | 0 (direct to origin) |
| Memory efficiency | 1× working set | 1–N× (depending on overlap) | N× working set |
| Node failure impact | Keys on lost node → cold miss | None for cached keys | None for cached keys |
| Invalidation latency | < 1 s (HTTP fan-out) | 1–5 s (gossip) | 1–5 s (gossip) |
| Cross-node traffic | Peer fetch + inv fan-out | Gossip invalidations only | Gossip everything (inv + replication) |

## Consequences

### Positive
- Operators choose the consistency model that matches their deployment.
- `eventual` eliminates per-miss peer-fetch latency; ideal for CDN edge
  or geo-distributed deployments where each PoP is independent.
- `full` maximises hit rate and resilience; ideal for small clusters (2–5
  nodes) where memory is plentiful and low miss rate matters most.
- `strong` remains the most memory-efficient option for large clusters.
- Fully backward-compatible config change.

### Negative / trade-offs
- `eventual` and `full`: redundant storage reduces effective cluster
  memory. Operators must provision more RAM or accept smaller per-node
  caches.
- `eventual` and `full`: invalidation propagation is bounded by gossip
  convergence time (~1–5 s), not sub-second HTTP fan-out. Stale reads
  are possible in the convergence window.
- `full`: replication gossip adds bandwidth proportional to cache fill
  rate. On a hot origin this can be significant. Mitigated by:
  - Only replicating cacheable responses (not `no-store`, not > warm
    threshold).
  - Batching replication events via memberlist compound messages.
  - Backpressure: if the gossip queue exceeds a soft limit, replication
    events are dropped (the node will still cache on its own miss).
- Config misconfiguration: `hop_limit` is a no-op in `eventual` and
  `full` modes. Validate and warn at startup.

### Risks
- Gossip delivery is best-effort; a partitioned node in `eventual` or
  `full` mode may serve stale content until the partition heals.
  Acceptable by design for these modes.
- `full` replication memory pressure on small instances. Mitigated by
  SIEVE eviction, `hot_max_bytes` quota, and `warm_max_bytes` quota.
- `full` replication bandwidth: on a 10-node cluster with 10k req/s and
  average 50 KiB responses, replication gossip is ~500 MB/s. This is
  acceptable for typical data-center networks but may be too much for
  WAN deployments. Mitigation: `full` mode targets small clusters (2–5
  nodes) or data-center deployments; document the bandwidth implication.
