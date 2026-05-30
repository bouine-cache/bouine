# Implementation Plan: Cluster Consistency Modes — strong, eventual, full

**ADR**: [0008-cluster-consistency-modes](../decisions/0008-cluster-mode-local-cache-gossip-invalidation.md)
**Date**: 2026-05-29
**Status**: Proposed

---

## Overview

Add a `cluster.mode` config field (`strong` | `eventual` | `full`) that lets operators choose the consistency model for their deployment:

- **`strong`** (current, default): consistent hash ring owns keys, peer fetch on miss, 1 copy per key, HTTP+gossip invalidation.
- **`eventual`**: every node caches locally, no peer fetch, invalidation by gossip only. Each node only stores what it has fetched from origin (no active replication).
- **`full`**: same routing as `eventual` (no peer fetch), but actively replicates every cached object to all peers via gossip. Highest hit rate, N× memory.

---

## Step 1 — Config schema: add `cluster.mode`

**File**: `internal/config/config.go`

Add `Mode` to `Cluster` and define constants:

```go
const (
    ClusterModeStrong   = "strong"    // consistent hash ring, peer fetch, 1 copy
    ClusterModeEventual  = "eventual"  // local cache, gossip invalidation, N independent copies
    ClusterModeFull      = "full"      // local cache + full replication, gossip everything
)

type Cluster struct {
    Enabled  bool       `yaml:"enabled"`
    Mode     string     `yaml:"mode"`       // strong (default) | eventual | full
    Join     []string   `yaml:"join"`
    Replicas int        `yaml:"replicas"`
    HopLimit int        `yaml:"hop_limit"`
    TLS      ClusterTLS `yaml:"tls"`
}
```

Extend validation:
- Default `Mode` to `"strong"` when empty.
- Reject unknown values at startup.
- Warn (log) when `hop_limit` is set in `eventual` or `full` mode (no-op).
- Warn when `full` mode is used — document memory implications.
- Return an error if `mode` is not `"strong"` and `cluster.enabled == false`.

**No breaking change**: omitted `mode` → `strong`, identical to current behaviour.

---

## Step 2 — Cluster: mode accessor + `NotifyMsg` gossip receive handler

**File**: `internal/cluster/cluster.go`

### 2a. Add `Mode()` method

```go
func (c *Cluster) Mode() string { return c.cfg.Mode }
```

### 2b. Add `Replicator` interface and `SetInvalidator` / `SetReplicator`

The gossip receive handler must process three event types. Define callbacks
so the cluster package doesn't import storage directly:

```go
type Invalidator struct {
    PeerPurgeFn func(ctx context.Context, evt api.PurgeEvent) error
    PeerBanFn   func(ctx context.Context, evt api.BanEvent) error
}

type Replicator struct {
    // StoreObject stores a replicated object into the local hot tier.
    // Called when a full-mode peer gossips a newly cached response.
    StoreObject func(ctx context.Context, obj *api.Object) error
}

func (c *Cluster) SetInvalidator(inv Invalidator) { c.inv = inv }
func (c *Cluster) SetReplicator(rep Replicator)    { c.rep = rep }
```

### 2c. Implement `NotifyMsg`

Replace the stub with a three-way dispatcher:

```go
func (c *Cluster) NotifyMsg(msg []byte) {
    // Try replication event first (has Method field that purge/ban lack).
    if c.rep.StoreObject != nil {
        var replEvt api.ReplicationEvent
        if err := json.Unmarshal(msg, &replEvt); err == nil && replEvt.Method != "" {
            if err := c.rep.StoreObject(context.Background(), replEvt.Object); err != nil {
                c.logger.Warn("cluster: gossip replication apply failed", "error", err)
            }
            return
        }
    }
    // Try purge event (Key != 0).
    if c.inv.PeerPurgeFn != nil {
        var purgeEvt api.PurgeEvent
        if err := json.Unmarshal(msg, &purgeEvt); err == nil && purgeEvt.Key != 0 {
            if err := c.inv.PeerPurgeFn(context.Background(), purgeEvt); err != nil {
                c.logger.Warn("cluster: gossip purge apply failed", "error", err)
            }
            return
        }
    }
    // Try ban event (non-empty predicate).
    if c.inv.PeerBanFn != nil {
        var banEvt api.BanEvent
        if err := json.Unmarshal(msg, &banEvt); err == nil && banEvt.Predicate != (api.BanExpr{}) {
            if err := c.inv.PeerBanFn(context.Background(), banEvt); err != nil {
                c.logger.Warn("cluster: gossip ban apply failed", "error", err)
            }
            return
        }
    }
    c.logger.Debug("cluster: unrecognized gossip message", "len", len(msg))
}
```

**Why all modes process invalidation gossip**: Even in `strong` mode, gossip
is the redundant secondary delivery path when a peer's admin port is
unreachable. Processing gossip messages makes all modes more resilient.

---

## Step 3 — New API type: `ReplicationEvent`

**File**: `pkg/api/cluster.go`

```go
// ReplicationEvent is broadcast when a node stores a cacheable response
// in full replication mode. Peers store the enclosed object in their
// local hot tier without making their own origin request.
type ReplicationEvent struct {
    // Method is the HTTP method of the original request (GET, HEAD).
    // Used as a discriminator to distinguish replication events from
    // purge/ban events in the gossip deserialiser.
    Method string `json:"method"`
    // Object is the full cached response to be stored locally.
    Object *api.Object `json:"object"`
    // Issuer is the node name that cached the response.
    Issuer   string    `json:"issuer"`
    IssuedAt time.Time `json:"issued_at"`
    Seq      uint64    `json:"seq"`
}
```

The `Method` field acts as the discriminator: purge events have `Key != 0`
but no `Method`; ban events have `Predicate != {}` but no `Method`. The
deserialiser tries `ReplicationEvent` first (by checking `Method != ""`),
then `PurgeEvent`, then `BanEvent`.

---

## Step 4 — Broadcaster: mode-aware fan-out strategy

**File**: `internal/cluster/broadcast.go`

Add a `mode` field:

```go
type Broadcaster struct {
    cluster *Cluster
    fetcher *PeerFetcher
    seq     atomic.Uint64
    logger  *slog.Logger
    token   string
    mode    string // ClusterModeStrong | ClusterModeEventual | ClusterModeFull
}
```

Wire `mode` in `NewBroadcaster` from `cluster.Mode()`.

### 4a. `BroadcastPurge` changes:

```go
func (b *Broadcaster) BroadcastPurge(ctx context.Context, key api.Key, varyKey string) {
    evt := api.PurgeEvent{...}

    if b.mode == ClusterModeStrong {
        // Current behaviour: HTTP POST to each peer admin
        peers := b.cluster.Members()
        var wg sync.WaitGroup
        for _, p := range peers {
            if p.Name == b.cluster.cfg.NodeName { continue }
            wg.Add(1)
            go func(peer api.PeerInfo) {
                defer wg.Done()
                if err := b.sendPurge(ctx, peer, evt); err != nil {
                    b.logger.Warn("purge broadcast failed", "peer", peer.Name, "error", err)
                }
            }(p)
        }
        wg.Wait()
    }

    // All modes: enqueue via gossip.
    // In strong mode this is redundant (HTTP already delivered it).
    // In eventual/full mode this is the sole delivery path for invalidations.
    if body, err := json.Marshal(evt); err == nil {
        b.cluster.QueueBroadcast(body)
    }
}
```

Same pattern for `BroadcastBan`.

### 4b. New `BroadcastReplicate` method (full mode only)

```go
func (b *Broadcaster) BroadcastReplicate(ctx context.Context, obj *api.Object) {
    if b.mode != ClusterModeFull {
        return
    }
    evt := api.ReplicationEvent{
        Method:   "GET", // replication is always a GET response
        Object:   obj,
        Issuer:   b.cluster.cfg.NodeName,
        IssuedAt: time.Now(),
        Seq:      b.seq.Add(1),
    }
    if body, err := json.Marshal(evt); err == nil {
        b.cluster.QueueBroadcast(body)
    }
}
```

This is called from the cache handler after storing a cacheable response
(see Step 5).

**Key difference**: in `eventual` and `full` modes, `BroadcastPurge` and
`BroadcastBan` never do HTTP fan-out. Gossip is the sole invalidation
transport. In `full` mode, `BroadcastReplicate` also gossips the full
object on fill.

---

## Step 5 — Cache handler: mode-aware routing + replication hook

**File**: `internal/cache/handler.go`

### 5a. No changes needed for `eventual`/`full` routing

The existing code already skips peer fetch when `ownerFn == nil`:

```go
if h.ownerFn != nil && h.peerFetch != nil {
    if owner, isLocal := h.ownerFn(key); !isLocal {
        // peer fetch block...
    }
}
```

Setting `ownerFn = nil` and `peerFetch = nil` in `eventual`/`full` modes
(Step 7) makes all misses go straight to origin. **No handler code change
needed for routing.**

### 5b. Replication callback on fill (full mode only)

Add a `ReplicateFn` field to `HandlerConfig`:

```go
type HandlerConfig struct {
    // ... existing fields ...
    // ReplicateFn, if non-nil, is called after a cacheable response is
    // stored locally. Used in full mode to broadcast the object to peers.
    // Nil in strong and eventual modes.
    ReplicateFn func(ctx context.Context, obj *api.Object)
}
```

In `writeAndMaybeStore`, after storing the object:

```go
if IsCacheable(res.StatusCode, r.Header, res.Header, h.negativeTTL) {
    obj := buildObject(storeKey, r, res, ...)
    _ = h.store.Put(r.Context(), storeKey, obj)
    // Also store primary entry for Vary (existing code)...

    // Replication hook: broadcast the newly cached object in full mode.
    if h.replicateFn != nil {
        h.replicateFn(r.Context(), obj)
    }
}
```

### 5c. Replication receive in engine wiring

In `engine.go`, when mode is `full`, the `NotifyMsg` handler receives
`ReplicationEvent` messages and calls `store.Put()` on the local store.
This is wired in Step 7.

**Important**: replicated objects are stored with their original TTL and
freshness metadata. The receiving node must not re-evaluate cache headers —
the originating node already did that.

---

## Step 6 — Engine wiring: mode-aware handler construction

**File**: `cmd/bouine/cmd/engine.go`

### 6a. Conditional `ownerFn` / `peerFetch` / `replicateFn`

```go
var ownerFn func(key api.Key) (api.PeerInfo, bool)
var peerFetch func(ctx context.Context, peer api.PeerInfo, key api.Key) (*api.Object, error)
var replicateFn func(ctx context.Context, obj *api.Object)

switch cfg.Cluster.Mode {
case config.ClusterModeStrong:
    if clusterNode != nil {
        ownerFn = clusterNode.IsLocal // or Owner+isLocal check
        peerFetch = peerFetcher.Fetch
    }
case config.ClusterModeEventual:
    // No peer fetch, no replication
case config.ClusterModeFull:
    if broadcaster != nil {
        replicateFn = broadcaster.BroadcastReplicate
    }
}
```

Pass these into `HandlerConfig`.

### 6b. Wire `Invalidator` and `Replicator`

```go
if clusterNode != nil {
    clusterNode.SetInvalidator(cluster.Invalidator{
        PeerPurgeFn: func(ctx context.Context, evt api.PurgeEvent) error {
            return store.Delete(ctx, evt.Key)
        },
        PeerBanFn: func(ctx context.Context, evt api.BanEvent) error {
            _, err := store.Ban(ctx, evt.Predicate)
            return err
        },
    })

    if cfg.Cluster.Mode == config.ClusterModeFull {
        clusterNode.SetReplicator(cluster.Replicator{
            StoreObject: func(ctx context.Context, obj *api.Object) error {
                return store.Put(ctx, obj.Key, obj)
            },
        })
    }
}
```

### 6c. Broadcaster mode

`NewBroadcaster` reads `clusterNode.Mode()` and stores it internally.
No explicit mode argument needed — it's derived from the cluster config.

---

## Step 7 — Dashboard: conditional cluster page views

**Files**: `internal/dashboard/templates/cluster.templ`, `models.go`, `handler.go`, `overview.templ`

### 7a. Add `Mode` to `ClusterMeta`

```go
type ClusterMeta struct {
    VirtualNodes     int
    LoadFactor       float64
    HopLimit         int
    PeerFetchTimeout string
    ProtocolVersion  string
    GossipInterval   string
    JoinRetryBudget  string
    Mode             string // "strong" | "eventual" | "full"
}
```

### 7b. Conditional ring display in `cluster.templ`

```templ
if d.Meta.Mode == "strong" {
    <!-- Current: ring SVG + peer-fetch stats + ring stats -->
} else if d.Meta.Mode == "eventual" {
    <div class="bc">
        <div class="bc-t">Eventual consistency mode</div>
        <div style="font-size:.72rem;color:var(--m);padding:.5rem 0">
            Each node caches independently. No key sharding. Invalidation
            propagates via gossip (eventual consistency, ~1–5 s convergence).
            No replication — nodes only cache what they fetch from origin.
        </div>
    </div>
} else if d.Meta.Mode == "full" {
    <div class="bc">
        <div class="bc-t">Full replication mode</div>
        <div style="font-size:.72rem;color:var(--m);padding:.5rem 0">
            Every node holds a copy of every cached object. Maximum hit rate
            at N× memory cost. Invalidation + replication via gossip.
        </div>
    </div>
}
```

### 7c. Conditional stat rows

- `hop_limit` → only show in `strong` mode
- `peer fetch timeout` → only show in `strong` mode
- `load factor` → only show in `strong` mode
- `mode` → show in all modes
- `virtual nodes / real` → only show in `strong` mode
- In `eventual`/`full` modes, show: `replication: none` / `replication: full`
- In `full` mode, show: `replication bandwidth` estimate

### 7d. Overview page compact ring

In `local_cache`/`full` modes, replace the ring SVG with per-node cache fill
bars (hot tier fill % per peer).

Add `ClusterMode string` to `OverviewData`.

### 7e. Engine wiring for dashboard

In `buildDashboard`:

```go
if clusterNode != nil {
    clusterMeta.Mode = clusterNode.Mode()
} else {
    clusterMeta.Mode = "single-node"
}
```

### 7f. Config viewer update

In `BuildConfigSections`, add a mode row:

```go
{Key: "mode", Value: cfg.Cluster.Mode, Kind: "str",
 Hint: "strong: ring-sharded · eventual: local cache, gossip invalidation · full: full replication, gossip everything"}
```

---

## Step 8 — Metrics: mode-specific counters

**File**: `internal/cluster/metrics.go` (new) or extend `internal/observability/dataplane.go`

```go
bouine_cluster_mode_info{mode="strong|eventual|full"} 1  // constant gauge
bouine_cluster_invalidations_gossip_total{type="purge|ban"}  // both modes
bouine_cluster_invalidations_http_total{type="purge|ban"}    // strong only
bouine_cluster_replications_sent_total                       // full only
bouine_cluster_replications_received_total                   // full only
bouine_cluster_replication_bytes_total{direction="sent|received"}  // full only
```

---

## Step 9 — Documentation updates

### 9a. ADR-0008
Already updated with full three-mode comparison table.

### 9b. `docs/runbook/20-purge-ban.md`
Add section on invalidation propagation per mode:

- **`strong`**: purge/ban reach all peers within ~1 s via HTTP fan-out + gossip. If the owning peer is down, the key is re-assigned.
- **`eventual`**: purge/ban propagate via gossip only. Convergence window ~1–5 s. Stale reads are possible.
- **`full`**: same as `eventual` for invalidations. Additionally, cached objects are replicated to all nodes within the gossip flush interval.

### 9c. `docs/migration/cluster-mode.md` (new)
Migration guide covering all three mode switches:

- `strong` → `eventual`: expect hit rate drop as each node cold-starts, then warm-up. Memory pressure may increase if overlap is high.
- `strong` → `full`: immediate high hit rate; memory usage jumps to N× working set.
- `eventual` → `strong`: peer fetch resumes; key redistribution may cause temporary misses.
- `full` → `strong`: memory freed; peer fetch resumes.
- `eventual` ↔ `full`: replication starts/stops; memory usage changes accordingly.
- Rollout: rolling restart (mode is read at startup). No data loss during transition.

### 9d. Plan.md update
Add `cluster.mode` to §5 (Clustering) and the mode comparison table.

---

## Step 10 — Integration tests: all three modes

**Files**: `test/integration/cluster_strong_test.go`, `test/integration/cluster_eventual_test.go`, `test/integration/cluster_full_test.go`

Build tag: `//go:build integration`

### 10a. Shared infrastructure

Extend `test/integration/driver/driver.go`:

- `Boot()` accepts `ClusterMode` in `Options` (`"strong"` | `"eventual"` | `"full"`).
- Generates bouine config with `cluster.mode` set accordingly.
- Docker compose supports a `BOUIINE_CLUSTER_MODE` env var.

### 10b. `cluster_strong_test.go` (existing behaviour, no regression)

| Test | What it asserts |
|------|-----------------|
| `TestStrong_ClusterFormation` | 3 nodes join, `GET /v1/cluster/peers` returns 3 |
| `TestStrong_PeerFetch` | PUT to node A; GET same key from node B → HIT |
| `TestStrong_PurgePropagation` | PURGE on node A; GET from node B → MISS |
| `TestStrong_BanPropagation` | BAN on node A; GET from node B → MISS |
| `TestStrong_SingleNodeFailure` | Kill node A; key owned by A falls through to origin |
| `TestStrong_HopLimit` | 3+ hops → origin fallback, no loop |

### 10c. `cluster_eventual_test.go`

| Test | What it asserts |
|------|-----------------|
| `TestEventual_ClusterFormation` | 3 nodes join, gossip membership healthy |
| `TestEventual_IndependentCaching` | GET from node A → MISS (origin); GET from node B → MISS (origin, not peer); GET from A again → HIT |
| `TestEventual_NoPeerFetch` | `bouine_peer_fetch_hits_total == 0` |
| `TestEventual_PurgePropagationGossip` | PURGE on node A; wait ≤ 10 s; GET from node B → MISS |
| `TestEventual_BanPropagationGossip` | BAN on node A; wait ≤ 10 s; GET from node B → MISS |
| `TestEventual_StaleDuringConvergence` | PURGE on node A; immediately GET from node B → may still be HIT; after convergence → MISS |
| `TestEventual_SingleNodeFailure` | Kill node A; node B still serves HITs for keys it has |

### 10d. `cluster_full_test.go`

| Test | What it asserts |
|------|-----------------|
| `TestFull_ClusterFormation` | 3 nodes join, gossip membership healthy |
| `TestFull_ReplicationOnFill` | PUT via node A; wait ≤ 5 s; GET from node B → HIT ( replicated) |
| `TestFull_ReplicationNoPeerFetch` | `bouine_peer_fetch_hits_total == 0` |
| `TestFull_PurgePropagationGossip` | PURGE on node A; wait ≤ 10 s; GET from node B → MISS |
| `TestFull_BanPropagationGossip` | BAN on node A; wait ≤ 10 s; GET from node B → MISS |
| `TestFull_SingleNodeFailure` | Kill node A; nodes B and C still serve HITs |
| `TestFull_ReplicationBandwidth` | Gauge that `bouine_cluster_replication_bytes_total{sent}` increases after fill |

### 10e. Docker compose

```yaml
bouine:
  environment:
    - BOUIINE_CLUSTER_MODE=${BOUIINE_CLUSTER_MODE:-strong}
```

### 10f. CI integration

```makefile
integration:
	docker compose -f test/integration/docker-compose.yaml up -d
	go test -race -tags=integration -run TestStrong ./test/integration/...
	go test -race -tags=integration -run TestEventual ./test/integration/...
	go test -race -tags=integration -run TestFull ./test/integration/...
	docker compose -f test/integration/docker-compose.yaml down
```

---

## Step 11 — Unit tests

### 11a. `internal/config/config_test.go`
- `TestClusterModeDefault`: empty mode defaults to `strong`.
- `TestClusterModeInvalid`: unknown value fails validation.
- `TestClusterModeEventualWarnOnHopLimit`: warns when `hop_limit` set in `eventual`.
- `TestClusterModeFullMemoryWarning`: warns about memory implications in `full`.

### 11b. `internal/cluster/cluster_test.go`
- `TestNotifyMsg_PurgeEvent`: gossip purge → `PeerPurgeFn` called.
- `TestNotifyMsg_BanEvent`: gossip ban → `PeerBanFn` called.
- `TestNotifyMsg_ReplicationEvent`: gossip replicate → `StoreObject` called.
- `TestNotifyMsg_MalformedPayload`: log + skip, no panic.
- `TestNotifyMsg_WhenNoInvalidator`: log + skip.

### 11c. `internal/cluster/broadcast_test.go`
- `TestBroadcastPurge_Strong`: HTTP fan-out + gossip.
- `TestBroadcastPurge_Eventual`: no HTTP fan-out, gossip only.
- `TestBroadcastPurge_Full`: no HTTP fan-out, gossip only.
- `TestBroadcastBan_Strong`: HTTP fan-out + gossip.
- `TestBroadcastBan_Eventual`: no HTTP fan-out, gossip only.
- `TestBroadcastReplicate_Full`: gossip only with `ReplicationEvent`.
- `TestBroadcastReplicate_Eventual`: no-op (not full mode).
- `TestBroadcastReplicate_Strong`: no-op (not full mode).

### 11d. `internal/cache/handler_test.go`
- `TestHandler_EventualNoPeerFetch`: `ownerFn=nil` → miss goes to origin.
- `TestHandler_StrongPeerFetchOnMiss`: existing behaviour, no regression.
- `TestHandler_FullReplicationHook`: after `writeAndMaybeStore`, `ReplicateFn` called with the stored object.
- `TestHandler_FullReplicationHookNotCalledOnBypass`: bypass → no replication.

---

## Step 12 — Regression prevention

### 12a. Benchmark gate

All three modes share the same cache-hit path (store lookup → serve). The
only branching is on `ownerFn != nil` which is a single nil check — zero
allocation overhead.

- `strong` hit path: **zero allocation, identical p99** (unchanged).
- `eventual` hit path: same hot-tier lookup, zero alloc (no peer-fetch code executed).
- `full` hit path: same as `eventual` on hit; `ReplicateFn` call on miss-path fill adds one `json.Marshal` of the object body.

`make bench` must show no p99 regression within 2%.

### 12b. Conformance gate

`make conformance` — RFC 9111 cache semantics are unchanged. The cache
engine, `Evaluate()`, `serveFromCache()` are untouched. Invalidations are
applied locally in all modes; the *propagation mechanism* changes but the
*semantics* (key gone after purge) remain identical.

### 12c. Coverage

- `internal/cluster`: ≥ 85% (new `NotifyMsg` + `BroadcastPurge/Replicate` mode branches)
- `internal/config`: ≥ 85% (new `Mode` validation)
- `internal/cache`: ≥ 95% (replication hook on fill path)

---

## Execution order

| # | Step | Files | Risk | Depends on |
|---|------|-------|------|------------|
| 1 | Config schema + constants | `internal/config/config.go` | Low | — |
| 2 | `ReplicationEvent` API type | `pkg/api/cluster.go` | Low | — |
| 3 | Cluster `Mode()` + `Invalidator`/`Replicator` + `NotifyMsg` | `internal/cluster/cluster.go` | Medium | 1 |
| 4 | Broadcaster mode-aware + `BroadcastReplicate` | `internal/cluster/broadcast.go` | Low | 1, 3 |
| 5 | Handler replication hook | `internal/cache/handler.go` | Low | 2 |
| 6 | Engine wiring | `cmd/bouine/cmd/engine.go` | Low | 1, 3, 4, 5 |
| 7 | Dashboard conditional views | `internal/dashboard/templates/*.templ`, `models.go`, `handler.go` | Low | 1 |
| 8 | Metrics | `internal/cluster/metrics.go` or `internal/observability/` | Low | 1 |
| 9 | Documentation | `docs/decisions/`, `docs/runbook/`, `docs/migration/` | None | 1–8 |
| 10 | Integration tests | `test/integration/` | High (docker) | 1–6 |
| 11 | Unit tests | `internal/{config,cluster,cache}/*_test.go` | Low | 1–6 |
| 12 | Regression gates | `bench/`, `test/cachetests/` | Low | 1–11 |

Steps 3, 4, and 5 are the critical path.

---

## Acceptance criteria

- [ ] `cluster.mode: strong|eventual|full` accepted by config; default `strong`.
- [ ] `strong`: behaviour identical to current (no regression).
- [ ] `eventual`: no peer-fetch traffic; every node fetches from origin on miss.
- [ ] `eventual`: purge/ban/refresh propagate via gossip within ≤ 10 s on 3-node cluster.
- [ ] `full`: newly cached objects replicate to all nodes within ≤ 5 s.
- [ ] `full`: no peer-fetch traffic; `peer_fetch_hits_total == 0`.
- [ ] `full`: kill one node; remaining nodes serve HITs from replicated data.
- [ ] Dashboard shows mode-appropriate cluster page (ring vs. eventual info vs. full info).
- [ ] Integration tests pass for all three modes.
- [ ] `make bench`: no p99 or allocation regression on hit path.
- [ ] `make conformance`: score unchanged.
- [ ] ADR-0008 merged; docs/migration guide published.
