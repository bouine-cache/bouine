# 10 — Cluster consistency modes

How to verify, diagnose, and switch between the two cluster consistency
modes: `strong` (default) and `eventual`.

---

## Verify your mode

```bash
# Prometheus metric: always 1 with the mode label.
curl -s http://127.0.0.1:9000/metrics | grep bouine_cluster_mode_info

# Admin API: /v1/cluster/peers returns all members in every mode.
curl -s http://127.0.0.1:9000/v1/cluster/peers \
  -H "Authorization: Bearer ${BOUINE_ADMIN_TOKEN}"

# CLI
bouine cluster peers --server 127.0.0.1:9000 --token "${BOUINE_ADMIN_TOKEN}"
```

Expected: every node reports the same `mode` label. If they disagree, the cluster
has a configuration drift — every pod must use the same mode.

---

## Per-mode expectations

### `strong` (default)

| Check | Expected |
|-------|----------|
| Dashboard shows ring | Yes — consistent-hash ring with per-node key slices |
| Peer-fetch metrics increment | `bouine_peer_fetch_hits_total` > 0 |
| Purge hits all nodes | < 1 s via HTTP fan-out |
| Node failure | Keys owned by lost node → cold miss on all nodes for those keys |

**When things go wrong:**

- **Purge didn't propagate.** Check `bouine_cluster_invalidations_http_total`.
  If zero, the admin port may be unreachable. Verify `cluster.tls` config and
  network policies. The gossip broadcast queue provides a secondary delivery path
  (check `bouine_cluster_invalidations_gossip_total`).

- **Peer fetch is slow or failing.** Check `bouine_peer_fetch_duration_seconds`
  (should be < 2 ms on LAN). If elevated, check cluster network health (`kubectl
  get endpoints bouine-headless`). Increase `hop_limit` if node churn is high.

### `eventual`

| Check | Expected |
|-------|----------|
| Dashboard shows ring | No — shows per-node fill rates and gossip stats |
| Peer-fetch metrics | Always zero (no peer fetch) |
| Purge propagation | 1–5 s via gossip; stale reads possible during convergence |
| Node failure | No impact — each node has its own cache |

**When things go wrong:**

- **Purge didn't propagate within 5 seconds.** Check gossip membership:
  ```bash
  curl -s http://127.0.0.1:9000/v1/cluster/peers
  ```
  Should show 3+ nodes on every pod. If a node is missing, check its `cluster.join`
  list and DNS resolution. The headless Service must have
  `publishNotReadyAddresses: true`.

- **Stale reads on one node.** That node may be partitioned. Check
  `bouine_cluster_invalidations_gossip_total` — if the counter hasn't
  incremented recently, the gossip link is broken. Restart the node.

- **Hit rate lower than expected.** Each node cold-starts independently in
  `eventual` mode. Over time, hit rate naturally plateaus as each node fills its
  cache from origin traffic. If load is unevenly distributed across nodes
  (e.g. session affinity), some nodes may have much lower hit rates.

### `full`

| Check | Expected |
|-------|----------|
| Dashboard shows ring | No — shows per-node fill rates and replication throughput |
| Peer-fetch metrics | Always zero |
| Purge propagation | < 1 s via HTTP fan-out |
| Replications sent/received | `bouine_cluster_replications_sent_total` and `_received_total` grow with cache fills. `bouine_cluster_replications_dropped_total` should stay near 0. |
| Node failure | No impact — every node holds a full replica |

**When things go wrong:**

- **Replication not reaching peers.** Check `bouine_cluster_replications_sent_total`
  on the fill node and `bouine_cluster_replications_received_total` on peers.
  If sent > 0 but received = 0, check:
  - `bouine_cluster_replications_dropped_total` — if increasing, the semaphore
    is full (cluster overloaded) or peer admin ports are unreachable.
  - Network policy allows `POST /v1/peer/replicate` on the admin port.
  - `bouine_cluster_replication_bytes_total` against the cluster bandwidth
    budget (see [Memory and bandwidth budget](#memory-and-bandwidth-budget)).

- **Memory pressure on individual nodes.** `full` mode stores the entire working
  set on every node. If `hot_max_bytes` is undersized, SIEVE eviction will
  evict recently-replicated objects. Symptoms:
  - Hit rate drops on nodes that recently received replications.
  - SIEVE eviction spikes visible via the dashboard.
  - Fix: increase `hot_max_bytes` to at least the total working set size, or
    switch to `eventual` or `strong` mode.

- **High cross-node bandwidth.** Replication gossip scales with fill rate ×
  object size. On a 5-node cluster with 1 000 cacheable fills/s and avg 50 KiB
  responses, replication bandwidth is ~50 MB/s per node. If you see bandwidth
  saturation, consider:
  - Switching to `strong` mode (one copy per key, no replication).
  - Reducing the cluster size (fewer nodes = fewer replication targets).
  - Increasing `hot_max_bytes` to reduce churn (fewer evictions = fewer re-fills).

---

## Memory and bandwidth budget

### `strong` mode

- Memory per node: working set ÷ N (where N = cluster size).
- Bandwidth: minimal. Peer-fetch RPCs are small (key hashes, HTTP headers).
  HTTP fan-out is infrequent (only on invalidation).

### `eventual` mode

- Memory per node: 1–N× depending on traffic overlap. With round-robin load
  balancing, expect ~1× (each node caches ~1/N of the working set).
- Bandwidth: minimal. Gossip invalidations only.

### `full` mode

- Memory per node: N× the working set. Every node holds every cached object.
- Bandwidth budget: `fills_per_second × avg_response_bytes × (cluster_size - 1)`.
  Example: 1 000 fills/s × 50 KiB × (5 - 1) = 200 MB/s per node.
  Replication is via async HTTP POST (not gossip), so bandwidth is TCP
  traffic on the admin port, not UDP gossip.

**Budget validation test:**

```bash
# Run a fill burst while watching replication metrics
watch -n 1 'curl -s http://127.0.0.1:9000/metrics | grep bouine_cluster_replication'
```

For quantitative validation, run the integration test bandwidth check:
```bash
make integration-cluster-full
```

The `TestFull_ReplicationBandwidthMetric` test fills the cache on node 0 and
verifies `bouine_cluster_replication_bytes_total` increases on both the sender
and receiver sides.

---

## Switching modes

Mode changes require a full cluster restart. The procedure differs per mode:

### From `strong` to `eventual`

1. Update `cluster.mode` in your ConfigMap.
2. Rolling restart all pods one by one (`kubectl rollout restart statefulset/bouine`).
3. Verify `bouine_cluster_mode_info{mode="..."}` on every pod.

No data migration needed — each node starts with an empty cache.

### From `eventual` to `strong`

1. Update `cluster.mode: strong` in your ConfigMap.
2. Add `replicas` and `hop_limit` fields if missing.
3. Rolling restart. The consistent-hash ring forms within seconds of all nodes
   joining.
4. Cache state from `eventual` is **not preserved** — nodes start with
   empty caches. Expect elevated miss rates for the first few minutes until
   caches warm up.


## Alerts

```yaml
# Alert if cluster mode differs across pods (configuration drift).
- alert: ClusterModeMismatch
  expr: count(count by (mode) (bouine_cluster_mode_info == 1)) > 1
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "Cluster mode mismatch — pods running different consistency modes"

# Alert if replications stall in full mode.
- alert: FullReplicationStalled
  expr: rate(bouine_cluster_replications_sent_total[5m]) > 0
    and rate(bouine_cluster_replications_received_total[5m]) == 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Full-mode replication sending but not receiving — gossip may be broken"

# Alert on memory pressure in full mode.
- alert: FullModeMemoryPressure
  expr: bouine_hot_store_bytes / bouine_hot_store_max_bytes > 0.9
    and on() bouine_cluster_mode_info{mode="full"} == 1
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Full-mode node at >90% hot store capacity"
```

---

## Troubleshooting quick reference

| Symptom | Mode | Probable cause | Check |
|---------|------|---------------|-------|
| Purge doesn't propagate | `strong` | Admin port unreachable | `bouine_cluster_invalidations_http_total` |
| Purge doesn't propagate | `eventual` | Gossip partition | `bouine_cluster_invalidations_gossip_total`, peers list |
| Stale reads | `eventual` | Gossip convergence window | Wait 5 s, re-check. If persistent, check gossip. |
| Low hit rate | `eventual` | Uneven node fill | Check per-node hit rates, consider `strong` |
| Node join fails | all | DNS not resolving | `kubectl get endpoints`, verify `publishNotReadyAddresses` |
| Gossip drops increasing | all | Handoff queue overflow | `bouine_cluster_gossip_drops_total`, see [Gossip drops](#gossip-drops) |
| Gossip queue drops increasing | all | Local gossip queue overflow | `bouine_cluster_gossip_queue_dropped_total`, see [Gossip queue drops](#gossip-queue-drops) |

---

## Gossip drops

`bouine_cluster_gossip_drops_total` counts memberlist "handler queue full"
warnings — messages dropped because the receiving node's per-peer handoff
queue overflowed. The counter is node-local: to get cluster-wide drops,
use `sum(bouine_cluster_gossip_drops_total)`.

### When to expect zero

On a healthy cluster with tuned `handoff_queue_depth`, this counter should
stay at zero. Non-zero values mean invalidation messages are being dropped;
gossip provides redundant delivery, so a few drops may not cause visible
staleness, but sustained drops indicate a capacity problem.

### What to do when non-zero

1. **Check the rate.** `rate(bouine_cluster_gossip_drops_total[5m])` — if
   it's a brief spike during an invalidation burst, no action needed.
   Sustained non-zero rate requires intervention.
2. **Increase `handoff_queue_depth`.** Default is 4096 (4× memberlist's
   upstream 1024). Increase in powers of 2 (8192, 16384) and re-check the
   metric. Each slot costs a pointer + message header per peer; 4096 × 10
   peers ≈ 40 K entries.
3. **Check `GossipApplyTimeout`.** If the `NotifyMsg` handler is slow
   (e.g. store writes exceeding 100 ms), the handoff queue backs up.
   Profile the store write path with `go tool pprof` against the
   `/debug/pprof/*` endpoints on the admin port.
4. **Check for slow consumers.** A node that is CPU-bound or disk-bound
   will drain its handoff queue slowly. Check `bouine_hot_store_bytes`
   and node CPU/disk metrics.

### Sample alert

```yaml
# Alert if gossip drops are sustained over 5 minutes.
- alert: GossipDropsSustained
  expr: rate(bouine_cluster_gossip_drops_total[5m]) > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Memberlist handoff queue overflow — invalidation messages being dropped"
    description: "Increase cluster.handoff_queue_depth or investigate slow NotifyMsg handler."
```

---

## Gossip queue drops

`bouine_cluster_gossip_queue_dropped_total` counts messages dropped from
the local gossip broadcast queue because it was full (drop-newest policy).
`bouine_cluster_gossip_queue_depth` is a gauge showing the current number
of pending messages in the queue. Both are node-local.

### When to expect zero

On a healthy cluster with a tuned `gossip_queue_depth`, the dropped counter
should stay at zero. Non-zero values mean the local gossip queue is
overflowing — invalidation messages are being produced faster than
memberlist's `GetBroadcasts` drain can send them. A few drops during a
purge storm may not cause visible staleness (anti-entropy repairs missed
gossip, and in strong mode HTTP fan-out is the primary delivery path),
but sustained drops indicate a capacity problem.

### What to do when non-zero

1. **Check the rate.** `rate(bouine_cluster_gossip_queue_dropped_total[5m])`
   — a brief spike during an invalidation burst is expected. Sustained
   non-zero rate requires intervention.
2. **Check the queue depth gauge.** `bouine_cluster_gossip_queue_depth`
   — if it's pegged at the configured `gossip_queue_depth`, the queue is
   consistently full.
3. **Increase `gossip_queue_depth`.** Default is 4096. Increase in powers
   of 2 (8192, 16384) and re-check the metric. Each slot is a slice
   header (24 bytes); 4096 × 24 B = 96 KiB.
4. **Check memberlist gossip interval.** If `GetBroadcasts` is called
   infrequently (e.g. high `push_pull_interval` or network issues), the
   queue backs up locally even though peers are healthy.
5. **Reduce invalidation burst size.** If bans fan out as individual
   gossip broadcasts, consider batching or throttling at the application
   layer.
