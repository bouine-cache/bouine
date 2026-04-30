# Migrating between cluster consistency modes

bouine supports three cluster consistency modes. This guide covers the
operational considerations when switching between them.

## Modes at a glance

| Mode | Peer fetch | Replication | Invalidation path | Memory per node |
|------|-----------|-------------|-------------------|-----------------|
| `strong` | Yes (owner node) | None | HTTP fan-out + gossip | Working set / N |
| `eventual` | No | None | Gossip only | Working set (independent) |
| `full` | No | All objects | Gossip only | Working set × N |

## Changing modes

Cluster mode is read at startup. To switch modes:

1. Update `cluster.mode` in the bouine config file.
2. Roll out via a rolling restart (one node at a time).

During a rolling restart, nodes running the old mode coexist with nodes
running the new mode. This is safe because:

- **strong → eventual/full**: Nodes in strong mode continue to peer-fetch
  from other strong nodes. New-mode nodes cache independently.
- **eventual → strong**: Strong-mode nodes start routing via the ring as
  soon as they rejoin. Existing entries on eventual nodes expire via TTL.
- **full → strong**: Full replicas expire via TTL; strong mode takes over.

No data is lost during the transition — cached entries simply age out
via TTL.

## Migration paths

### `strong` → `eventual`

- **Hit rate**: Temporary drop as each node builds its own cache from
  origin. Recovery time depends on traffic volume.
- **Memory**: May increase if nodes overlap heavily (same popular objects
  cached N times instead of 1).
- **Invalidation**: Gossip-only convergence (~1–5 s). Stale reads are
  possible during the convergence window.
- **Recommendation**: Watch `bouine_cluster_invalidations_gossip_total`
  and `bouine_cache_result_total{result="miss"}` during transition.

### `strong` → `full`

- **Hit rate**: Immediately high — every node has a copy of every
  cached object.
- **Memory**: Jumps to N× working set size. Ensure each node has
  sufficient hot-tier capacity (`storage.hot_max_bytes`).
- **Invalidation**: Gossip-only, same convergence window as eventual.
- **Recommendation**: Pre-scale hot-tier capacity before switching.

### `eventual` → `strong`

- **Hit rate**: Temporary misses during key redistribution as the
  consistent-hash ring assigns new owners.
- **Memory**: Decreases as key sharding reduces per-node working set.
- **Invalidation**: Faster (HTTP fan-out + gossip, ~1 s).

### `eventual` ↔ `full`

- **eventual → full**: Replication begins immediately via gossip.
  Memory usage increases as objects are replicated. No cache misses
  during transition for objects already cached on any node.
- **full → eventual**: Replication stops. Existing replicas expire via
  TTL. Memory usage decreases.

### `full` → `strong`

- **Hit rate**: Temporary misses during key redistribution (same as
  `eventual → strong`). Memory decreases as replicas expire.

## Configuration

```yaml
cluster:
  enabled: true
  mode: eventual  # strong (default) | eventual | full
  join:
    - 10.0.0.1:7946
    - 10.0.0.2:7946
```

## Startup warnings

bouine logs warnings at startup:

- `cluster.hop_limit is set but has no effect in non-strong mode` —
  `hop_limit` only applies in `strong` mode (where peer fetch happens).
  In `eventual`/`full` there is no peer fetch, so the setting is ignored.
- `cluster.mode is 'full' — every node holds a copy of every cached
  object; memory usage scales linearly with cluster size` — reminds
  operators about the N× memory cost.
