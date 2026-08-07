# 20 — Purge and ban operations

Cache invalidation via purge (exact URL), ban (predicate), and refresh
(soft-purge / mark-stale).

---

## Concepts

| Method    | Scope            | Effect                                    | Latency     |
|-----------|------------------|-------------------------------------------|-------------|
| **Purge** | Single URL       | Immediate removal from cache.             | Synchronous |
| **Ban**   | Predicate match  | Lazy: entries checked on next lookup.     | Synchronous |
| **Refresh** | Single URL    | Marks stale; revalidates on next request. | Synchronous |

---

## Purge (exact key)

Removes a single cached response by URL.

### CLI

```bash
bouine purge https://example.com/products/123 \
  --server 127.0.0.1:9000 \
  --token "${BOUINE_ADMIN_TOKEN}"
```

### Admin API

```bash
curl -X POST http://127.0.0.1:9000/v1/purge \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${BOUINE_ADMIN_TOKEN}" \
  -d '{"url":"https://example.com/products/123"}'
```

### Response

```json
{"status":"purged"}
```

### How it works

1. The URL is hashed (XXH128, producing a 128-bit `[16]byte` key) to produce the cache key.
2. The key is deleted from the hot tier (in-RAM sharded map).
3. In a clustered setup, the purge is forwarded to the key's owner
   node via the consistent-hash ring.

### Caveats

- Purge operates on the **exact URL**. Query parameter order matters
  (keys are sorted during normalization, so `?a=1&b=2` and `?b=2&a=1`
  produce the same key).
- `Vary`-based variants: all variants sharing the base key are purged.
- Purge does **not** propagate to warm (disk) tier in the current
  implementation — warm entries expire naturally via TTL.

---

## Ban (predicate-based)

Bans invalidate entries lazily: a ban predicate is stored, and entries
are checked against active bans on each cache lookup. Matched entries
are treated as misses.

### CLI

```bash
bouine ban host_regex=example.com path_regex=^/api/ \
  --server 127.0.0.1:9000 \
  --token "${BOUINE_ADMIN_TOKEN}"
```

Multiple predicates are ANDed: the entry must match **all** predicates.

### Admin API

```bash
curl -X POST http://127.0.0.1:9000/v1/ban \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${BOUINE_ADMIN_TOKEN}" \
  -d '{"host_regex":"example.com","path_regex":"^/api/"}'
```

### Response

```json
{"status":"banned","count":42}
```

`count` is the number of entries invalidated at the time of the ban.
Additional entries matching the predicate will be invalidated lazily on
lookup.

### Available predicates

| Predicate     | Type  | Description                          |
|---------------|-------|--------------------------------------|
| `host_regex`  | regex | Match against the `Host` header.     |
| `path_regex`  | regex | Match against the URL path.          |
| `method`      | exact | HTTP method (GET, HEAD, etc.).        |

> **Note**: Predicate-based bans with surrogate keys are planned for
> phase 5. The current implementation supports host/path/method.

### Operational notes

- Bans accumulate in memory. They are garbage-collected when all cached
  entries older than the ban's timestamp have expired.
- Monitor `bouine_ban_list_size` to track active ban count.
- Excessive bans (>10 k) can impact lookup latency — prefer purge for
  single-URL invalidation.

---

## Refresh (soft-purge)

Marks an entry as stale without removing it. The next request triggers
an origin revalidation (`If-None-Match` / `If-Modified-Since`). If the
origin returns `304 Not Modified`, the existing cached body is reused.

### CLI

```bash
bouine refresh https://example.com/products/123 \
  --server 127.0.0.1:9000 \
  --token "${BOUINE_ADMIN_TOKEN}"
```

### Admin API

```bash
curl -X POST http://127.0.0.1:9000/v1/refresh \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${BOUINE_ADMIN_TOKEN}" \
  -d '{"url":"https://example.com/products/123"}'
```

### When to use refresh vs purge

| Scenario                          | Use        |
|-----------------------------------|------------|
| Content is wrong / security issue | **Purge**  |
| Content updated, old is acceptable temporarily | **Refresh** |
| Bulk invalidation by pattern      | **Ban**    |

Refresh is gentler: if the origin is slow or down, the stale response
is still served (stale-on-error fallback is always active for responses
without `must-revalidate`; explicit `stale-if-error` windows extend it
further). Purge forces a full miss.

> **Note**: When bouine serves a stale response it adds
> `Warning: 110 - "Response is Stale"` to the reply (RFC 7234 §5.5.3).
> Downstream clients and load balancers may log or act on this header.

---

## Monitoring invalidation

| Metric                                | Description                          |
|---------------------------------------|--------------------------------------|
| `bouine_purge_total`                  | Total purge operations.              |
| `bouine_ban_total`                    | Total ban operations.                |
| `bouine_ban_list_size`                | Current active ban predicates.       |
| `bouine_cache_result_total{result="miss_purged"}` | Requests that hit a purged/banned entry. |
| `bouine_cache_result_total{result="stale"}` | Stale serves (SWR, SIE, or error fallback). |

### Alerts

```yaml
# Alert if purge rate spikes (possible invalidation storm)
- alert: HighPurgeRate
  expr: rate(bouine_purge_total[5m]) > 100
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Elevated purge rate on {{ $labels.instance }}"

# Alert if stale serves climb above baseline (possible origin outage)
- alert: HighStaleServeRate
  expr: |
    rate(bouine_cache_result_total{result="stale"}[5m])
    /
    rate(bouine_cache_result_total[5m]) > 0.10
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "Stale serve ratio > 10 % on {{ $labels.instance }}"
    description: |
      Bouine is serving stale responses at an elevated rate. Possible causes:
      origin is returning 5xx, SWR window is large, or heuristic freshness
      objects have gone stale. Check X-Cache: STALE responses and upstream health.
```

---

## Cluster propagation

In a clustered deployment, invalidation commands are forwarded to the
appropriate peer(s). The propagation mechanism depends on the cluster mode:

### `strong` mode (default)

- **Purge**: forwarded to the key's owner node (consistent-hash ring),
  then broadcast via gossip as a secondary path.
- **Ban**: broadcast to all peers via HTTP fan-out, then gossiped.
- **Refresh**: forwarded to the key's owner node.
- Convergence: ~1 s. If a peer is temporarily unreachable, gossip
  ensures eventual delivery.

### `eventual` mode

- **Purge/Ban**: broadcast via gossip only. No HTTP fan-out.
- Convergence: ~1–5 s (gossip interval dependent). Stale reads are
  possible during the convergence window.
- Each node caches independently; no key sharding, no peer fetch on miss.


The admin API on any node accepts invalidation requests and handles
routing internally regardless of cluster mode.

---

## Cluster metrics

| Metric | Description | Mode |
|--------|-------------|------|
| `bouine_cluster_mode_info` | Constant gauge with mode label (strong/eventual) | all |
| `bouine_cluster_invalidations_http_total{type="purge\|ban"}` | HTTP fan-out invalidations | strong |
| `bouine_cluster_invalidations_gossip_total{type="purge\|ban"}` | Gossip invalidation events received | all |
| `bouine_cluster_replications_sent_total` | Cached objects broadcast to peers | full |
| `bouine_cluster_replications_received_total` | Replicated objects stored locally | full |
| `bouine_cluster_replication_bytes_total{direction="sent\|received"}` | Approximate byte size of replicated objects | full |
