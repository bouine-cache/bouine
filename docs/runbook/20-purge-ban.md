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

1. The URL is hashed (xxhash64) to produce the cache key.
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
is still served (with `stale-if-error` if configured). Purge forces a
full miss.

---

## Monitoring invalidation

| Metric                                | Description                          |
|---------------------------------------|--------------------------------------|
| `bouine_purge_total`                  | Total purge operations.              |
| `bouine_ban_total`                    | Total ban operations.                |
| `bouine_ban_list_size`                | Current active ban predicates.       |
| `bouine_cache_result_total{result="miss_purged"}` | Requests that hit a purged/banned entry. |

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
```

---

## Cluster propagation

In a clustered deployment, invalidation commands are forwarded to the
appropriate peer(s):

- **Purge**: forwarded to the key's owner node (consistent-hash ring).
- **Ban**: broadcast to all peers so every node applies the predicate.
- **Refresh**: forwarded to the key's owner node.

The admin API on any node accepts invalidation requests and handles
routing internally.
