# E-commerce example

A 2-node bouine cluster in front of a mock origin that serves a typical
e-commerce catalog: static assets, product pages, cart endpoints, and
personalized account pages.

## What this demonstrates

| Route              | TTL      | SWR   | SIE  | Invalidation strategy |
|--------------------|----------|-------|------|-----------------------|
| `/static/**`       | 7 days   | —     | 1h   | Surrogate-key ban     |
| `/images/**`       | 24 hours | 2min  | 1h   | Surrogate-key ban     |
| `/products/**`     | 1 minute | 30s   | 5min | Surrogate-key ban     |
| `/` (homepage)     | 2 minutes| 30s   | 5min | Exact URL purge       |
| `/cart/`, `/checkout/`, `/account/` | disabled | — | — | n/a (private) |

The mock origin returns `Cache-Control: public, max-age=...` for
cacheable routes and `Cache-Control: private, no-store` for cart and
account endpoints, so bouine's RFC 9111 compliance is exercised end to
end.

## Quick start

```bash
docker compose up -d --build

# Verify cluster health:
curl http://localhost:19001/healthz
curl http://localhost:19002/healthz

# Warm cache:
curl -i http://localhost:18081/products/widget   # MISS
curl -i http://localhost:18081/products/widget   # HIT
curl -i http://localhost:18082/products/widget   # HIT (peer fetch)
```

## Try surrogate-key invalidation

```bash
export BOUINE_ADMIN_TOKEN=dev-token

# Ban all products:
curl -X POST http://localhost:19001/admin/bans \
  -H "Authorization: Bearer $BOUINE_ADMIN_TOKEN" \
  -d '{"surrogate_keys":["products/.*"]}'

# Next request should be MISS:
curl -i http://localhost:18081/products/widget   # MISS
```

## Trade-offs

**Why 1-minute product TTL?**
- Short enough to reflect catalog updates within minutes
- Long enough to absorb traffic spikes without hitting origin
- SWR window (30s) keeps product pages fresh while revalidating in
  background

**Why disable cache for cart/account?**
- These endpoints return user-specific data
- `Cache-Control: private, no-store` from origin is honored by bouine
- No risk of cross-user data leakage

**Why surrogate-key bans instead of exact-URL purges?**
- Product catalog changes affect many URLs (`/products/widget`,
  `/products/widget/reviews`, `/products/widget?variant=red`)
- A single ban on `products/.*` invalidates all variants
- More efficient than enumerating every URL

```bash
# Prometheus metrics:
curl http://localhost:19001/metrics | grep bouine_cache

# Admin stats:
curl http://localhost:19001/admin/stats
```

## Cleanup

```bash
docker compose down
```

## Extending this example

- Add a warm tier: set `warm_dir: /var/lib/bouine` and `warm_max_bytes: 10Go`
  in `config.yaml`, mount a volume to persist across restarts
- Enable cluster mode: add `mode: eventual` to the `cluster` section for
  faster convergence (trades strict consistency for speed)
- Add health checks: the mock origin supports `/healthz`; tune thresholds
  in `upstream_pools[].health_checks`

## Related

- [Configuration reference](../../docs/configuration.md)
- [Varnish migration guide](../../docs/migration/varnish.md)
- [Admin API](../../docs/admin-api.md)
