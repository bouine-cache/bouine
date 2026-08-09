# API Gateway Example

Short-TTL API caching with stale-while-revalidate and stale-if-error.
Suitable for REST/GraphQL APIs behind a microservice.

## Quick start

```bash
docker compose up -d

# First request: MISS
curl -sI http://localhost:8080/ | grep x-cache

# Second request: HIT
curl -sI http://localhost:8080/ | grep x-cache

# Check metrics
curl -s http://localhost:9000/metrics | grep bouine_requests
```

## Cleanup

```bash
docker compose down -v
```

## Config overview

- **TTL**: 30s (short, respects `Cache-Control` from origin)
- **SWR**: 10s (background revalidation while serving stale)
- **SIE**: 5m (serve stale on origin 5xx/timeout)
- **Health checks**: active `/healthz` + passive (consecutive 5xx)
- **Storage**: 512 MiB hot tier (RAM only, no warm tier)

## Trade-offs

- Short TTLs mean higher origin load but fresher data
- No warm tier — all cache lives in RAM and is lost on restart
- Cluster port is configured but not used in single-node mode
