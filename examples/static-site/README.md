# Static Site Example

CDN-like caching with long TTLs for static assets and heuristic
freshness for HTML. Suitable for static sites, documentation, and
marketing pages.

## Quick start

```bash
docker compose up -d

# HTML (5m TTL + SWR)
curl -sI http://localhost:8080/ | grep -E "x-cache|age"

# CSS asset (24h TTL)
curl -sI http://localhost:8080/assets/style.css | grep -E "x-cache|age"

# Second request: HIT
curl -sI http://localhost:8080/ | grep x-cache
```

## Cleanup

```bash
docker compose down -v
```

## Config overview

- **`/assets/`**: 24h TTL (static files are immutable)
- **`/` (fallback)**: 5m TTL + 60s SWR (HTML changes occasionally)
- **Storage**: 1 GiB hot tier (RAM only)
- **Health checks**: active `/healthz` every 10s

## Trade-offs

- No warm tier — all cache lives in RAM
- No cluster — single node only
- Origin `Cache-Control` headers are respected and override `ttl_default`
