# Migrating from NGINX to bouine

This document maps NGINX `proxy_cache` directives to bouine config.

## Directive mapping

| NGINX | bouine | Notes |
|-------|--------|-------|
| `proxy_cache_path /var/cache levels=1:2 keys_zone=zone:10m max_size=1g` | `storage.warm_dir: /var/lib/bouine`<br>`storage.warm_max_bytes: 1Go`<br>`storage.hot_max_bytes: 10Mo` | bouine uses sharded RAM + mmap tiers instead of filesystem levels. |
| `proxy_cache zone` | (automatic) | bouine has a single global store; no zone declaration needed. |
| `proxy_cache_valid 200 60m` | `routes[].cache.ttl_default: 60m` | Per-route TTL override when origin sends no `Cache-Control`. |
| `proxy_cache_valid 404 1m` | `routes[].cache.negative_ttl: 1m` | Caches 404, 405, 410, 501 responses for the configured duration. Zero disables negative caching. |
| `proxy_cache_valid 404 1m 500 10s` | `routes[].cache.negative_ttl: {404: 1m, 5xx: 10s}` | Per-status (or class) negative-caching TTLs, 400-599 only; the same key as the scalar form. A zero value explicitly disables caching for that status. |
| `proxy_cache_use_stale error timeout` | `routes[].cache.stale_if_error: 5m` | Serve stale on origin 5xx or timeout. Duration controls how long stale is served. |
| `proxy_cache_use_stale updating` | `routes[].cache.stale_while_revalidate: 30s` | Serve stale while revalidating in background. |
| `proxy_cache_key "$scheme$host$request_uri"` | (default) | bouine's default key is `scheme|host|path|query|method`. |
| `proxy_cache_bypass $http_x_no_cache` | Request `Cache-Control: no-cache` | bouine respects RFC 9111 request directives. |
| `proxy_no_cache $http_set_cookie` | `cache.cookies.allow_set_cookie: false` (default) | Responses with `Set-Cookie` not cached unless opt-in. |
| `proxy_pass http://backend` | `upstream_pools[].targets: [backend:80]` | |
| `rewrite ^/payment/orchestrator/callback/(.*)$ /scrooge/callback/$1 break;` | `routes[].request.path_rewrite: {match, replace}` | Regex path rewrite on the origin-bound request. Go RE2 (linear time, no ReDoS); first match replaced; query never matched or modified. Cache key keeps the public path. Mutually exclusive with `strip_prefix`. |
| `proxy_pass http://backend/internal/;` (URI rewriting via trailing proxy_pass path) | `routes[].request.strip_prefix: /public` | Only for pure prefix removal; for any other shape use `path_rewrite`. |
| `proxy_set_header Host $host` | (automatic) | bouine forwards `Host` from the client. |
| `proxy_next_upstream error timeout` | `health.passive.consecutive_5xx: 5` | Passive health ejection replaces retry-on-error. |
| `proxy_connect_timeout 5s` | `upstream_pools[].connect.timeout: 5s` | |
| `proxy_cache_lock on` | (automatic) | bouine coalesces concurrent misses for the same cache key (request collapsing). |
| `proxy_cache_valid 200 1d` for `/static/` | `routes[].cache.ttl_default: 86400s` on a `path_prefix: /static/` route | Per-route TTLs replace location blocks. |
| `proxy_cache_methods GET HEAD` | `routes[].match.methods: [GET, HEAD]` | Restrict a route to specific HTTP methods. |
| `proxy_cache_max_size` | `routes[].cache.max_object_size: 10Mo` | Skip caching responses larger than the configured size. |

## Key differences

- **No zone configuration**: bouine has a single embedded store. No `keys_zone` or `levels` to configure.
- **Clustering built-in**: NGINX requires third-party modules for cache sharing. bouine clusters natively via gossip.
- **RFC 9111 native**: bouine follows the caching RFC strictly. NGINX often requires manual `proxy_cache_valid` to enable caching.
- **No `lua-resty-*`**: bouine's config DSL covers most use cases that required Lua in NGINX.
- **Observability built-in**: Prometheus `/metrics` replaces the need for `ngx_http_stub_status` + exporters.
- **Purge and ban**: bouine's admin API (`POST /v1/purge`, `POST /v1/ban`) replaces NGINX's cache purge module. Ban supports predicate-based invalidation (regex on host/path, surrogate keys).
- **Soft-purge (refresh)**: `POST /v1/refresh` marks an object stale and triggers background revalidation on next access, similar to NGINX's `proxy_cache_purge` with a grace period.
- **Query-param stripping**: `routes[].cache.key.strip_query_params: [utm_source, fbclid]` drops tracking params from the cache key while still forwarding them to the origin.

## Example: API gateway

NGINX:
```nginx
proxy_cache_path /var/cache/nginx levels=1:2 keys_zone=api:10m max_size=1g;
server {
    listen 80;
    location /api/ {
        proxy_pass http://backend:8080;
        proxy_cache api;
        proxy_cache_valid 200 5m;
        proxy_cache_use_stale error timeout updating;
    }
}
```

bouine:
```yaml
listen:
  http: ":80"
  admin: ":9000"
storage:
  hot_max_bytes: 10Mo
  warm_dir: /var/lib/bouine
  warm_max_bytes: 1Go
upstream_pools:
  - name: backend
    targets: [backend:8080]
routes:
  - match: { path_prefix: /api/ }
    pool: backend
    cache:
      ttl_default: 5m
      stale_if_error: 5m
      stale_while_revalidate: 30s
```

## Example: path rewrite (nginx `rewrite ... break`)

NGINX:
```nginx
location ~ ^/payment/orchestrator/callback/* {
    rewrite ^/payment/orchestrator/callback/(.*)$ /scrooge/callback/$1 break;
    proxy_pass http://payment-orchestrator:8080;
}
```

bouine:
```yaml
upstream_pools:
  - name: payment-orchestrator
    targets: [payment-orchestrator:8080]
routes:
  - match: { path_prefix: /payment/orchestrator/callback }
    pool: payment-orchestrator
    request:
      path_rewrite:
        match: ^/payment/orchestrator/callback/(.*)$
        replace: /scrooge/callback/$1
```

The pattern is Go RE2 (linear time — no ReDoS; Go rejects repeat counts
above 1000 and programs whose nested counted repeats would exceed its
size cap, so oversized-program patterns never compile). Only the first
match is replaced, the query string is never matched or modified, and a
result that is not an absolute path (`/...`) is discarded and the
original path forwarded. Validation fails startup on: pattern or
template over 512 bytes; raw control bytes (CR/LF/NUL); raw bytes a
request-target cannot carry in the template (space, `?`, `#` — a `?`
would splice a second query delimiter in front of the preserved
original query); and any `$reference` that does not resolve against the
pattern's capture groups — write `${1}x` for "group 1 plus literal x"
(`$1x` is a reference to a group named `1x`, and a padded index like
`$01` is not an index at all — both would otherwise silently expand to
nothing). The loader's `${VAR}` environment interpolation only applies
to env-var-shaped names, so `${1}x` loads exactly as written. The cache
key, ban matching, and purges keep the public path — `POST /v1/purge`
and `/v1/ban` address the URLs clients request, not the rewritten
upstream paths.
