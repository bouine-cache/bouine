# Varnish to bouine Migration Guide

This guide helps operators migrate from Varnish Cache (v4.x–v6.x) to bouine. It covers conceptual mapping, side-by-side configuration examples, and validation strategies.

---

## 0. Quick reference

| Varnish concept              | bouine equivalent           | Notes                                    |
|------------------------------|-----------------------------|------------------------------------------|
| VCL subroutines              | declarative YAML config     | bouine uses config, not code             |
| `vcl_recv`                   | `routes[].match`            | routing and request matching             |
| `vcl_hash`                   | `routes[].match` + `vary_headers` | implicit via match rules            |
| `vcl_backend_fetch`          | `upstream_pools[]`          | backend pool config                      |
| `vcl_backend_response`       | origin `Cache-Control`      | bouine honors RFC 9111 strictly          |
| `beresp.ttl`                 | `cache.ttl_default`          | fallback when origin sends no freshness  |
| `beresp.grace`               | `cache.stale_while_revalidate` | SWR semantics                         |
| `beresp.keep`                | `cache.stale_if_error`      | serve stale on origin failure            |
| `ban()`                      | admin API `POST /v1/ban`    | predicate-based invalidation             |
| `purge`                      | admin API `POST /v1/purge`  | exact-match invalidation                 |
| `return (pass)`              | `cache.enabled: false`      | bypass cache for a route                  |
| Varnish log (`-g request`)   | `slog` access logs          | structured JSON logs to stdout           |
| `varnishstat`                | `GET /metrics`              | Prometheus-compatible metrics            |
| VSM/shared memory            | in-process memory           | no mmap, no VSM files                    |

---

## 1. Conceptual mapping

### 1.1 From VCL to declarative config

Varnish requires operators to write VCL subroutines (`vcl_recv`, `vcl_backend_response`, etc.) to control caching behavior. bouine replaces this imperative model with a declarative YAML configuration:

```
Varnish (imperative)          →  bouine (declarative)
─────────────────────────────────────────────────────────
sub vcl_recv {                →  routes:
  if (req.url ~ "/api/") {    →    - match:
    return (pass);            →        path_prefix: /api/
  }                           →      cache:
  return (hash);              →        enabled: false
}                             →
```

VCL gives you programmatic control at the cost of:
- Compilation and reload complexity (`varnishadm vcl.load` + `vcl.use`)
- Runtime errors that crash the cache (VCL syntax errors, runtime panics)
- Debugging difficulty (no stack traces, limited introspection)

bouine's declarative model:
- Configuration applied by rolling the pod (standard Kubernetes rolling update)
- Configuration validation at startup; invalid configs fail fast
- Structured error reporting with line numbers

### 1.2 Request processing model

Both systems follow a similar request flow, but with different terminology:

```
Varnish                      →  bouine
─────────────────────────────────────────────────────────
client_req → vcl_recv         →  request → route matching
            → hash lookup     →         → cache lookup
            vcl_hit/vcl_miss  →         → cache hit/miss decision
            vcl_backend_fetch →         → upstream fetch
            vcl_backend_resp  →         → response processing
            vcl_deliver       →         → response delivery
```

bouine adds explicit layers for:
- **Request collapsing** (coalescing concurrent requests for the same cache key)
- **Origin pool selection** (round-robin across healthy targets)
- **Peer fetch** (cluster-wide cache lookup before hitting origin)

### 1.3 Cache key construction

**Varnish:**
```vcl
sub vcl_hash {
    hash_data(req.url);
    hash_data(req.http.host);
    if (req.http.Cookie) {
        hash_data(regsub(req.http.Cookie, ".*\bsession_id=([^;]+);.*", "\1"));
    }
}
```

**bouine:**
```yaml
routes:
  - match:
      path_prefix: /
    cache:
      vary_headers: [Accept, Accept-Language]  # automatic normalization
```

bouine automatically normalizes common headers (Accept-Encoding, Cookie) to improve cache hit rates. The `vary_headers` list controls which request headers participate in the cache key. Query parameters are sorted by key automatically.

---

## 2. Side-by-side example: E-commerce site

### 2.1 Varnish configuration (VCL)

```vcl
vcl 4.1;

backend default {
    .host = "origin.example.com";
    .port = "443";
    .connect_timeout = 5s;
    .first_byte_timeout = 30s;
    .probe = {
        .url = "/health";
        .timeout = 2s;
        .interval = 5s;
        .expected_response = 200;
    }
}

sub vcl_recv {
    # Strip tracking cookies for cacheable content
    if (req.url ~ "^/(images/|static/|css/|js/)") {
        unset req.http.Cookie;
        return (hash);
    }

    # API endpoints: pass through
    if (req.url ~ "^/api/v[12]/") {
        return (pass);
    }

    # Personalized content: bypass cache if session cookie present
    if (req.http.Cookie ~ "session_id=") {
        return (pass);
    }

    # Product pages: cache with short TTL
    if (req.url ~ "^/products/") {
        return (hash);
    }

    return (pass);
}

sub vcl_backend_response {
    # Images: 30 days
    if (bereq.url ~ "^/images/") {
        set beresp.ttl = 30d;
        set beresp.grace = 7d;
    }

    # Static assets: 7 days
    if (bereq.url ~ "^/static/") {
        set beresp.ttl = 7d;
        set beresp.grace = 3d;
    }

    # Product pages: 5 minutes with SWR
    if (bereq.url ~ "^/products/") {
        set beresp.ttl = 5m;
        set beresp.grace = 1h;
    }

    # Remove cookies from cached responses
    if (beresp.ttl > 0s) {
        unset beresp.http.Set-Cookie;
    }
}

sub vcl_deliver {
    # Add debug headers
    if (obj.hits > 0) {
        set resp.http.X-Cache = "HIT";
    } else {
        set resp.http.X-Cache = "MISS";
    }
}
```

### 2.2 bouine configuration (YAML)

```yaml
listen:
  http: ":80"
  admin: ":9000"

upstream_pools:
  - name: origin
    targets:
      - origin.example.com:443
    health:
      active:
        path: /health
        interval: 5s
        timeout: 2s
        unhealthy_threshold: 3

routes:
  # Images: 30-day TTL, 7-day SWR
  - match:
      path_prefix: /images/
    pool: origin
    cache:
      ttl_default: 720h        # 30d
      stale_while_revalidate: 168h  # 7d
    response:
      header_remove: [Set-Cookie]

  # Static assets: 7-day TTL, 3-day SWR
  - match:
      path_prefix: /static/
    pool: origin
    cache:
      ttl_default: 168h        # 7d
      stale_while_revalidate: 72h   # 3d
    response:
      header_remove: [Set-Cookie]

  # API endpoints: no caching
  - match:
      path_prefix: /api/v1/
    pool: origin
    cache:
      enabled: false

  - match:
      path_prefix: /api/v2/
    pool: origin
    cache:
      enabled: false

  # Product pages: 5-minute TTL with SWR
  - match:
      path_prefix: /products/
    pool: origin
    cache:
      ttl_default: 5m
      stale_while_revalidate: 1h
      vary_headers: [Accept-Language]
    response:
      header_remove: [Set-Cookie]

  # Default: pass through
  - match:
      path_prefix: /
    pool: origin
    cache:
      enabled: false

admin:
  token_env: BOUINE_ADMIN_TOKEN
```

### 2.3 Key differences

| Aspect                 | Varnish VCL                          | bouine YAML                          |
|------------------------|--------------------------------------|--------------------------------------|
| Cookie stripping       | `unset req.http.Cookie` in vcl_recv  | `response.header_remove: [Set-Cookie]` |
| Session detection      | `if (req.http.Cookie ~ "...")`       | automatic (origin returns private)   |
| TTL per path           | `set beresp.ttl = ...` conditionals  | `ttl_default` in route config        |
| Backend health checks  | `.probe = {...}`                     | `health.active`                      |
| Debug headers          | `set resp.http.X-Cache`              | automatic (`X-Cache: HIT/MISS/STALE`) |

---

## 3. How VCL constructs map

### 3.1 Backend configuration

**Varnish:**
```vcl
backend primary {
    .host = "app1.example.com";
    .port = "8080";
    .probe = { .url = "/health"; .interval = 5s; }
}
backend fallback {
    .host = "app2.example.com";
    .port = "8080";
}
```

**bouine:**
```yaml
upstream_pools:
  - name: app
    targets:
      - app1.example.com:8080
      - app2.example.com:8080
    health:
      active:
        path: /health
        interval: 5s
      passive:
        consecutive_5xx: 5
        eject_for: 30s
```

bouine uses round-robin selection across healthy targets automatically. No strategy field is needed.

### 3.2 Cache-Control header handling

**Varnish:**
```vcl
sub vcl_backend_response {
    if (beresp.http.Cache-Control ~ "no-cache") {
        set beresp.uncacheable = true;
    }
}
```

**bouine:** bouine strictly honors RFC 9111 `Cache-Control` directives. No configuration needed.

- `no-store`: never cache
- `no-cache`: require revalidation
- `max-age=N`: respect TTL
- `private`: bypass cache
- `s-maxage=N`: override max-age for shared caches

### 3.3 Request collapsing

**Varnish:**
```vcl
sub vcl_recv {
    # Varnish 6+ has built-in request collapsing
    return (hash);
}
```

**bouine:** request collapsing is enabled by default for cache misses. Concurrent requests for the same cache key are coalesced into a single upstream fetch.

### 3.4 Purge and ban operations

**Varnish:**
```bash
varnishadm ban "req.url ~ ^/products/"
varnishadm purge req.url "/products/item-123"
```

**bouine:**
```bash
# Ban (predicate-based — supports host_regex, path_regex, surrogate_key)
curl -X POST http://127.0.0.1:9000/v1/ban \
  -H "Authorization: Bearer $BOUINE_ADMIN_TOKEN" \
  -d '{"path_regex": "^/products/"}'

# Purge (exact match by URL)
curl -X POST http://127.0.0.1:9000/v1/purge \
  -H "Authorization: Bearer $BOUINE_ADMIN_TOKEN" \
  -d '{"url": "https://example.com/products/item-123"}'

# Refresh / soft-purge (mark stale, revalidate on next access)
curl -X POST http://127.0.0.1:9000/v1/refresh \
  -H "Authorization: Bearer $BOUINE_ADMIN_TOKEN" \
  -d '{"url": "https://example.com/products/item-123"}'
```

### 3.5 Surrogate keys (tag-based purging)

**Varnish:**
```vcl
sub vcl_backend_response {
    if (beresp.http.Surrogate-Key) {
        set beresp.http.xkey = beresp.http.Surrogate-Key;
    }
}
```

**bouine:** bouine automatically indexes objects by surrogate keys from the `Surrogate-Key` or `X-Cache-Tags` response header.

```bash
# Origin returns: Surrogate-Key: product-123, category-electronics
# Purge all objects tagged with "product-123"
curl -X POST http://127.0.0.1:9000/v1/ban \
  -H "Authorization: Bearer $BOUINE_ADMIN_TOKEN" \
  -d '{"surrogate_key": "product-123"}'
```

---

## 4. What we don't support

### 4.1 VCL subroutine hooks

bouine does not expose VCL's fine-grained subroutine hooks:
- `vcl_init`, `vcl_fini`: use lifecycle hooks in your deployment
- `vcl_pipe`: not applicable (bouine is HTTP/1.1+2, not raw TCP)
- `vcl_synth`: use a separate service for dynamic content generation

### 4.2 Custom hash functions

Varnish allows arbitrary hash functions. bouine uses a deterministic hash based on:
- Request path (normalized)
- Query string (sorted by key)
- Headers listed in `vary_headers`
- Host header

If you need custom cache key logic, consider upstream request transformation.

### 4.3 ESI (Edge Side Includes)

bouine does not implement ESI. For dynamic content composition:
- Use a service mesh (Istio, Linkerd) for request routing
- Implement composition in your application layer
- Use GraphQL or similar for aggregation

### 4.4 varnishlog / varnishncsa

bouine emits:
- **Access logs**: structured `slog` JSON to stdout
- **Metrics**: Prometheus endpoint at `GET /metrics` on the admin port
- **Traces**: distributed tracing via OpenTelemetry

### 4.5 VSM (Varnish Shared Memory) files

bouine runs in-process; there are no VSM files, no shared memory segments, no `/var/lib/varnish` directories. All state is in-memory within the process.

---

## 5. Testing and validation

### 5.1 Configuration validation

bouine validates configuration at startup. To test before deploying:

```bash
# Start bouine and check for config errors — it will fail fast and log
# structured error messages with line numbers if the config is invalid.
./bouine serve --config /etc/bouine/config.yaml
```

### 5.2 Traffic replay

Use [gor replay](https://github.com/buger/goreplay) or similar tools to replay production traffic against a bouine instance:

```bash
# Capture from Varnish
gor --input-raw :6081 --output-file traffic.bin

# Replay to bouine
gor --input-file traffic.bin --output-http "http://bouine-host:8080"
```

Compare cache hit rates and response latency.

### 5.3 Cache debugging

Use the admin API to inspect cluster state and metrics:

```bash
# Cluster peers
curl -H "Authorization: Bearer $BOUINE_ADMIN_TOKEN" \
  http://127.0.0.1:9000/v1/cluster/peers

# Prometheus metrics (cache hit rate, object count, latency)
curl http://127.0.0.1:9000/metrics | grep bouine_
```

---

## 6. Common patterns

### 6.1 A/B testing with cache bypass

**Varnish:**
```vcl
sub vcl_recv {
    if (req.http.Cookie ~ "ab-test=variant-b") {
        return (pass);
    }
}
```

**bouine:** rely on origin to return `Cache-Control: private` for A/B test variants, or use `vary_headers`:

```yaml
routes:
  - match:
      path_prefix: /landing/
    pool: origin
    cache:
      vary_headers: [Cookie]  # vary on full cookie header
```

### 6.2 Geolocation-based routing

**Varnish:**
```vcl
sub vcl_recv {
    set req.http.X-Country = geoip.country_code;
    if (req.http.X-Country == "US") {
        set req.backend_hint = us_backend;
    }
}
```

**bouine:** use separate upstream pools and route matching:

```yaml
upstream_pools:
  - name: us_origin
    targets:
      - us.app.example.com:443
  - name: eu_origin
    targets:
      - eu.app.example.com:443
```

Route selection based on the `X-Forwarded-For` or `CF-IPCountry` header (if using Cloudflare).

### 6.3 Graceful degradation (origin failure)

**Varnish:**
```vcl
sub vcl_backend_response {
    set beresp.grace = 24h;
}
sub vcl_synth {
    if (resp.status == 503) {
        # Serve from grace
    }
}
```

**bouine:**

```yaml
routes:
  - match:
      path_prefix: /
    pool: origin
    cache:
      stale_if_error: 1h        # serve stale on 5xx or timeout
      stale_while_revalidate: 1h
```

`stale_if_error` takes a duration; when the origin returns 5xx or times out, bouine serves the stale cached response for up to that duration.

### 6.4 Query-param stripping

**Varnish:**
```vcl
sub vcl_hash {
    # Strip tracking params from cache key
    hash_data(regsub(req.url, "(utm_source|fbclid)=[^&]+&?", ""));
}
```

**bouine:**

```yaml
routes:
  - match:
      path_prefix: /
    pool: origin
    cache:
      key:
        strip_query_params: [utm_source, fbclid, gclid]
```

Tracking params are removed from the cache key but still forwarded to the origin. Zero added allocations on the hit path.

---

## 7. Migration checklist

- [ ] **Audit VCL**: identify all `vcl_recv`, `vcl_backend_response`, `vcl_deliver` logic
- [ ] **Map subroutines**: use section 3 to translate VCL constructs to bouine config
- [ ] **Write YAML**: start with the example in section 2.2 as a template
- [ ] **Validate config**: run `bouine serve --config config.yaml` and check for startup errors
- [ ] **Test in staging**: deploy bouine in staging with traffic replay
- [ ] **Compare metrics**: verify cache hit rate matches or exceeds Varnish baseline via `/metrics`
- [ ] **Monitor access logs**: check for unexpected cache bypasses or `no_store` decisions
- [ ] **Gradual rollout**: use weighted routing or a load balancer to shift traffic
- [ ] **Decommission Varnish**: once metrics are stable for 7+ days

---

## 8. FAQ

**Q: Can I run Varnish and bouine side-by-side?**

A: Yes. Use a load balancer (HAProxy, NGINX) with weighted routing to gradually shift traffic.

**Q: Does bouine support HTTP/2 push?**

A: No. HTTP/2 push has been deprecated by browsers and RFC 9113. bouine focuses on standard caching and `Link: rel=preload` header forwarding.

**Q: How do I migrate custom VCL modules (vmods)?**

A: VCL modules cannot be ported directly. Identify the behavior and implement it as:
- Upstream service (for complex logic)
- Route-level configuration (for cache policy)
- Response header manipulation (for header-based logic)

**Q: Is there a VCL-to-YAML converter?**

A: No automated tool exists. The declarative models are different enough that manual translation is required. Use this guide's examples as a starting point.

**Q: What if my origin doesn't return proper Cache-Control headers?**

A: Use `ttl_default` in route config as a fallback. bouine honors origin headers when present, falling back to the configured default.

**Q: Can I use Varnish's `std.log()` in bouine?**

A: bouine's access logs capture request/response details automatically in structured JSON (`slog`). For custom logging, use response headers that your logging pipeline can extract.

---

## 9. Further reading

- [Architecture reference](../architecture.md)
- [Configuration reference](https://bouine.org/docs/configuration/)
- [Admin API](https://bouine.org/docs/reference/)
- [RFC 9111 (HTTP Caching)](https://datatracker.ietf.org/doc/html/rfc9111)
