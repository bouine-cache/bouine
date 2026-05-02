# Varnish to bouine Migration Guide

This guide helps operators migrate from Varnish Cache (v4.x–v6.x) to bouine. It covers conceptual mapping, side-by-side configuration examples, and validation strategies.

## Status

**Stable for production use.** This guide assumes familiarity with basic VCL concepts and bouine's YAML configuration model (see [configuration reference](../configuration/)).

---

## 0. Quick reference

| Varnish concept              | bouine equivalent           | Notes                                    |
|------------------------------|-----------------------------|------------------------------------------|
| VCL subroutines              | declarative YAML config     | bouine uses config, not code             |
| `vcl_recv`                   | `routes[].match`            | routing and request matching             |
| `vcl_hash`                   | `routes[].match.headers`    | implicit via match rules                 |
| `vcl_backend_fetch`          | `upstream_pools[]`          | backend pool config                     |
| `vcl_backend_response`       | origin `Cache-Control`      | bouine honors RFC 9111 strictly          |
| `beresp.ttl`                 | `cache.ttl_default`         | overridden by origin headers             |
| `beresp.grace`               | `cache.stale_while_revalidate` | SWR semantics                         |
| `ban()`                      | admin API `/admin/bans`     | HTTP-based purge API                     |
| `purge`                      | admin API `/admin/purge`    | exact-match invalidation                 |
| Varnish log (`-g request`)   | `access_logs`               | OpenTelemetry-compatible output          |
| `varnishstat`                | `/admin/stats`              | Prometheus-compatible metrics            |
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
    return (pass);            →        path: "/api/**"
  }                           →      cache:
  return (hash);              →        enabled: false
}                             →        pass_through: true
```

VCL gives you programmatic control at the cost of:
- Compilation and reload complexity (`varnishadm vcl.load` + `vcl.use`)
- Runtime errors that crash the cache (VCL syntax errors, runtime panics)
- Debugging difficulty (no stack traces, limited introspection)

bouine's declarative model:
- Hot-reloadable config (watch mode on SIGUSR1 or file change)
- Configuration validation before reload (`bouine config validate`)
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
- **Pipeline processing** (request transformation, response transformation)
- **Conditional request collapsing** (coalescing concurrent requests for the same cache key)
- **Origin pool selection** (round-robin, weighted, least-conn)

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
      path: "/**"
    cache:
      key:
        include_query: true
        include_host: true
        vary_headers: ["Accept", "Cookie"]  # automatic normalization
```

bouine automatically normalizes common headers (accept-encoding, cookie) to improve cache hit rates. The `vary_headers` list controls which request headers participate in the cache key.

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
  admin: "127.0.0.1:6082"

upstream_pools:
  - name: origin
    targets:
      - address: "origin.example.com:443"
        tls:
          enabled: true
          server_name: "origin.example.com"
    health_checks:
      active:
        path: "/health"
        interval: "5s"
        timeout: "2s"
        unhealthy_threshold: 3
    selection:
      strategy: round_robin

routes:
  # Images: 30-day TTL, 7-day SWR
  - match:
      path: "/images/**"
    cache:
      enabled: true
      ttl_default: "30d"
      stale_while_revalidate: "7d"
      pass_through: false
    response:
      remove_headers: ["Set-Cookie"]

  # Static assets: 7-day TTL, 3-day SWR
  - match:
      path: "/static/**"
    cache:
      enabled: true
      ttl_default: "7d"
      stale_while_revalidate: "3d"
      pass_through: false
    response:
      remove_headers: ["Set-Cookie"]

  # API endpoints: no caching
  - match:
      path: "/api/v[12]/**"
    cache:
      enabled: false
      pass_through: true

  # Product pages: 5-minute TTL, respect origin max-age
  - match:
      path: "/products/**"
    cache:
      enabled: true
      ttl_default: "5m"
      stale_while_revalidate: "1h"
      honor_origin_cache_control: true
      vary_headers: ["Accept-Language"]
    response:
      remove_headers: ["Set-Cookie"]

  # Default: pass through
  - match:
      path: "/**"
    cache:
      enabled: false
      pass_through: true

admin:
  enabled: true
  auth:
    type: bearer_token
    token_env: "BOUINE_ADMIN_TOKEN"
```

### 2.3 Key differences

| Aspect                 | Varnish VCL                          | bouine YAML                          |
|------------------------|--------------------------------------|--------------------------------------|
| Cookie stripping       | `unset req.http.Cookie` in vcl_recv  | implicit via `remove_headers`        |
| Session detection      | `if (req.http.Cookie ~ "...")`       | automatic (origin returns private)   |
| TTL per path           | `set beresp.ttl = ...` conditionals  | `ttl_default` in route config        |
| Backend health checks  | `.probe = {...}`                     | `health_checks.active`               |
| Debug headers          | `set resp.http.X-Cache`              | automatic (controlled by config)     |

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
      - address: "app1.example.com:8080"
        weight: 10
      - address: "app2.example.com:8080"
        weight: 5
    health_checks:
      active:
        path: "/health"
        interval: "5s"
    selection:
      strategy: weighted_round_robin
```

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

### 3.3 Conditional request collapsing

**Varnish:**
```vcl
sub vcl_recv {
    # Varnish 6+ has built-in request collapsing
    return (hash);
}
```

**bouine:** request collapsing is enabled by default for cache misses. Concurrent requests for the same cache key are coalesced into a single upstream fetch.

### 3.4 Purge operations

**Varnish:**
```bash
varnishadm ban "req.url ~ ^/products/"
varnishadm purge req.url "/products/item-123"
```

**bouine:**
```bash
# Ban (pattern-based)
curl -X POST http://127.0.0.1:6082/admin/bans \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"path": "/products/**"}'

# Purge (exact match)
curl -X POST http://127.0.0.1:6082/admin/purge \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"path": "/products/item-123"}'
```

---

## 4. What we don't support

### 4.1 VCL subroutine hooks

bouine does not expose VCL's fine-grained subroutine hooks:
- `vcl_init`, `vcl_fini`: use lifecycle hooks in your deployment
- `vcl_pipe`: not applicable (bouine is HTTP/1.1+2+3, not raw TCP)
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
- **Access logs**: OpenTelemetry-compatible structured logs to stdout
- **Metrics**: Prometheus endpoint at `/admin/stats` (or `/metrics` if configured)
- **Traces**: distributed tracing via OpenTelemetry

See [observability guide](../observability/) for details.

### 4.5 VSM (Varnish Shared Memory) files

bouine runs in-process; there are no VSM files, no shared memory segments, no `/var/lib/varnish` directories. All state is in-memory within the process.

---

## 5. Testing and validation

### 5.1 Configuration validation

```bash
bouine config validate /etc/bouine/config.yaml
```

Reports:
- Syntax errors
- Unknown fields
- Invalid upstream addresses
- Missing required fields

### 5.2 Dry-run mode

```bash
bouine serve --config /etc/bouine/config.yaml --dry-run
```

Validates config and prints effective configuration without starting the server.

### 5.3 Traffic replay

Use [gor replay](https://github.com/buger/goreplay) or similar tools to replay production traffic against a bouine instance:

```bash
# Capture from Varnish
gor --input-raw :6081 --output-file traffic.bin

# Replay to bouine
gor --input-file traffic.bin --output-http "http://bouine-host:8080"
```

Compare cache hit rates and response latency.

### 5.4 Cache key debugging

Use the admin API to inspect cached objects:

```bash
# List all cached keys
curl -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:6082/admin/cache/keys

# Fetch specific object
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:6082/admin/cache/object?path=/products/123"
```

Response includes:
- Cache key hash
- TTL and age
- Response headers
- Last fetch timestamp

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
      path: "/landing/**"
    cache:
      enabled: true
      vary_headers: ["Cookie"]  # vary on full cookie header
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

**bouine:** use upstream pool selection or a service mesh:

```yaml
upstream_pools:
  - name: us_origin
    targets:
      - address: "us.app.example.com:443"
  - name: eu_origin
    targets:
      - address: "eu.app.example.com:443"
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
      path: "/**"
    cache:
      enabled: true
      stale_if_error: true       # serve stale on 5xx
      stale_while_revalidate: "1h"
```

`stale_if_error: true` enables serving stale content when the origin returns a 5xx status or times out.

### 6.4 Surrogate keys (tag-based purging)

**Varnish:**
```vcl
sub vcl_backend_response {
    if (beresp.http.Surrogate-Key) {
        set beresp.http.xkey = beresp.http.Surrogate-Key;
    }
}
```

**bouine:** use the `Cache-Tags` response header:

```bash
# Origin returns: Cache-Tags: product-123, category-electronics
# Purge all objects tagged with "product-123"
curl -X POST http://127.0.0.1:6082/admin/bans \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"cache_tag": "product-123"}'
```

bouine automatically indexes objects by `Cache-Tags` for efficient tag-based invalidation.

---

## 7. Migration checklist

- [ ] **Audit VCL**: identify all `vcl_recv`, `vcl_backend_response`, `vcl_deliver` logic
- [ ] **Map subroutines**: use §3 to translate VCL constructs to bouine config
- [ ] **Write YAML**: start with the example in §2.2 as a template
- [ ] **Validate config**: run `bouine config validate`
- [ ] **Test in staging**: deploy bouine in staging with traffic replay
- [ ] **Compare metrics**: verify cache hit rate ≥ Varnish baseline
- [ ] **Monitor access logs**: check for unexpected `pass_through` or `no_store` decisions
- [ ] **Gradual rollout**: use weighted routing or feature flags to shift traffic
- [ ] **Decommission Varnish**: once metrics are stable for 7+ days

---

## 8. FAQ

**Q: Can I run Varnish and bouine side-by-side?**
A: Yes. Use a load balancer (HAProxy, NGINX) with weighted routing to gradually shift traffic.

**Q: Does bouine support HTTP/2 push?**
A: Yes. bouine automatically pushes resources referenced in `Link: <...>; rel=preload` headers.

**Q: How do I migrate custom VCL modules (vmods)?**
A: VCL modules cannot be ported directly. Identify the behavior and implement it as:
- Upstream service (for complex logic)
- Request/response transformation pipeline
- Route-level configuration

**Q: Is there a VCL-to-YAML converter?**
A: No automated tool exists. The declarative models are different enough that manual translation is required. Use this guide's examples as a starting point.

**Q: What if my origin doesn't return proper Cache-Control headers?**
A: Use `ttl_default` in route config as a fallback. bouine honors origin headers when present, falling back to the configured default.

**Q: Can I use Varnish's `std.log()` in bouine?**
A: bouine's access logs capture request/response details automatically. For custom logging, use response headers that your logging pipeline can extract.

---

## 9. Further reading

- [Configuration reference](../configuration/)
- [Cache decision flow](../cache-decisions.md)
- [Admin API](../admin-api/)
- [Observability guide](../observability/)
- [RFC 9111 (HTTP Caching)](https://datatracker.ietf.org/doc/html/rfc9111)
