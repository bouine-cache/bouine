# bouine — Implementation Plan

`bouine` is a horizontally-scalable, observability-first HTTP reverse-proxy cache
written in Go 1.26. It targets the same problem space as Varnish (RFC 9111 cache,
fast purge, predicate-based bans) but is designed from day one for Kubernetes,
multi-instance clustering, and first-class metrics/traces/logs.

This document is the single source of truth for the roadmap. Each phase has
explicit deliverables, exit criteria, and the tests that gate moving forward.

---

## 1. Goals & Non-Goals

### 1.1 Goals
- **Protocol coverage** — terminate HTTP/1.1 and HTTP/2, with
  TLS, ALPN, and HTTP upgrade.
- **RFC 9111 compliance** — score at least on par with Varnish on
  [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests).
- **Embedded storage** — no external KV (Redis, Memcached, etcd…). Hot tier in
  RAM, warm tier on local disk, optional cluster fan-out.
- **Multi-instance clustering** — consistent-hash sharding, peer fetch,
  gossip-based membership; eventual consistency is acceptable.
- **High performance** — zero-allocation hot path on hits, regression-gated by a
  benchmark CI job (>= Varnish single-node RPS for the canonical workload).
- **Fault tolerance** — active and passive upstream health checks, hedged
  requests, circuit breakers, request collapsing.
- **Operator UX** — Cobra-based CLI, declarative YAML/HCL config, hot reload,
  purge & ban APIs (URL, regex, full).
- **Observability built-in** — OpenTelemetry traces, Prometheus metrics, slog
  structured access logs, pprof.
- **Advanced caching** — prefetching, stale-while-revalidate, stale-if-error,
  request collapsing, Vary canonicalisation.
- **AI-assisted insights (later phase)** — traffic analysis daemon plus a
  client-facing dashboard suggesting cache-strategy improvements.

### 1.2 Non-Goals
- WAF / DDoS scrubbing (delegated to a sidecar or Layer-7 LB).
- Generic L3 load-balancing (we are an L6 HTTP cache).
- WebSockets as cached objects (passed through but never cached).
- Becoming a drop-in VCL interpreter — we expose a richer config DSL instead.
- **End-user authentication / authorization on the data plane** —
  bouine forwards `Authorization` / `Cookie` headers and lets the origin
  enforce. Deferred (see §18).
- **Per-route request-rate limiting on the data plane** — only connection
  and slow-body backpressure ship in v1.0. Deferred (see §18).
- **Encryption at rest of the warm tier** — delegated to the cloud volume
  (EBS/PD/Azure Disk) per §17.2.
- **Built-in ACME / cert issuance** — bouine reloads certs from disk; ACME
  is delegated to cert-manager or an external sidecar in v1.0 (see §18).
- **Multi-tenant isolation beyond virtual host** — operators with strong
  tenancy needs run one bouine deployment per tenant in v1.0 (see §18).

---

## 2. High-Level Architecture

Layered design, each layer testable in isolation. Lower numbers are closer to
the wire.

```
┌──────────────────────────────────────────────────────────────────────┐
│ L8  AI Insights & Dashboard         (phase 6)                        │
├──────────────────────────────────────────────────────────────────────┤
│ L7  Observability         (metrics · traces · logs · pprof)          │
├──────────────────────────────────────────────────────────────────────┤
│ L6  Control Plane          admin API · purge · config · dashboard    │
├──────────────────────────────────────────────────────────────────────┤
│ L5  Cluster                gossip · hashring · peer fetch · digests  │
├──────────────────────────────────────────────────────────────────────┤
│ L4  Origin / Upstream      pool · health · hedge · circuit breaker   │
├──────────────────────────────────────────────────────────────────────┤
│ L3  Cache Engine           RFC 9111 · Vary · revalidation · SWR      │
├──────────────────────────────────────────────────────────────────────┤
│ L2  Storage                hot (RAM) · warm (mmap) · eviction · WAL  │
├──────────────────────────────────────────────────────────────────────┤
│ L1  Server                 HTTP/1.1 · HTTP/2 · TLS · route            │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.1 HTTP stacks

The daemon runs on a **single** HTTP implementation (ADR-0006):

- `net/http` — for HTTP/1.1 + HTTP/2 (via `http2.ConfigureServer` +
  ALPN). Serves both the **data plane** (proxy/cache) and the **control
  plane** (admin API, metrics, pprof). Plaintext HTTP/2 (h2c) via
  `golang.org/x/net/http2/h2c` is available for in-mesh traffic. The control plane is a plain
`*http.Server` on its own port, using `net/http.ServeMux` (Go 1.22+
pattern routing). This keeps the entire handler chain, middleware, and
test surface uniform.

Fiber was used initially but dropped in phase 1 (ADR-0006) because it
added a third HTTP stack (`valyala/fasthttp`) and 7 transitive
dependencies for a surface that serves ≤ 10 RPS.

### 2.2 Module layout (Go)

```
/cmd/bouine                  Cobra entrypoint
/internal/server             L1 — HTTP/1, /2, TLS, route matching
/internal/cache              L3 — RFC 9111 state machine, Vary, conditionals
/internal/storage            L2 — RAM tier, mmap tier, eviction, WAL
/internal/storage/sieve      SIEVE eviction implementation
/internal/origin             L4 — upstream pool, health, hedge, breaker
/internal/cluster            L5 — memberlist gossip, consistent hash, peer fetch
/internal/admin              L6 — net/http admin: purge, ban, config, dash
/internal/observability      L7 — OTEL, Prom, slog, pprof
/internal/config             config loader, schema, hot reload
/internal/ai                 L8 — traffic analytics (phase 8)
/web/dashboard               L8 — HTMX dashboard templates (phase 6)
/pkg/bouineapi               public Go SDK (purge/ban/refresh/stats client)
/pkg/api                     shared types between SDK, admin server, dashboard
/test/integration            docker-compose driven scenarios
/test/cachetests             http-tests/cache-tests harness
/bench                       benchmark suite + nightly comparison
```

### 2.3 Cross-cutting principles
- **Zero-alloc hot path** — pooled buffers, `sync.Pool` for headers, no
  `fmt.Sprintf` on lookups.
- **Context everywhere** — every request carries an OTEL span context.
- **Interface boundaries** — `cache.Store`, `origin.Fetcher`, `cluster.Peer`
  are interfaces so each layer can be unit-tested with fakes.
- **No global state** — the daemon is a single `Engine` struct, instantiated
  by the Cobra command. Easier to test, easier to embed.

### 2.4 Security model
The full threat model lives in [`docs/security/threat-model.md`](docs/security/threat-model.md).
It is the source of truth for trust boundaries, attacker classes, and
the controls that every layer must preserve. PRs touching a threat row
(`Txx`) update the document in the same change; CI enforces this via a
doc-coverage check. Headline guarantees:

- **TB1 — Internet ↔ data plane**: TLS terminates here. Strict RFC 9112
  parser blocks request smuggling (T05). Header / URL / body caps are
  always enforced (T37). HTTP/2 reset-flood mitigations are on
  by default (T10).
- **TB3 — bouine ↔ origin**: TLS verified by default; mTLS, custom CA
  bundles, optional SPKI pinning, server-name override — all configured
  per upstream pool (see §6.1).
- **TB4 — bouine ↔ peer bouine**: cluster mTLS is mandatory, on its own
  CA, with versioned wire protocol (see §5.5).
- **TB5 — operator ↔ admin API**: bearer token (constant-time compare)
  or mTLS; admin port is never bound externally in default manifests.
  Every write action is audit-logged with token-ID hash + predicate.
- **TB6 — process ↔ disk**: warm-tier segments are `0600`; encryption-
  at-rest is delegated to the cloud volume.

---

## 3. Cache Engine (L3) — RFC 9111 details

The engine is implemented as a deterministic state machine driven by request
and stored-response metadata. Its only inputs are an `http.Request`, the
matching `*StoredObject` (if any), and "now"; its outputs are an action
(`HIT`, `MISS`, `REVALIDATE`, `STALE_HIT`, `BYPASS`) and a `Disposition`
(what to write back, what to forward).

### 3.1 RFC surface

Mandatory surface area to match Varnish on cache-tests (RFC 9110, 9111,
9112, plus RFC 5861 for SWR/SIE):

- Request methods: `GET`, `HEAD`, `POST` (only when explicit), `PURGE`.
- Cacheability rules: `Cache-Control` (request & response), `Pragma`,
  `Authorization`, `Set-Cookie`, `Expires`, heuristic freshness.
- Response directives: `no-store`, `no-cache`, `private`, `public`,
  `max-age`, `s-maxage`, `must-revalidate`, `proxy-revalidate`,
  `immutable`, `stale-while-revalidate`, `stale-if-error`.
- Request directives: `no-cache`, `no-store`, `max-age`, `min-fresh`,
  `max-stale`, `only-if-cached`.
- `Vary` — canonical normalization, secondary cache key, `Vary: *` opt-out.
- Conditional revalidation: `ETag` (strong & weak), `Last-Modified`,
  `If-None-Match`, `If-Modified-Since`, `If-Match`, `If-Unmodified-Since`,
  `If-Range`.
- Range responses (206), partial cache (cache full body, serve range from
  stored bytes when possible).
- `Age` calculation including `Date` skew and forward proxies.
- Warnings / `Warning` header (RFC 9111 still allows generating them).
- Trailer headers, `Transfer-Encoding: chunked` for HTTP/1.1.
- Method invalidation (`POST`/`PUT`/`DELETE` evict matching URLs).

Each rule is unit-tested before wiring into the cache-tests harness so
regressions are caught at the smallest scope.

### 3.2 Cache key construction

The canonical cache key is deterministic and stable across nodes — the
cluster depends on every node computing the same `xxhash64` for the same
request.

The primary key is built from, in this order:

1. **Scheme** — lowercased (`http` / `https`).
2. **Host** — lowercased, IDN → punycode, default port stripped
   (`:80` for http, `:443` for https).
3. **Path** — percent-decoded then re-encoded canonically, collapsed
   duplicate slashes, no fragment.
4. **Query** — parameters sorted lexicographically; default policy keeps
   all parameters, per-route allow-list (`cache.key.query.allow`) or
   deny-list (`cache.key.query.deny`) prunes tracking parameters
   (`utm_*`, `gclid`, `fbclid`).
5. **Method** — only `GET` and `HEAD` share key space (`HEAD` lookups
   may serve a stored `GET` per RFC 9111).

The secondary key (Vary) is derived per stored variant from the canonical
set of headers listed in the response's `Vary`. Headers participate in
the cache key **only** via `Vary` or an explicit per-route
`cache.key.include_headers` allow-list — never implicitly. This is the
primary defense against cache-poisoning via unkeyed input (threat T06,
T07 in the threat model).

### 3.3 Compression policy

Sensible default for Varnish/NGINX replacement: **store responses in the
encoding the origin produced**, treat `Accept-Encoding` as an implicit
`Vary`, and bucket client `Accept-Encoding` into a small canonical set
to keep variant count bounded.

- Canonical encoding buckets: `br`, `zstd`, `gzip`, `identity` — anything
  else collapses to `identity`.
- Per-route knob `cache.compression.mode`:
  - `passthrough` (default) — store as-is, one variant per
    encoding-bucket. Same behavior as NGINX `gzip_static off` + Vary.
  - `normalize_identity` — request `identity` from origin, store one
    copy, recompress on egress using the bucket the client asked for.
    Higher hit ratio, costs CPU on egress. **Forbidden on routes that
    serve responses containing secrets** (mitigates BREACH-class
    oracles, threat T25).
- bouine never compresses an origin response that came uncompressed if
  the origin explicitly set `Cache-Control: no-transform`.
- HTTP/2 HPACK never indexes `Authorization`, `Cookie`,
  `Set-Cookie` (RFC 7541 §7.1.3).

### 3.4 Cookie & authorization policy

A naive cache that treats `Cookie` as varying explodes the keyspace; a
naive cache that treats `Set-Cookie` as cacheable leaks sessions.
Defaults follow the RFC 9111 letter and the NGINX/Varnish status quo:

- **Request `Cookie` header** — does NOT participate in the cache key by
  default. Per-route opt-in: `cache.cookies.key: [name1, name2]`. Values
  are length-capped (default 256 bytes), canonicalized (sorted by name),
  and missing names hash to the empty string for stability.
- **Response `Set-Cookie`** — by default, a response carrying
  `Set-Cookie` is NOT stored (treated as private). Per-route opt-in:
  `cache.cookies.allow_set_cookie: true` requires an explicit operator
  acknowledgement and emits a boot-time warning naming the route.
- **`Authorization` request header** — per RFC 9111 §3.5, responses to
  authorized requests are NOT stored unless the response itself carries
  one of `must-revalidate`, `public`, or `s-maxage`. Implemented exactly
  as written; no operator override (deviating here invites cross-user
  leaks, threat T13).

### 3.5 Implementation note

Each rule above is unit-tested before wiring into the cache-tests
harness so regressions are caught at the smallest scope.

---

## 4. Storage (L2) — Embedded, Multi-Tier

### 4.1 Tiers
- **L0 / Hot** — in-memory `map[uint64]*Entry` keyed by 64-bit xxhash of the
  canonical cache key; sharded N=runtime.NumCPU() ways to avoid contention.
- **L1 / Warm** — append-only segmented mmap files (à la Varnish "file"
  storage), 64 MiB segments, garbage collected by tombstone compaction.
- **Spillover policy** — small objects pinned in
  L0, large/cold objects demoted to L1.

### 4.2 Eviction
- Primary: **SIEVE** (simple, near-LRU-K performance, O(1) per op).

- Tombstones for in-flight invalidations to avoid races with revalidation.

### 4.3 Durability
- WAL only for the index (not bodies) — survives crash with a warm restart
  in seconds; bodies live in mmap segments validated on open.
- Optional `--no-persist` mode for ephemeral pods.

### 4.4 Public Store interface
```go
type Store interface {
    Get(ctx context.Context, key Key) (*Object, error)
    Put(ctx context.Context, key Key, obj *Object) error
    Delete(ctx context.Context, key Key) error
    Ban(ctx context.Context, predicate BanExpr) (int, error)
    Stats() Stats
}
```

---

## 5. Clustering (L5)

### 5.1 Membership
- `hashicorp/memberlist` for gossip; nodes publish a hash ring digest.
- K8s-friendly: a headless `Service` + `StatefulSet` is enough; nodes
  bootstrap via DNS SRV lookups, no external coordinator.

### 5.2 Sharding
- Consistent hash with bounded loads (Google's "Consistent Hashing with
  Bounded Loads") to avoid hot spots.
- 256 virtual nodes per real node; weight is configurable for heterogeneous
  pods.

### 5.3 Peer fetch protocol
- Internal HTTP/2 over mTLS (port `:8443` by default).
- On miss: owner first, two-hop fallback, then origin.
- Cuckoo-filter-based digests gossiped every 5 s to short-circuit fetches
  for keys we know peers don't have.
- "Bouine-Hop" header increments to bound traversal depth (default 2).

### 5.4 Consistency
- Writes are local-first; eventual replication is fire-and-forget to N-1
  replicas with hinted handoff in a bounded queue.
- Purges are broadcast via gossip + anti-entropy reconciliation every 30 s
  (configurable). A purge is monotonic — once a TTL marker is set, late
  writes for that key are rejected until TTL expires.

### 5.5 Wire protocol versioning

The cluster moves through rolling deploys constantly. Every gossip and
peer-fetch frame carries a version header so mixed-version clusters
work during a rollout and break cleanly when they cannot.

- **Framing**: every cluster message begins with `magic` (4 bytes,
  `"BOUI"`) + `version` (`uint16`, big-endian) + `kind` (`uint16`).
  Versions are integer; bumps are deliberate.
- **Negotiation**: on peer-fetch connection (HTTP/2 over mTLS) the
  client sends an `X-Bouine-Cluster-Version` header on the first
  request; the server replies with its accepted-range header
  (`X-Bouine-Cluster-Accepts: 3-5`). Mismatch outside the accepted range
  causes the peer-fetch to fall back to the origin path; the cluster
  does NOT panic.
- **Compatibility window**: every release supports the current
  protocol version and the previous one (N and N-1). A protocol bump is
  released in two steps:
  1. Release X — both N and N+1 are spoken (preferred N+1).
  2. Release X+1 — N is dropped.
  Operators must complete a full rollout of X across all pods before
  upgrading to X+1.
- **Breaking changes**: any bump to N+1 is documented in `CHANGELOG.md`
  and emits a metric (`bouine_cluster_protocol_mismatch_total`) when
  encountered, so monitoring can catch a stuck rollout.
- **Tests**: an integration scenario in `test/integration/cluster_mixed`
  boots a 3-node cluster with two adjacent versions and validates
  bidirectional traffic + purge propagation.

---

## 6. Upstream / Origin (L4)

- Connection pool per upstream, keyed by host:port + TLS profile.
- **Active health checks** — optional, configurable: HTTP probe, expected
  status codes, expected body regex, jittered interval, EWMA latency.
- **Passive health checks** — outlier ejection based on rolling error rate.
- **Hedged requests** — fire a duplicate after p99 latency, cancel the
  loser. Only for idempotent methods (`GET`, `HEAD`, `OPTIONS`,
  `PROPFIND`); never `POST` / `PUT` / `DELETE` / `PATCH`.
- **Request collapsing** — single-flight per cache key, latches subscribers
  while the leader fetches; protects origins during cache stampedes.
- **Circuit breaker** — half-open probes, exponential backoff.

### 6.1 Upstream TLS

Upstream TLS is fully configurable per pool. Sensible defaults match
NGINX `proxy_ssl_*` and Varnish `backend.ssl = on` behavior so the
migration story is short:

- `tls.enabled` (default off; on when target scheme is `https`).
- `tls.server_name` — SNI / verify hostname override (useful when the
  pool fronts a load balancer with a different cert).
- `tls.ca_bundle` — path to PEM bundle for verification. Falls back to
  the OS trust store.
- `tls.insecure_skip_verify` — accepted in config; **refused at startup
  in release builds** (build tag `release`). Available only in dev /
  test builds so it can't ship to production accidentally.
- `tls.client_cert` / `tls.client_key` — mTLS to origin.
- `tls.min_version` — default `1.2`; recommended `1.3`.
- `tls.alpn` — default `[h2, http/1.1]`.
- `tls.pinned_spki_sha256` — optional list of base64-encoded SHA-256
  fingerprints of the origin's SubjectPublicKeyInfo. Any match passes
  verification; empty list disables pinning.
- `tls.session_tickets` — default on for performance.

---

## 7. Listeners (L1) & Pipeline (L1)

- L1 owns sockets, TLS, and ALPN.
- Each protocol exposes a `chan *http.Request` adapter; from there the
  pipeline is protocol-agnostic.
- L1 pipeline stages (configurable, ordered):
  1. URL & host normalization (lowercasing, percent-decoding).
  2. ACL / IP allow-list / geo block.
  3. Cache key construction (with `Vary` injection from previous lookups).
  4. Request collapsing latch acquisition.
  5. Hand-off to L3.

---

## 8. Control Plane (L6)

`net/http.ServeMux` app on a dedicated admin port. Endpoints:

- `POST /v1/purge`             — exact URL purge.
- `POST /v1/ban`               — predicate ban (regex on host/path/header).
- `POST /v1/refresh`           — soft purge: mark stale, revalidate on next
                                request.
- `GET  /v1/config`            — current effective config.
- `POST /v1/config/reload`     — re-read config from disk.
- `GET  /v1/cluster/peers`     — gossip view.
- `GET  /v1/stats`             — JSON snapshot of counters.
- `GET  /healthz` / `/readyz`  — k8s probes.
- `GET  /metrics`              — Prometheus.
- `GET  /debug/pprof/*`        — pprof.
- `GET  /dashboard/*`          — embedded SPA (phase 6).

All write endpoints require a bearer token or mTLS.

### 8.1 Purge / Ban semantics
- **Purge** — synchronous local removal + async broadcast.
- **Ban** — predicate persisted in an in-memory "ban list"; objects matched
  lazily on lookup. Compacted when the oldest still-live object is younger
  than the predicate.
- **Refresh** — sets `Age = max-age - 1` so the next request triggers a
  conditional revalidation.

---

## 9. Configuration

YAML by default, with envvar interpolation. Hot-reloadable except where
noted. Validated by a JSON-schema generated from struct tags.

```yaml
listen:
  http:   ":80"
  https:  ":443"
  admin:  ":9000"            # admin (net/http)
  cluster: ":8443"           # peer mTLS

tls:
  # Data-plane TLS. Multiple certs supported for SNI; first match wins.
  certs:
    - cert_file: /etc/bouine/tls/api.crt
      key_file:  /etc/bouine/tls/api.key
      sni:       ["api.example.com", "*.api.example.com"]
    - cert_file: /etc/bouine/tls/static.crt
      key_file:  /etc/bouine/tls/static.key
      sni:       ["static.example.com"]
  alpn: [h2, http/1.1]
  min_version: "1.2"          # "1.3" recommended; "1.2" for legacy clients
  ocsp_stapling: auto          # uses staple if origin/CA supplies one
  reload:
    fsnotify: true             # reload on file change
    sighup:   true             # reload on SIGHUP
  # ACME / cert issuance is out of scope in v1.0 — use cert-manager or a
  # sidecar like step-ca and project the cert into /etc/bouine/tls.

storage:
  hot_max_bytes:   2Go
  warm_dir:        /var/lib/bouine     # encryption-at-rest delegated to the
                                       # cloud provider (EBS/PD/Azure Disk).
                                       # bouine never encrypts the warm tier.
  warm_max_bytes:  20Go
  eviction:        sieve

cluster:
  enabled:    true
  join:       ["bouine-headless.default.svc.cluster.local"]
  replicas:   2
  hop_limit:  2

upstream_pools:
  - name: app
    targets: [app.default.svc:8080]
    tls:
      enabled: false                # toggled automatically when scheme=https
      server_name: app.default.svc  # SNI / verify-name override
      ca_bundle: /etc/bouine/upstream-ca.pem
      client_cert: /etc/bouine/mtls/app.crt   # optional mTLS to origin
      client_key:  /etc/bouine/mtls/app.key
      min_version: "1.2"
      alpn: [h2, http/1.1]
      pinned_spki_sha256: []        # optional list of base64 fingerprints
    health:
      active:
        path: /healthz
        interval: 5s
        timeout: 1s
        unhealthy_threshold: 3
      passive:
        consecutive_5xx: 5

routes:
  - match: { host: "api.example.com" }
    pool: app
    cache:
      ttl_default: 60s
      stale_while_revalidate: 30s
      stale_if_error: 5m
      key:
        include_headers: [Accept-Language]
      prefetch:
        link_rel_preload: true
        sitemap: https://api.example.com/sitemap.xml
        max_concurrency: 4
```

---

## 10. Observability (L7)

- **Metrics** — Prometheus, RED + USE for: requests, hits, misses, stale
  hits, revalidations, evictions, upstream latency p50/p95/p99, queue depth,
  goroutine count, mmap segment usage.
- **Traces** — OpenTelemetry, one span per layer (`listener`, `pipeline`,
  `cache.lookup`, `origin.fetch`, `cluster.peerfetch`).
- **Logs** — `slog` JSON, sampled access log with cache result + key hash;
  structured error log unsampled.
- **Profiling** — `pprof` mounted on admin port behind auth.
- **Self-test** — `/debug/cachecheck?url=...` shows the decision tree the
  engine would take for a given request. Critical for ops.

---

## 11. CLI (Cobra)

```
bouine serve [--config /etc/bouine/config.yaml]
bouine purge <url>
bouine ban <predicate>            # e.g. host=example.com path~^/foo
bouine refresh <url>
bouine cluster peers
bouine cluster join <addr>
bouine stats
bouine config validate <file>
bouine config schema              # emit JSON schema
bouine bench [--profile canonical]
bouine cachecheck <url>           # offline decision dry-run
bouine version
```

All subcommands honor `--server`, `--token`, and `--insecure`.

---

## 12. Testing Strategy

### 12.1 Unit tests
- Per package, table-driven, race-detector on.
- Strict coverage gate per package (≥ 85 %, ≥ 95 % for `cache` and
  `storage`).

### 12.2 RFC 9111 conformance
- Vendored harness against [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests).
- Result published as a JSON badge in CI.
- Gate: must not regress vs. previous main; must reach Varnish parity by end
  of phase 3.

### 12.3 Integration
- `docker-compose` with: origin (echo + slow + flaky), 3× bouine nodes,
  load generator (`vegeta`, `k6`).
- Scenarios:
  - Cold start, cache fill, eviction, restart with warm tier.
  - Rolling restart of an upstream, hedged requests fire.
  - Network partition of one node, gossip reconciles.
  - Mass purge of 1M keys, cluster propagation.

### 12.4 Benchmarks (regression-gated)
- `bench/` runs nightly and on every PR via `benchstat` diff.
- Workloads:
  - **canonical** — 1 KB JSON, 99 % hits, 1 % misses, Zipf 0.99.
  - **large** — 1 MB, 80/20 hit ratio.
  - **purge-storm** — 100 RPS purges + 50 kRPS traffic.
  - **TLS** — both ECDSA and RSA, h2.
- Hard gates:
  - ≤ 2 % p99 regression on canonical RPS.
  - ≤ 5 % memory regression.
  - No allocation increase on the hit path (allocs/op == previous main).

### 12.5 Fuzzing
- `go test -fuzz` against header parsing, `Vary` canonicalisation,
  `Cache-Control` tokenizer.

### 12.6 Static analysis
- `staticcheck`, `govulncheck`, `golangci-lint`, `gosec`.

---

## 13. Performance Engineering Checklist

- xxhash64 for cache keys.
- Pre-sized maps; never grow on hot path.
- `sync.Pool` for header maps and 4 KiB IO buffers.

- Goroutine budget per request: 1 reader + 1 writer max, no per-stage
  spawn.
- TLS session tickets for reduced handshake latency.
- Body streaming — bodies never fully buffered in memory unless < 64 KiB.
- Background compaction on a separate goroutine pool with rate-limit.

---

## 14. Kubernetes Story

- Stateless w.r.t. external services; warm tier is an `emptyDir` or PVC.
- StatefulSet + headless Service for stable peer DNS.
- Liveness: `/healthz`. Readiness: `/readyz` (waits for cluster quorum).
- Helm chart and Kustomize overlays delivered in phase 4.
- HPA-friendly: scaling out triggers automatic ring rebalance with
  bounded movement (consistent-hash property).

### 14.1 Graceful shutdown sequence

Zero-5xx rolling deploys hinge on a strict, observable shutdown order.
On `SIGTERM` (or pod preStop hook), bouine executes the following in
order, each step bounded by a fraction of `terminationGracePeriodSeconds`
(default 30 s):

1. **t+0s — fail readiness.** `/readyz` returns `503`; the K8s Service
   stops sending new connections within one probe period (typically
   ≤ 5 s). A `bouine_shutdown_phase` gauge transitions to `draining`.
2. **t+0s — stop accepting new connections on the data plane.**
   - HTTP/1.1: close listener; existing keep-alives are signalled with
     `Connection: close` on the next response.
   - HTTP/2: send `GOAWAY` with `last_stream_id`; refuse new streams,
     finish in-flight.

3. **t+~1s — leave the cluster.** Gossip a `Leaving` membership update.
   Peers stop routing new requests to us within one gossip cycle.
4. **t+~1s — drain in-flight requests.** Bounded by the per-request
   deadline (default 30 s; capped at `terminationGracePeriodSeconds - 5s`).
5. **t+~Ns — flush hinted-handoff queue.** Best-effort: replicate any
   pending writes to peers; if budget exhausted, log and drop with a
   metric. Operators monitor `bouine_hh_drops_total`.
6. **t+~Ns — checkpoint warm tier.** Stop background compaction, fsync
   the WAL, close mmap segments cleanly so the next start is warm.
7. **t+~Ns — close admin & cluster listeners.** In this order so that a
   final purge can still be issued externally before shutdown.
8. **Process exits.** If any step exceeds its budget, bouine prefers a
   timely exit over a clean one and logs the unfinished step at `warn`.

A `preStop` lifecycle hook is shipped in the Helm chart that sleeps for
`readinessProbe.periodSeconds + 1s` before sending `SIGTERM`, to let LB
state propagate. The chart also sets `terminationGracePeriodSeconds` to
at least 30 s.

---

## 15. Roadmap — Pending Work

Phases 0–6 (including 3.5, 4.5) are **complete** and tagged. The items
below are the confirmed remaining work before v1.0 ships.

---

### Phase 7 — Simplification (remaining items)

All §7.1–§7.4 items are done. Two tasks remain:

#### 7.3 (remaining) — Reduce `cmd/bouine/cmd/engine.go` to ≤ 300 LOC

`engine.go` is currently 548 LOC. `builder.go` already exists
(`buildPools`, `buildRouter`, `buildStore`, `buildCluster` extracted) but
the lifecycle orchestration (`run`, `startListeners`, `startAdmin`,
`startHealthChecks`) has not been separated. Move these into a
`runner` type in `cmd/bouine/cmd/runner.go`. `engine.go` becomes a thin
wiring layer that reads from `builder` and delegates to `runner`.

#### 7.5 — Benchmark baseline refresh

Re-run `make bench` after the engine.go reduction and overwrite
`bench/results/baseline.txt` so `make benchstat` diffs are meaningful for
future work.

#### 7.6 Exit criteria

- `cmd/bouine/cmd/engine.go` ≤ 300 LOC.
- `bench/results/baseline.txt` updated; `make benchstat` compares against it
  without regression.
- `golangci-lint unused` and `staticcheck U1000` report zero findings.
- `make bench` gates pass; `make conformance` score does not decrease.

---

### Phase 6.5 — Dashboard UX gaps

Reference: `docs/assets/dashboard-reference.html`.

Items are classified: **Effort** S < 30 min · M 1–3 h · L > 3 h.
**Data** ✅ available · ⚠️ partial · ❌ needs new wiring.

#### Global / Layout

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-L1 | Sidebar footer: add live `peers N/N`, `hit X%`, `req/s N` rows — currently shows only node name | S | ✅ |
| G-L2 | Header pill: render "N peers healthy / stale peers" dynamically instead of static `● dashboard` | S | ✅ |
| G-L3 | Move time-range selector (1H / 6H / 24H) into the tabs bar on every page, not just Overview | S | ✅ |
| G-L4 | Light-mode active nav: add `box-shadow: inset 2px 0 0 var(--a)` (only dark-mode variant present) | S | ✅ |
| G-L5 | Sort icons: replace static `↕` text with `th.sortable` + `asc`/`desc` CSS classes toggled by `sortTable()` | M | ✅ |
| G-L6 | Add `.br` CSS class (red/danger button) for destructive actions | S | ✅ |
| G-L7 | Tier bar label row: add `.tier-bar-wrap` + `.tier-bar-label` flex row above `.tier-bar` | S | ✅ |
| G-L8 | Move theme-toggle button to `position:fixed; bottom:1rem; right:1rem` | S | ✅ |

#### Overview page

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-O1 | Metric cards: trend indicators already wired; verify `↑ X% vs prior` text renders correctly | S | ✅ |
| G-O2 | Route table in Overview: add Pool and TTL columns; join `config.Route` with `RouteStat` | M | ⚠️ |
| G-O3 | Hot & Warm store tile: add warm tier data + evictions/min (rate ring or 60s sample delta) | M | ⚠️ |
| G-O4 | Replace Quick Purge in tile 3 with compact circular SVG ring (120×120 `stroke-dasharray`); move Quick Purge to Invalidation page | M | ✅ |
| G-O5 | Peer rows: render `DataAddr`, `AdminAddr`, and `JoinedAt` — currently only name + live/stale | S | ✅ |
| G-O6 | Chart: change second dataset from "hits" to "errors" to match reference | S | ✅ |

#### Routes page

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-R1 | Subtitle: show configured route count `N configured routes · polled every 10s` | S | ⚠️ |
| G-R2 | Add config columns (Pool, TTL, SWR, SIE, neg_ttl, stayin_alive, jitter) to route table | M | ⚠️ |
| G-R3 | Add two bar charts: "hit ratio by route (6h)" and "req/min by route" | S | ✅ |
| G-R4 | Search bar placeholder: `Filter by prefix or pool…` | S | ✅ |

#### Cluster page

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-C1 | Subtitle: `gossip membership · consistent hash ring · peer fetch` | S | ✅ |
| G-C2 | Peer table: replace current columns with `Node \| Data addr \| Admin addr \| Weight \| Joined \| Status` | S | ✅ |
| G-C3 | Add ring stats box: virtual nodes/real, load factor, hop limit, peer fetch timeout, protocol version | S | ⚠️ |
| G-C4 | Replace horizontal band ring SVG with circular donut (220×220, `stroke-dasharray` arcs, node labels) | M | ✅ |
| G-C5 | Add peer fetch stats box: peer hits (6h), peer misses (6h), avg hop count, fan-out timeout | S | ⚠️ |

#### Invalidation page

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-I1 | Move Quick Purge form here from Overview tile 3 | S | ✅ |
| G-I2 | Ban form: add `header_regex` field alongside host/path | S | ✅ |
| G-I3 | Show recent invalidation history (last 10 operations, from an in-memory audit ring) | M | ⚠️ |

#### Config page

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-CF1 | Show current effective config as syntax-highlighted YAML (read-only) | M | ✅ |
| G-CF2 | Reload: show diff of changed keys after successful reload | M | ⚠️ |

---

### Phase 8 — AI traffic analysis (post-v1.0)

A streaming traffic analytics layer and ML-assisted suggestion engine,
running in a separate goroutine pool with a strict CPU budget so it
never competes with the data plane.

- Streaming analyzer: ingest sampled access logs into an embedded DuckDB
  (or Parquet on disk) for ad-hoc queries.
- Feature extraction: hit/miss patterns per route, TTL utilisation, top
  cacheable but uncached URLs, Vary blow-up detection, Cookie pollution.
- ML layer (initially heuristics, then a small ONNX-served model) suggests:
  - TTL changes per route.
  - Vary header pruning.
  - Candidate prefetch URLs.
  - Ban predicate strategies.
- "Suggestions" inbox in the dashboard with one-click apply (writes a
  config diff or pushes via the admin API).
- All AI features are **opt-in** and off by default.
- **Exit:** at least 5 actionable suggestion types implemented; no
  measurable impact on data-plane p99.

---

### Phase 9 — Production load testing & competitive benchmarking

Prove bouine is production-ready by measuring its behaviour under
realistic and extreme load and comparing head-to-head against NGINX,
Varnish, and Envoy on the same hardware and workloads.

All scripts live under `bench/loadtest/`.

#### Competitors

| TUT | Version | Config |
|-----|---------|--------|
| **bouine** | HEAD | YAML, SIEVE, 1 GiB hot tier |
| **NGINX** | 1.27.x | `proxy_cache_path`, same pool |
| **Varnish** | 7.6.x | VCL with matching TTL/SWR/SIE, malloc 1G |
| **Envoy** | 1.32.x | HTTP cache filter, same upstream cluster |

#### Core single-node scenarios

1. **Throughput ramp** — 90% hits / 10% misses, 1k→100k RPS in steps.
   Measure p50/p95/p99/p999, throughput, error rate, CPU, RSS.
2. **Cache-hit-only baseline** — 100% hits to 10k pre-warmed keys at
   50k RPS for 120s. Measure p50/p99, allocs/op, GC pauses.
3. **Cache-miss storm** — 100% unique or no-store URLs at 10k RPS.
   Measure origin connections, RSS growth, error rate.
4. **Working-set overflow** — 64 MiB cache vs 3.2 GiB working set.
   Measure hit ratio over time, eviction rate, p99 latency.
5. **Mixed realistic** — 60% hit / 15% miss / 10% stale / 5% revalidate /
   5% bypass / 3% vary / 2% error at 10k RPS for 300s.

#### Multi-node scenarios

1. **Horizontal scaling** — 1/2/3/5/10 nodes, fixed per-node load.
   Measure aggregate throughput, peer-fetch hit rate.
2. **Gossip convergence** — 5-node cluster, kill + restart node 3 under
   50k total RPS. Measure detection time, peer-fetch errors, rebalance duration.
3. **Rolling update** — 3-node StatefulSet under 10k RPS.
   Measure error rate and latency spike during `kubectl rollout restart`.

#### Stress scenarios

1. **Connection exhaustion** — 1k→50k concurrent connections, 1 req/conn.
2. **Large body streaming** — 10 MB bodies at 1k RPS; no buffering in RAM.
3. **Vary blow-up** — 1 URL × 1000 distinct `Vary: X-Test` values.

#### Exit criteria

- Results published in `bench/loadtest/results/` with charts and
  percentile tables.
- bouine canonical RPS ≥ Varnish (already passing in CI bench; this phase
  extends to the full competitive matrix).
- All 3 multi-node scenarios run end-to-end with no data loss.

---

## 16. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| ~~Fiber/fasthttp can't speak H2~~ | Resolved: Fiber dropped (ADR-0006). Admin runs on `net/http`, same as data plane. |
| mmap on macOS dev machines behaves differently from Linux prod | CI matrix runs storage tests on linux/amd64, linux/arm64, darwin/arm64. |
| RFC 9111 edge cases drift | cache-tests in CI, blocks merge on regression. |
| Cluster split-brain on purges | Monotonic purge tokens + anti-entropy reconciler. |
| Benchmark noise | Use `benchstat`, run on a pinned self-hosted runner, require N=10 samples. |
| AI features creep into the hot path | Hard architectural boundary: L8 only reads sampled telemetry, never writes to L2/L3. |
| VCL shim becomes a maintenance sink | Hard-cap supported subset (§17.4), fail loudly on unsupported constructs, never silently approximate. Deferred to post-v1.0 (§18). |
| SDK and HTTP API drift apart | Single source of truth in `pkg/api`; contract tests run both surfaces against the same fixtures. |
| Cache poisoning via unkeyed input | Default policy forbids implicit header keying; Vary cap; threat-model rows T06/T07/T09 wired to CI fuzz corpus. |
| HTTP request smuggling on the front door | Strict RFC 9112 parser, ambiguous-framing rejection, fuzz corpus seeded with public PortSwigger inputs. |
| TLS cert rotation race causing 5xx | fsnotify + SIGHUP reload tested in CI; reload is atomic (load + parse + swap). |
| Mixed-version cluster deadlocks | Wire-protocol versioning §5.5 with N/N-1 compatibility window. |
| Operator destructive purge with no audit trail | Admin write audit log with token-ID hash, IP, predicate, count, seq. |

---

## 17. Resolved Design Decisions

These were open questions during planning; locked in for v1.0.

1. **Go SDK is shipped** — `pkg/bouineapi` exposes a typed client for every
   admin endpoint (`Purge`, `Ban`, `Refresh`, `Stats`, `Peers`, `Config`,
   `Reload`). Same wire format as the HTTP API, share types via `pkg/api`.
   Auth via bearer token or mTLS, matching the server. Cobra subcommands
   are thin wrappers over the SDK so behaviour stays consistent. Versioned
   independently of the daemon and published with semver.
2. **No encryption-at-rest in bouine** — the warm tier writes plaintext
   segments to disk. Operators are expected to back `warm_dir` with an
   encrypted volume from the cloud provider (EBS/PD/Azure Disk LUKS/SSE).
   This keeps the storage engine simple and avoids key-management surface.
3. **HTTP/3 is not supported** — deferred to post-v1.0 pending demand
   (see §18).
4. **VCL-compatible shim is deferred** — designed to live in `/internal/vcl`. It is a
   parser + lowering pass that translates a useful subset of VCL 4.1 into
   the native bouine config tree at load time. Supported surface area:
   - `sub vcl_recv`, `vcl_hash`, `vcl_backend_response`, `vcl_deliver`,
     `vcl_hit`, `vcl_miss`, `vcl_pass`, `vcl_purge`.
   - `set req.*`, `set bereq.*`, `set beresp.*`, `set resp.*` for headers,
     TTL, grace, keep, and `obj.hits`.
   - `return(...)` actions: `hash`, `pass`, `lookup`, `deliver`, `restart`,
     `synth`, `purge`, `fetch`.
   - `backend` and `director` declarations, mapped to bouine upstream
     pools and health-check config.
   - `acl`, mapped to bouine ACL stage in L1.
   Out of scope: inline C, `vmod_*` modules, custom storage backends.
   The shim emits a diff report at load time showing every VCL construct
   that was dropped or approximated, so operators can audit migrations.
   `bouine config validate --vcl file.vcl` and
   `bouine config translate --vcl file.vcl` are first-class CLI commands.
5. **Compression policy: passthrough by default** — store responses in
   the encoding the origin produced; bucket `Accept-Encoding` into
   `br|zstd|gzip|identity`. Per-route `normalize_identity` mode trades
   CPU for a smaller variant set. See §3.3.
6. **Cookie policy: ignore by default, opt-in per route** — `Cookie`
   headers do not participate in the cache key by default; responses
   with `Set-Cookie` are not stored unless explicitly opted in per
   route. `Authorization` follows RFC 9111 §3.5 strictly. See §3.4.
7. **TLS cert lifecycle: file-backed, hot reload** — bouine reloads
   certs on fsnotify + SIGHUP; multiple certs supported via SNI rules;
   OCSP staples are forwarded when present. Built-in ACME / autocert is
   out of scope in v1.0; cert-manager (or equivalent) projects the cert
   into the pod. See §9 config and §18 for the deferred ACME work.
8. **Upstream TLS is a first-class config** — mTLS to origin, custom CA
   bundle, optional SPKI pinning, `insecure_skip_verify` available only
   in dev builds. See §6.1.
9. **Cluster wire protocol is versioned** — magic bytes + uint16
   version on every frame; N/N-1 compatibility window so a single
   release supports the rolling-upgrade transition. See §5.5.
10. **Graceful shutdown is a fixed sequence** — fail readiness, stop
    accepting new connections (with H2 `GOAWAY`), leave the cluster,
    drain, flush hinted handoff, checkpoint warm tier, close admin and
    cluster listeners last. See §14.1.

---

## 18. Out of Scope / Future Roadmap (post-v1.0)

Features deliberately deferred so v1.0 stays focused on "a faster, more
observable, K8s-native Varnish/NGINX replacement". Each item below is a
candidate for a future minor release; none of them block adoption for
the canonical NGINX or not-too-VCL-heavy-Varnish use cases.

1. **Data-plane authentication & authorization** — JWT validation,
   basic-auth check, OAuth introspection. v1.0 forwards `Authorization`
   and lets the origin enforce; this is sufficient for typical reverse
   caches and matches NGINX's default posture. v1.1 candidate.
2. **Per-route request-rate limiting** — v1.0 ships connection caps,
   slow-body backpressure, and admin-API rate limits, but not per-route
   request-rate limiting in the data plane. Today operators put a token
   bucket (cloud LB, sidecar, edge) in front of bouine. v1.1 candidate.
3. **Built-in ACME / autocert** — v1.0 uses file-backed certs reloaded
   on fsnotify/SIGHUP. cert-manager covers the K8s story; an
   integrated ACME client may ship as an opt-in module in v1.1.
4. **Multi-tenant routing scopes** — strong per-tenant isolation beyond
   virtual host (per-tenant storage quota, per-tenant admin tokens,
   per-tenant metrics namespacing). Operators with strong tenancy needs
   run one bouine deployment per tenant today.
5. **Forward-proxy mode** — v1.0 is a reverse cache only.
6. **gRPC caching** — passthrough only in v1.0.
7. **WebSocket caching** — never (passthrough only).
8. **HTTP/3** — both client-facing and origin-facing HTTP/3 are
   deferred until demand materializes. The `quic-go` dependency was
   removed to reduce binary size and complexity.
9. **Pluggable storage backends** — v1.0 is RAM + mmap, embedded only.
   A plugin interface (e.g., NVMe-direct, Optane, object storage) may
   land later.
10. **Backup / restore of ban list** — caches are rebuildable; admin
    state is recoverable from the config + the audit log. A dedicated
    snapshot/restore tool is a v1.1 candidate.
11. **Emergency stale-mode** — serve stale beyond `stale-if-error` for
    explicitly allow-listed routes during an origin outage. v1.1
    candidate; today operators bump `stale-if-error` config and reload.
12. **VCL inline-C and `vmod_*`** — never. The shim covers config-shape
    constructs only.
13. **Dashboard multi-tenancy** — phase-6 dashboard is single-tenant
    operator-facing in v1.0/v1.1.
14. **Web-Cache-Deception heuristic generalization** — current default
    (block on extension mismatch + private semantics) covers the common
    cases; broader path-confusion mitigations may land later.
15. **Federated multi-cluster** — gossip and consistent hash today are
    single-cluster. Cross-cluster federation (e.g., regional tiering)
    is a future feature.
16. **ESI-lite** — `<esi:include>` support was originally scoped for
    phase 5 but dropped to keep v1.0 focused. Most modern architectures
    prefer edge-side composition at the CDN layer or client-side
    includes. v1.1+ candidate if demand materializes.
17. **Surrogate keys** — `Surrogate-Key` response header indexed for
    ban was originally scoped for phase 5 but dropped. Operators use
    host/path/method predicate bans in v1.0. v1.1 candidate.
18. **VCL-compatible shim** — parser + lowering pass translating a
    subset of VCL 4.1 into the native bouine config tree. Originally
    planned as phase 5.5 but deferred. The design is documented in
    §17.4; `/internal/vcl` is reserved. v1.1+ candidate when Varnish
    migration demand justifies the maintenance cost.
19. **Multi-layer cache (cache-of-caches)** — deploy multiple bouine
    tiers (e.g. edge → regional → origin-shield) where each layer is
    an independent bouine instance or cluster. Requires: hierarchical
    cache invalidation propagation (purge/ban forwarded from outer to
    inner layers), parent-fetch protocol (inner layer acts as origin
    for the outer), loop detection across tiers (`X-Bouine-Tier` +
    hop budget), and per-layer TTL/SWR overrides so the edge can be
    aggressive while the shield stays conservative. Also useful as a
    circuit-breaker topology: the inner layer absorbs origin failures
    while the outer continues serving stale. v1.2+ candidate.
20. **Always-warm (pre-warm on eviction)** — when an object is evicted
    from the hot tier (SIEVE), instead of discarding it,
    schedule an immediate background re-fetch from origin to keep the
    entry warm. Controlled per route via `cache.always_warm: true`.
    Guards: bounded re-fetch concurrency (shared with the prefetcher
    semaphore), skip if origin returned 5xx on last attempt, honour
    `Cache-Control: no-cache` / `no-store` from origin on re-fetch.
    Useful for high-traffic routes where a cache miss is unacceptable
    (e.g. homepage, product listing). v1.1+ candidate.

When one of these graduates from "deferred" to "planned", it gets a
phase entry in §15, a row in the threat model if security-relevant, and
its own ADR under `docs/decisions/`.

---

## 19. Definition of Done (v1.0)

- All phases 0–7 (plus 3.5, 4.5, 6, and 8) complete and tagged.
- Hit-path allocs/op = 0 on the canonical benchmark (phase 3.5 gate).
- cache-tests score ≥ Varnish on every category.
- Canonical benchmark within 10 % of Varnish RPS, never regressing in CI.
- `golangci-lint unused` and `staticcheck U1000` zero findings (phase 7
  gate).
- No orphaned / unwired production packages.
- Threat model (`docs/security/threat-model.md`) has zero `Txx` in
  `unaddressed` state without an explicit §18 deferral.
- Helm chart published.
- Documentation site live with quickstart, reference config, ops
  runbook, threat model, and migration guides for Varnish and NGINX.
- Phase 6 may ship as v1.1 to keep v1.0 focused on the cache itself.
