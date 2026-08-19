# bouine — Architecture Reference

`bouine` is a horizontally-scalable, observability-first HTTP reverse-proxy
cache written in Go 1.26. This document is the design reference: goals,
layer model, implementation decisions, and operational characteristics.

---

## 1. Goals & Non-Goals

### 1.1 Goals

- **Protocol coverage** — terminate HTTP/1.1 and HTTP/2 with TLS, ALPN, and
  HTTP upgrade.
- **RFC 9111 compliance** — score at least on par with Varnish on
  [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests).
- **Embedded storage** — no external KV (Redis, Memcached, etcd…). Hot tier
  in RAM, warm tier on local disk, optional cluster fan-out.
- **Multi-instance clustering** — consistent-hash sharding, peer fetch,
  gossip-based membership; eventual consistency is acceptable.
- **High performance** — zero-allocation hot path on hits, regression-gated
  by a benchmark CI job (≥ Varnish single-node RPS for the canonical workload).
- **Fault tolerance** — active and passive upstream health checks, hedged
  requests, circuit breakers, request collapsing.
- **Operator UX** — Cobra-based CLI, declarative YAML config,
  purge & ban APIs (URL, regex, full).
- **Observability built-in** — OpenTelemetry traces, Prometheus metrics,
  slog structured access logs, pprof.
- **Advanced caching** — prefetching, stale-while-revalidate, stale-if-error,
  request collapsing, Vary canonicalisation.
- **AI-assisted insights (phase 8)** — traffic analysis daemon plus a
  dashboard suggesting cache-strategy improvements.

### 1.2 Non-Goals

- WAF / DDoS scrubbing (delegated to a sidecar or Layer-7 LB).
- Generic L3 load-balancing (we are an L6 HTTP cache).
- WebSockets as cached objects (passed through but never cached).
- Becoming a drop-in VCL interpreter — we expose a richer config DSL.
- End-user authentication / authorization on the data plane — bouine forwards
  `Authorization` / `Cookie` headers and lets the origin enforce.
- Per-route request-rate limiting on the data plane — only connection and
  slow-body backpressure ship in v1.0.
- Encryption at rest of the warm tier — delegated to the cloud volume
  (EBS/PD/Azure Disk).
- Built-in ACME / cert issuance — bouine reloads certs from disk; ACME is
  delegated to cert-manager or a sidecar.
- Multi-tenant isolation beyond virtual host — operators with strong tenancy
  needs run one bouine deployment per tenant.

---

## 2. High-Level Architecture

Layered design, each layer testable in isolation. Lower numbers are closer
to the wire.

```
┌──────────────────────────────────────────────────────────────────────┐
│ L8  AI Insights & Dashboard         (phase 8)                        │
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

### 2.1 HTTP stack

The daemon runs on a **single** HTTP implementation (ADR-0006): `net/http`
for both the data plane (proxy/cache) and the control plane (admin API,
metrics, pprof). Plaintext HTTP/2 (h2c) is available via
`golang.org/x/net/http2/h2c` for in-mesh traffic. The control plane uses
`net/http.ServeMux` (Go 1.22+ pattern routing) on its own port.

Fiber was used initially but dropped in phase 1 (ADR-0006) because it added a
third HTTP stack (`valyala/fasthttp`) and 7 transitive dependencies for a
surface that serves ≤ 10 RPS.

### 2.2 Module layout

```
/cmd/bouine                  Cobra entrypoint
/internal/server             L1 — HTTP/1, /2, TLS, route matching
/internal/cache              L3 — RFC 9111 state machine, Vary, conditionals
/internal/storage            L2 — RAM tier, mmap tier, eviction, WAL
/internal/storage/evictor   Shared eviction Entry/List abstraction
/internal/storage/sieve     SIEVE eviction policy
/internal/storage/cachaner  cachaner eviction policy (SIEVE + freq counter)
/internal/origin             L4 — upstream pool, health, hedge, breaker
/internal/staticfile        L4 — local file serving (alternative to upstream pool)
/internal/cluster            L5 — memberlist gossip, consistent hash, peer fetch
/internal/admin              L6 — net/http admin: purge, ban, refresh, config
/internal/dashboard          L6 — embedded operator dashboard (templ + htmx)
/internal/observability      L7 — OTEL, Prom, slog, pprof
/internal/cloudflare         Cloudflare Cache API invalidation propagation
/internal/config             config loader, schema, validation
/internal/runtime            supervised goroutines, graceful shutdown
/web/dashboard               embedded dashboard assets (embed.FS)
/pkg/bouineapi               public Go SDK (purge/ban/refresh/stats client)
/pkg/api                     shared types between SDK, admin server, dashboard
/test/integration            in-process 3-node cluster scenarios + chaos
/test/cachetests             http-tests/cache-tests harness
/bench                       benchmark suite + nightly comparison
```

> L8 (AI insights, `/internal/ai`) is a **design target, not yet
> implemented** — see the backlog below. The same applies to
> prefetching and the VCL shim listed under Goals above.

### 2.3 Cross-cutting principles

- **Zero-alloc hot path** — pooled buffers, `sync.Pool` for headers, no
  `fmt.Sprintf` on lookups.
- **Context everywhere** — every request carries an OTEL span context.
- **Interface boundaries** — `cache.Store`, `origin.Fetcher`, `cluster.Peer`
  are interfaces so each layer can be unit-tested with fakes.
- **No global state** — the daemon is a single `Engine` struct instantiated by
  the Cobra command.

### 2.4 Security model

The full threat model lives in
[`docs/security/threat-model.md`](security/threat-model.md).
Headline trust-boundary guarantees:

- **TB1 — Internet ↔ data plane**: TLS terminates here. Strict RFC 9112 parser
  blocks request smuggling (T05). Header / URL / body caps always enforced
  (T37). HTTP/2 reset-flood mitigations on by default (T10).
- **TB3 — bouine ↔ origin**: TLS verified by default; mTLS, custom CA bundles,
  optional SPKI pinning, server-name override — all configured per pool.
- **TB4 — bouine ↔ peer bouine**: cluster mTLS is mandatory, on its own CA,
  with versioned wire protocol.
- **TB5 — operator ↔ admin API**: bearer token (constant-time compare) or
  mTLS; admin port never bound externally in default manifests. Every write
  action is audit-logged.
- **TB6 — process ↔ disk**: warm-tier segments are `0600`; encryption-at-rest
  delegated to the cloud volume.

---

## 3. Cache Engine (L3) — RFC 9111

The engine is a deterministic state machine. Its only inputs are an
`http.Request`, the matching `*StoredObject` (if any), and "now"; its outputs
are an action (`HIT`, `MISS`, `REVALIDATE`, `STALE_HIT`, `BYPASS`) and a
`Disposition`.

### 3.1 RFC surface

- Request methods: `GET`, `HEAD`, `POST` (only when explicit), `PURGE`.
- Cacheability rules: `Cache-Control` (request & response), `Pragma`,
  `Authorization`, `Set-Cookie`, `Expires`, heuristic freshness.
- Response directives: `no-store`, `no-cache`, `private`, `public`,
  `max-age`, `s-maxage`, `must-revalidate`, `proxy-revalidate`, `immutable`,
  `stale-while-revalidate`, `stale-if-error`.
- Request directives: `no-cache`, `no-store`, `max-age`, `min-fresh`,
  `max-stale`, `only-if-cached`.
- `Vary` — canonical normalization, secondary cache key, `Vary: *` opt-out.
- Conditional revalidation: `ETag` (strong & weak), `Last-Modified`,
  `If-None-Match`, `If-Modified-Since`, `If-Match`, `If-Unmodified-Since`,
  `If-Range`.
- Range responses (206), partial cache (cache full body, serve range from
  stored bytes when possible).
- `Age` calculation including `Date` skew and forward proxies.
- Trailer headers, `Transfer-Encoding: chunked` for HTTP/1.1.
- Method invalidation (`POST`/`PUT`/`DELETE` evict matching URLs).

### 3.2 Cache key construction

The canonical cache key is deterministic and stable across nodes. The primary
key is built from (in order): scheme → host (lowercased, IDN → punycode,
default port stripped) → path (percent-decoded, re-encoded canonically) →
query (parameters sorted lexicographically) → method (GET and HEAD share
key space).

The secondary key (Vary) is derived from headers listed in the response's
`Vary`. Headers participate in the cache key **only** via `Vary` or an
explicit per-route allow-list — never implicitly. This is the primary
defense against cache-poisoning via unkeyed input (threat T06, T07).

### 3.3 Compression policy

Store responses in the encoding the origin produced. Bucket `Accept-Encoding`
into `br | zstd | gzip | identity` to bound variant count.

- `passthrough` (default) — store as-is, one variant per encoding-bucket.
- `normalize_identity` — request `identity` from origin, recompress on egress.
  Forbidden on routes serving secrets (mitigates BREACH-class oracles, T25).

### 3.4 Cookie & authorization policy

- **Request `Cookie`** — does NOT participate in the cache key by default.
  Per-route opt-in: `cache.cookies.key: [name1, name2]`.
- **Response `Set-Cookie`** — a response carrying `Set-Cookie` is NOT stored
  by default. Per-route opt-in requires explicit operator acknowledgement.
- **`Authorization` request header** — per RFC 9111 §3.5, responses to
  authorized requests are NOT stored unless the response carries
  `must-revalidate`, `public`, or `s-maxage`. No operator override.

---

## 4. Storage (L2) — Embedded, Multi-Tier

### 4.1 Tiers

- **L0 / Hot** — in-memory `map[uint64]*Entry` keyed by 64-bit xxhash;
  sharded `N=runtime.NumCPU()` ways to avoid contention.
- **L1 / Warm** — append-only segmented mmap files (64 MiB segments), garbage
  collected by tombstone compaction.
- **Spillover** — small objects pinned in L0, large/cold objects demoted to L1.

### 4.2 Eviction

The hot tier supports pluggable eviction policies via the
`internal/storage/evictor` package. The policy is selected by
`storage.hot_eviction_algorithm` in config:

- **`sieve`** (default): SIEVE, a near-LRU-K algorithm with O(1)
  amortized per op and a 1-bit visited field. A hand pointer sweeps
  the list; visited entries get a second chance (visited cleared),
  unvisited entries are evicted.
- **`cachaner`**: SIEVE with a 3-bit saturating frequency counter
  packed into the `evictor.Entry`'s `ioBits` field. Hot objects get up
  to 7 second chances (vs SIEVE's 1) before eviction, reducing origin
  bandwidth and RSS at a small p50 latency cost. See ADR-0031.

Both tiers support both policies. `storage.eviction_algorithm` sets
the default for both tiers; `storage.hot_eviction_algorithm` and
`storage.warm_eviction_algorithm` override it per-tier. When non-empty,
the per-tier field takes precedence.

Both policies share the same `evictor.Entry` struct (40B on 64-bit).
The `ioBits` field fills the 4B padding slot after `atomic.Bool`, so
SIEVE users see zero memory overhead. The hit-path fast path reads
`Entry.Visited()` directly — no interface dispatch, zero allocations.

Ban check uses a lock-free atomic counter fast path — the global mutex
is only taken when the ban list is non-empty. Eviction runs off the
request critical path via a background sweeper with a bounded inline
cap per `Put`. The sweep is capped at `maxSweepProbes = 128` (see
ADR-0026) regardless of policy.

### 4.3 Durability

WAL for the index only (not bodies). Warm restart in seconds; mmap segments
validated on open. Optional `--no-persist` for ephemeral pods.

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

`hashicorp/memberlist` for gossip; nodes publish a hash ring digest. K8s:
headless `Service` + `StatefulSet`, bootstrap via DNS SRV lookups.

### 5.2 Sharding

Consistent hash with bounded loads (Google's "Consistent Hashing with Bounded
Loads"). 256 virtual nodes per real node; weight is configurable.

### 5.3 Peer fetch protocol

Internal HTTP/2 over mTLS (port `:8443` by default). On miss: owner first,
two-hop fallback, then origin. Cuckoo-filter-based digests gossiped every 5s
to short-circuit fetches for keys known absent from peers. `Bouine-Hop` header
bounds traversal depth (default 2).

### 5.4 Consistency

Writes are local-first; eventual replication is fire-and-forget to N-1
replicas with hinted handoff in a bounded queue. Purges are broadcast via
gossip. A
purge is monotonic — once a TTL marker is set, late writes for that key are
rejected until TTL expires.

### 5.5 Wire protocol versioning

Every gossip and peer-fetch frame carries a version header for mixed-version
rolling-deploy compatibility.

- **Framing**: magic `"BOUI"` (4 bytes) + `version` (`uint16`, big-endian) +
  `kind` (`uint16`).
- **Negotiation**: `X-Bouine-Cluster-Version` request header;
  `X-Bouine-Cluster-Accepts: 3-5` response header. Mismatch falls back to
  origin path — no panic.
- **Compatibility window**: every release supports N and N-1. N is dropped in
  the release after the transition (two-step bump).
- **Breaking changes** documented in `CHANGELOG.md`; a
  `bouine_cluster_protocol_mismatch_total` metric fires on mismatch.

---

## 6. Upstream / Origin (L4)

- Connection pool per upstream, keyed by host:port + TLS profile.
- **Active health checks** — HTTP probe, expected status codes, EWMA latency,
  jittered interval.
- **Passive health checks** — outlier ejection based on rolling error rate.
- **Hedged requests** — fire a duplicate after p99 latency for idempotent
  methods only (`GET`, `HEAD`, `OPTIONS`, `PROPFIND`).
- **Request collapsing** — single-flight per cache key, latches subscribers
  while the leader fetches.
- **Circuit breaker** — half-open probes, exponential backoff.

### 6.1 Upstream TLS

Configurable per pool: `tls.enabled`, `tls.server_name` (SNI override),
`tls.ca_bundle`, `tls.client_cert` / `tls.client_key` (mTLS to origin),
`tls.min_version` (default `1.2`), `tls.alpn` (default `[h2, http/1.1]`),
`tls.pinned_spki_sha256`. `tls.insecure_skip_verify` is accepted in config
but refused at startup in release builds.

---

## 7. Listeners (L1) & Pipeline

L1 owns sockets, TLS, and ALPN. L1 pipeline stages (configurable, ordered):

1. URL & host normalization (lowercasing, percent-decoding).
2. ACL / IP allow-list / geo block.
3. Cache key construction (with `Vary` injection from previous lookups).
4. Request collapsing latch acquisition.
5. Hand-off to L3.

---

## 8. Control Plane (L6)

`net/http.ServeMux` on a dedicated admin port.

| Endpoint | Description |
|----------|-------------|
| `POST /v1/purge` | Exact URL purge |
| `POST /v1/ban` | Predicate ban (regex on host/path/header) |
| `POST /v1/refresh` | Soft purge: mark stale, revalidate on next request |
| `GET /v1/config` | Current effective config |
| `GET /v1/cluster/peers` | Gossip view |
| `GET /v1/stats` | JSON snapshot of counters |
| `GET /healthz` `/readyz` | K8s probes |
| `GET /metrics` | Prometheus |
| `GET /debug/pprof/*` | pprof (opt-in via `admin.pprof_enabled`, auth-exempt) |
| `GET /dashboard/*` | Embedded operator dashboard |

All write endpoints require a bearer token or mTLS.

**Purge / Ban / Refresh semantics:**
- **Purge** — synchronous local removal + async broadcast.
- **Ban** — predicate persisted in-memory; objects matched lazily on lookup.
  Compacted when the oldest still-live object is younger than the predicate.
- **Refresh** — sets `Age = max-age - 1` so the next request triggers a
  conditional revalidation.

---

## 9. Configuration

YAML by default, with envvar interpolation. Applied by rolling the pod
(no live reload).
Validated by a JSON-schema generated from struct tags (`bouine config schema`).

```yaml
listen:
  http:    ":80"
  https:   ":443"
  admin:   ":9000"
  cluster: ":8443"

tls:
  certs:
    - cert_file: /etc/bouine/tls/api.crt
      key_file:  /etc/bouine/tls/api.key
      sni:       ["api.example.com", "*.api.example.com"]
  alpn: [h2, http/1.1]
  min_version: "1.2"
  ocsp_stapling: auto

storage:
  hot_max_bytes:  2Go
  warm_dir:       /var/lib/bouine
  warm_max_bytes: 20Go
  eviction:       sieve

cluster:
  enabled:   true
  join:      ["bouine-headless.default.svc.cluster.local"]
  replicas:  2
  hop_limit: 2

upstream_pools:
  - name: app
    targets: [app.default.svc:8080]
    tls:
      enabled:    false
      ca_bundle:  /etc/bouine/upstream-ca.pem
      min_version: "1.2"
    health:
      active:
        path:                /healthz
        interval:            5s
        timeout:             1s
        unhealthy_threshold: 3
      passive:
        consecutive_5xx: 5

routes:
  - match: { host: "api.example.com" }
    name:  api
    pool:  app
    cache:
      ttl_default:           60s
      stale_while_revalidate: 30s
      stale_if_error:        5m
      key:
        include_headers: [Accept-Language]
      prefetch:
        link_rel_preload: true
        sitemap:          https://api.example.com/sitemap.xml
        max_concurrency:  4
```

---

## 10. Observability (L7)

- **Metrics** — Prometheus, RED + USE: requests, hits, misses, stale hits,
  revalidations, evictions, upstream latency p50/p95/p99, queue depth,
  goroutine count, mmap segment usage.
- **Traces** — OpenTelemetry, one span per layer (`listener`, `pipeline`,
  `cache.lookup`, `origin.fetch`, `cluster.peerfetch`).
- **Logs** — `slog` JSON, sampled access log with cache result + key hash;
  structured error log unsampled.
- **Profiling** — `pprof` mounted on admin port, auth-exempt, opt-in via
  `admin.pprof_enabled` config flag (default false). The admin port is network-isolated
  via K8s NetworkPolicy in production.
- **Self-test** — `/debug/cachecheck?url=...` shows the decision tree the
  engine would take for a given request.

---

## 11. CLI

```
bouine serve [--config /etc/bouine/config.yaml]
bouine purge <url>
bouine ban <predicate>            # e.g. host=example.com path~^/foo
bouine refresh <url>
bouine cluster peers
bouine cluster join <addr>
bouine stats
bouine config validate <file>
bouine config schema
bouine bench [--profile canonical]
bouine cachecheck <url>
bouine version
```

All subcommands honor `--server`, `--token`, and `--insecure`.

---

## 12. Testing Strategy

| Layer | Approach |
|-------|----------|
| Unit | Per package, table-driven, `-race` always on. Coverage gate ≥ 85%; ≥ 95% for `cache` and `storage`. |
| Conformance | [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests) harness in CI; score published as JSON badge; regressions block merge. Current: **93.2% (340/365)**. |
| Integration | In-process 3-node cluster + chaos scenarios (peer kill, origin flap, slow origin, rolling restart, concurrent purge, rejoin). |
| Benchmarks | `bench/` nightly + on every PR via `benchstat`. Gates: ≤ 2% p99 regression, ≤ 5% memory regression, zero hit-path allocation increase. |
| Fuzz | `go test -fuzz` against header parsing, `Vary` canonicalisation, `Cache-Control` tokenizer. |
| Static analysis | `staticcheck`, `govulncheck`, `golangci-lint`, `gosec`. |

---

## 13. Performance Engineering

- xxhash64 for cache keys.
- Pre-sized maps; never grow on hot path.
- `sync.Pool` for header maps and 4 KiB IO buffers.
- Goroutine budget per request: 1 reader + 1 writer max, no per-stage spawn.
- TLS session tickets for reduced handshake latency.
- Body streaming — bodies never fully buffered in memory unless < 64 KiB.
- Background compaction on a separate goroutine pool with rate-limit.
- Hit-path budget: < 5 µs CPU per request at p50, allocs/op = 0, bytes/op = 0
  after warm-up.

---

## 14. Kubernetes

- Warm tier on `emptyDir` or PVC; otherwise stateless w.r.t. external services.
- StatefulSet + headless Service for stable peer DNS.
- Liveness: `/healthz`. Readiness: `/readyz` (store loaded + listeners bound).
- HPA-friendly: scaling out triggers automatic ring rebalance.
- Helm chart: PodDisruptionBudget, anti-affinity, topology spread,
  NetworkPolicy template, ServiceMonitor optional.

### 14.1 Graceful shutdown sequence

On `SIGTERM` (or pod `preStop` hook), in order — each step bounded by a
fraction of `terminationGracePeriodSeconds` (default 30s):

1. **t+0s** — `/readyz` returns 503; service stops sending new connections.
2. **t+0s** — stop accepting new data-plane connections. HTTP/1.1: close
   listener + `Connection: close`. HTTP/2: send `GOAWAY`, refuse new streams.
3. **t+~1s** — gossip `Leaving` membership update; peers stop routing to us.
4. **t+~1s** — drain in-flight requests (bounded by per-request deadline).
5. **t+~Ns** — flush hinted-handoff queue (best-effort).
6. **t+~Ns** — checkpoint warm tier (fsync WAL, close mmap segments cleanly).
7. **t+~Ns** — close admin & cluster listeners.
8. **Process exits.** Timely exit preferred over clean exit on budget overrun.

A `preStop` httpGet hook in the Helm chart calls the `/drain` endpoint,
which marks the pod not-ready and blocks for `admin.drain_duration`
(default 10 s) before `SIGTERM` to let kube-proxy propagate endpoint removal.
The distroless container image has no shell, so an exec-based sleep cannot
be used.

---

## 15. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| mmap on macOS behaves differently from Linux prod | CI matrix: linux/amd64, linux/arm64, darwin/arm64. |
| RFC 9111 edge cases drift | `cache-tests` in CI; blocks merge on regression. |
| Cluster split-brain on purges | Monotonic purge tokens. |
| Benchmark noise | `benchstat`, pinned self-hosted runner, N ≥ 10 samples. |
| AI features creep into the hot path | Hard boundary: L8 only reads sampled telemetry, never writes to L2/L3. |
| VCL shim becomes a maintenance sink | Hard-cap supported subset; fail loudly on unsupported constructs; deferred to post-v1.0. |
| SDK and HTTP API drift apart | Single source of truth in `pkg/api`; contract tests run both surfaces against the same fixtures. |
| Cache poisoning via unkeyed input | Default policy forbids implicit header keying; Vary cap; T06/T07/T09 wired to CI fuzz corpus. |
| HTTP request smuggling | Strict RFC 9112 parser, ambiguous-framing rejection, fuzz corpus seeded with PortSwigger inputs. |
| TLS cert rotation race causing 5xx | TLS certs are file-backed and rotated by updating the mounted Secret/ConfigMap and rolling the pod; no in-process reload. |
| Mixed-version cluster deadlocks | Wire-protocol versioning with N/N-1 compatibility window. |
| Operator destructive purge with no audit trail | Admin write audit log with token-ID hash, IP, predicate, count, seq. |

---

## 16. Key Design Decisions

These decisions are locked in for v1.0. See also
[`docs/decisions/`](decisions/) for individual ADRs.

1. **Go SDK shipped** — `pkg/bouineapi` exposes a typed client for every admin
   endpoint. Cobra subcommands are thin wrappers over the SDK.
2. **No encryption-at-rest in bouine** — warm tier writes plaintext segments.
   Operators back `warm_dir` with an encrypted cloud volume.
3. **HTTP/3 deferred** — post-v1.0 pending demand.
4. **VCL-compatible shim deferred** — `/internal/vcl` reserved; design
   documented in [`docs/migration/varnish.md`](migration/varnish.md).
   Supported surface: `sub vcl_recv/hash/backend_response/deliver/hit/miss/
   pass/purge`, `set req.*`/`bereq.*`/`beresp.*`/`resp.*`, `return(...)`,
   `backend`/`director` → upstream pools, `acl` → L1 ACL stage.
5. **Compression: passthrough by default** — store as origin produced; bucket
   `Accept-Encoding` into `br|zstd|gzip|identity`.
6. **Cookie policy: ignore by default, opt-in per route** — `Cookie` not in
   cache key; `Set-Cookie` responses not stored unless explicitly opted in.
   `Authorization` follows RFC 9111 §3.5 strictly.
7. **TLS cert lifecycle: file-backed** — certs read from mounted files
   at startup; multiple certs via SNI rules; OCSP staples forwarded when
   present. Rotation by updating the mounted Secret/ConfigMap and rolling
   the pod.
8. **Upstream TLS is a first-class config** — mTLS to origin, custom CA,
   optional SPKI pinning, `insecure_skip_verify` only in dev builds.
9. **Cluster wire protocol is versioned** — magic bytes + `uint16` version;
   N/N-1 compatibility window for rolling upgrades.
10. **Graceful shutdown is a fixed sequence** — fail readiness → stop
    accepting → leave cluster → drain → flush HH → checkpoint warm tier →
    close admin/cluster listeners.
