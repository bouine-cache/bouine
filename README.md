<p align="center">
  <img src="docs/assets/bouine_anglerfish.png" alt="bouine" width="200">
</p>

<h1 align="center">bouine</h1>

<p align="center">
  <a href="https://github.com/thylong/bouine/actions/workflows/ci.yml"><img src="https://github.com/thylong/bouine/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://github.com/thylong/bouine/actions/workflows/release.yml"><img src="https://github.com/thylong/bouine/actions/workflows/release.yml/badge.svg" alt="Release"></a>
  <a href="https://github.com/thylong/bouine/releases/latest"><img src="https://img.shields.io/github/v/release/thylong/bouine" alt="Latest Release"></a>
  <a href="https://hub.docker.com/r/thylong/bouine"><img src="https://img.shields.io/docker/v/thylong/bouine?label=docker" alt="Docker"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/thylong/bouine" alt="License"></a>
</p>

`bouine` is a scalable, cloud native, HTTP reverse-proxy
cache written in Go. It targets the same problem space as Varnish
(RFC 9111 cache, fast purge, predicate-based bans) but is designed from
day one for Kubernetes, multi-instance clustering, and first-class
observability.

> Status: **v1.0-rc** — phases 0–7 complete. Caching, clustering,
> prefetching, negative caching, jittered TTLs, soft-purge, and the Go
> SDK are shipped. Validated on k3s with 3-node gossip cluster.
> `make conformance` scores **340/365 (93.2%)** on
> [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests).
> See [`PLAN.md`](PLAN.md) for the roadmap.

---

## Highlights

- **Protocols**: HTTP/1.1 and HTTP/2 on the data plane.
  `net/http` on a separate admin port for the operator surface.
- **Embedded storage**: sharded in-RAM hot tier + mmap warm tier. No
  external KV.
- **Clustering**: gossip membership + consistent hash + peer fetch. K8s
  StatefulSet friendly.
- **Compliance**: **93.2 % on [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests)**
  (340/365). Covers RFC 9111 freshness, stale-while-revalidate, stale-if-error, CDN-Cache-Control,
  heuristic caching, Vary, conditional requests, and `must-understand`.
- **Performance**: zero-alloc hit path, benchmark-gated CI.
- **Observability**: Prometheus, OpenTelemetry, slog, pprof.
- **Migration**: NGINX migration guide included.

---

## Quickstart

```bash
git clone https://github.com/thylong/bouine.git
cd bouine

# One-time setup: install pre-commit hooks (mandatory, see AGENTS.md §2.11).
make hooks

# Build, test, lint.
make build       # -> ./bin/bouine
make test        # -> go test -race ./...
make lint        # -> golangci-lint run

# Verify the binary runs and the admin port responds.
./bin/bouine version
./bin/bouine serve &
curl -sf http://127.0.0.1:9000/healthz
kill %1
```

### Admin authentication

All write endpoints on the admin port require a bearer token.
Set it in your config:

```yaml
admin:
  token: your-secret-token
```

If no token is configured, bouine generates a random one at startup
and logs it as a `WARN`. Retrieve it with:

```bash
make admin-token CONFIG=path/to/config.yaml  # from config file
# or from logs if auto-generated:
./bin/bouine serve ... 2>&1 | grep 'admin token'
```

Use it in CLI commands with `--token`:

```bash
bouine purge https://example.com/page --token your-secret-token
```

---

## Kubernetes

```bash
docker buildx build --platform linux/amd64 -t bouine:dev --load .

helm install bouine deploy/helm/bouine \
  --namespace bouine --create-namespace \
  --set image.repository=bouine \
  --set image.tag=dev \
  --set image.pullPolicy=Never \
  --set "config.upstream_pools[0].name=app" \
  --set "config.upstream_pools[0].targets[0]=app.default.svc:8080" \
  --set "config.routes[0].pool=app" \
  --set config.cluster.enabled=true
```

The chart deploys a 3-replica StatefulSet with gossip clustering,
a headless Service for peer discovery, and a PodDisruptionBudget.
See [`deploy/helm/bouine/values.yaml`](deploy/helm/bouine/values.yaml)
for all options.

---

## Documentation

| Topic                            | Where                                                 |
|----------------------------------|--------------------------------------------------------|
| Roadmap, architecture, phases    | [`PLAN.md`](PLAN.md)                                  |
| Working agreement (binding)      | [`AGENTS.md`](AGENTS.md)                              |
| Threat model                     | [`docs/security/threat-model.md`](docs/security/threat-model.md) |
| Contributing (humans)            | [`CONTRIBUTING.md`](CONTRIBUTING.md)                  |
| Security policy & disclosure     | [`SECURITY.md`](SECURITY.md)                          |
| Migration from NGINX             | [`docs/migration/nginx.md`](docs/migration/nginx.md)   |
| Decision records (ADRs)          | [`docs/decisions/`](docs/decisions/)                  |
| SLO / SLI reference              | [`docs/operations/slo.md`](docs/operations/slo.md)    |
| Soak + chaos report (v1.0 gate)  | [`docs/operations/soak-chaos-report.md`](docs/operations/soak-chaos-report.md) |

---

## Project layout

The high-level Go module layout follows the layered architecture in
[`PLAN.md §2.2`](PLAN.md). Lower numbers are closer to the wire.

```
/cmd/bouine                  Cobra entrypoint
/internal/server             L1 — HTTP/1, /2, TLS, route matching
/internal/cache              L4 — RFC 9111 state machine, Vary, conditionals
/internal/storage            L3 — RAM tier, mmap tier, eviction, WAL
/internal/origin             L5 — upstream pool, health, hedge, breaker
/internal/cluster            L6 — gossip, consistent hash, peer fetch
/internal/admin              L7 — net/http admin: purge, ban, config, dash
/internal/observability      L8 — OTEL, Prom, slog, pprof
/internal/config             config loader, schema, hot reload
/internal/prefetch           prefetcher: Link preload + sitemap crawler
/internal/ai                 L9 — traffic analytics (phase 6)
/internal/vcl                VCL-compatible shim (deferred — see §18)
/pkg/bouineapi               public Go SDK
/pkg/api                     shared types between SDK, admin server, dashboard
```

---

## License

[Apache License 2.0](LICENSE).

## Contributing

All contributors are bound by [`AGENTS.md`](AGENTS.md). Humans should
start at [`CONTRIBUTING.md`](CONTRIBUTING.md); AI agents start at
[`AGENTS.md`](AGENTS.md). Security issues go through
[`SECURITY.md`](SECURITY.md), never public issues.
