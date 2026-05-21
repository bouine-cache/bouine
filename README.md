# bouine

`bouine` is a horizontally-scalable, observability-first HTTP reverse-proxy
cache written in Go 1.26. It targets the same problem space as Varnish
(RFC 9111 cache, fast purge, predicate-based bans) but is designed from
day one for Kubernetes, multi-instance clustering, and first-class
metrics/traces/logs.

> Status: **v1.0-rc** — phases 0–7 complete. Caching, clustering,
> prefetching, negative caching, jittered TTLs, soft-purge, and the Go
> SDK are shipped. Validated on k3s with 3-node gossip cluster.
> See [`PLAN.md`](PLAN.md) for the roadmap.

---

## Highlights

- **Protocols**: HTTP/1.1, HTTP/2, and HTTP/3 (QUIC) on the data plane.
  `net/http` on a separate admin port for the operator surface.
- **Embedded storage**: sharded in-RAM hot tier + mmap warm tier. No
  external KV.
- **Clustering**: gossip membership + consistent hash + peer fetch. K8s
  StatefulSet friendly.
- **Compliance**: targets parity with Varnish on
  [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests).
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

---

## Project layout

The high-level Go module layout follows the layered architecture in
[`PLAN.md §2.2`](PLAN.md). Lower numbers are closer to the wire.

```
/cmd/bouine                  Cobra entrypoint
/internal/listener           L1 — HTTP/1, /2, /3, TLS, PROXY proto
/internal/pipeline           L2 — normalization, ACL, collapsing
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

## CI status

<!-- TODO: replace with the real shields once the repo is public. -->

| Pipeline   | Status |
|------------|--------|
| CI         | ![ci](https://img.shields.io/badge/ci-pending-lightgrey) |
| Coverage   | ![coverage](https://img.shields.io/badge/coverage-pending-lightgrey) |
| cache-tests| ![cache-tests](https://img.shields.io/badge/cache--tests-pending-lightgrey) |
| Bench diff | ![bench](https://img.shields.io/badge/bench-pending-lightgrey) |
| govulncheck| ![vuln](https://img.shields.io/badge/govulncheck-pending-lightgrey) |

---

## License

[Apache License 2.0](LICENSE).

## Contributing

All contributors are bound by [`AGENTS.md`](AGENTS.md). Humans should
start at [`CONTRIBUTING.md`](CONTRIBUTING.md); AI agents start at
[`AGENTS.md`](AGENTS.md). Security issues go through
[`SECURITY.md`](SECURITY.md), never public issues.
