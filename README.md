<p align="center">
  <img src="docs/assets/logo_font.png" alt="bouine" width="280">
</p>

<p align="center">
  <em>pronounce: /bwin/</em>
</p>

<p align="center">
  <a href="https://github.com/bouine-cache/bouine/actions/workflows/ci.yml"><img src="https://github.com/bouine-cache/bouine/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://github.com/bouine-cache/bouine/actions/workflows/release.yml"><img src="https://github.com/bouine-cache/bouine/actions/workflows/release.yml/badge.svg" alt="Release"></a>
  <a href="https://github.com/bouine-cache/bouine/releases/latest"><img src="https://img.shields.io/github/v/release/bouine-cache/bouine" alt="Latest Release"></a>
  <a href="https://hub.docker.com/r/bouinecache/bouine"><img src="https://img.shields.io/docker/pulls/bouinecache/bouine" alt="Docker Pulls"></a>
  <a href="https://hub.docker.com/r/bouinecache/bouine"><img src="https://img.shields.io/docker/v/bouinecache/bouine?logoColor=blue&color=blue" alt="Docker"></a>
  <a href="https://artifacthub.io/packages/helm/bouine/bouine"><img src="https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/bouine" alt="Artifact Hub"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"></a>
</p>

bouine is a cloud-native HTTP cache in Go — RFC 9111
compliant, zero-alloc hit path, gossip clustering, no external K/V store.
It targets the same problem space as a classic HTTP cache but is designed from day one for Kubernetes, multi-instance clustering, and first-class observability.

> Status: **v1.0-rc** — core caching, clustering, negative caching,
> jittered TTLs, soft-purge, and the Go SDK are shipped. Validated on k3s
> with 3-node gossip cluster.
> `make conformance` scores **342/365 (93.7%)** on
> [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests).
> See [`CHANGELOG.md`](CHANGELOG.md) for what's shipped and what's deferred
> (prefetching, HTTP/3, VCL shim, and AI insights are not yet implemented).

---

## Highlights

- **Protocols**: HTTP/1.1 and HTTP/2 on the data plane.
  `net/http` on a separate admin port for the operator surface.
- **Embedded storage**: sharded in-RAM hot tier + mmap warm tier. No
  external KV.
- **Clustering**: gossip membership + consistent hash + peer fetch. K8s
  StatefulSet friendly.
- **Compliance**: **93.7 % on [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests)**
  (342/365). Covers RFC 9111 freshness, stale-while-revalidate, stale-if-error, CDN-Cache-Control,
  heuristic caching, Vary, conditional requests, and `must-understand`.
- **Performance**: zero-alloc hit path, benchmark-gated CI.
- **Observability**: Prometheus, OpenTelemetry, slog, pprof.
- **Migration**: NGINX and Varnish migration guides included.

### Why bouine?

| | bouine | Varnish | NGINX |
|---|---|---|---|
| Architecture | Reverse-proxy cache | Reverse-proxy cache | Reverse proxy + cache |
| Clustering | Gossip + consistent hash (built-in) | Commercial (Varnish Plus) or external orchestration | Upstream hash (no cluster state) |
| Cache invalidation | Purge + ban (predicate) + soft-purge | Purge + ban | Purge only (no ban) |
| Observability | Prometheus + OTel + slog (built-in) | Vmod-based | Third-party or NGINX plus |
| Config | YAML (declarative) | VCL (imperative) | NGINX.conf |
| Kubernetes | StatefulSet + Helm chart, first-class | Sidecar / external | Ingress controller pattern |

### Architecture

```
                    ┌─────────────────────────────────────────┐
  Client ──HTTP──►  │                 bouine                  │  ──►  Origin
                    │  ┌───────────┐   ┌───────────┐          │
                    │  │ Hot tier  │   │ Warm tier  │          │
                    │  │ (RAM)     │   │ (mmap disk)│          │
                    │  └───────────┘   └───────────┘          │
                    │  ┌────────────────────────────┐         │
                    │  │ Cache engine (RFC 9111)    │         │
                    │  │ Vary · SWR · SIE · purge   │         │
                    │  └────────────────────────────┘         │
                    └──────────┬──────────┬──────────┬────────┘
                          gossip   peer fetch   metrics / traces
                              │         │          │
                     ┌────────▼───┐     │     ┌────▼─────┐
                     │  peers     │◄────┘     │  Prom    │
                     │ (cluster)  │          │  OTel    │
                     └────────────┘          └──────────┘
```

---

## Quick Start

Run bouine with Docker and cache a request in under a minute.

Create a minimal config:

```bash
cat > bouine.yaml <<EOF
listen:
  http: ":8080"
  admin: ":9000"

upstream_pools:
  - name: origin
    targets: ["httpbin.org:80"]

routes:
  - match: {}
    pool: origin
    cache:
      ttl_default: 60s
EOF
```

Run bouine:

```bash
docker run -d --name bouine -p 8080:8080 -p 9000:9000 \
  -v "$PWD/bouine.yaml:/etc/bouine/config.yaml" \
  bouinecache/bouine:latest serve --config /etc/bouine/config.yaml
```

Test it — first request is a MISS, second is a HIT:

```bash
curl -s -I http://localhost:8080/get | grep x-cache
# X-Cache: MISS

curl -s -I http://localhost:8080/get | grep x-cache
# X-Cache: HIT
```

Check the admin endpoint:

```bash
curl -s http://localhost:9000/healthz
# ok
```

<details>
<summary>Demo: first request is a MISS, second is a HIT</summary>

```
$ curl -sI http://localhost:8080/get
HTTP/1.1 200 OK
Content-Type: application/json
X-Cache: MISS
Age: 0

$ curl -sI http://localhost:8080/get
HTTP/1.1 200 OK
Content-Type: application/json
X-Cache: HIT
Age: 1
```

</details>

### Install

```bash
# Prebuilt binary (from releases)
curl -L https://github.com/bouine-cache/bouine/releases/latest/download/bouine-v0.3.7-linux-amd64 -o bouine
chmod +x bouine

# or via Go
go install github.com/bouine-cache/bouine/cmd/bouine@latest

# or Docker
docker pull bouinecache/bouine:latest
```

### Building from source

```bash
git clone https://github.com/bouine-cache/bouine.git
cd bouine
make build   # -> ./bin/bouine
./bin/bouine serve --config bouine.yaml
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the full development setup.

---

## Kubernetes

```bash
helm repo add bouine https://charts.bouine.org
helm repo update

helm install bouine bouine/bouine \
  --namespace bouine --create-namespace \
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

📖 **Full documentation: [bouine.org](https://bouine.org)**
(getting started, configuration, operations, guides). The in-repo docs
below are the canonical source the site is built from.

| Topic                            | Where                                                 |
|----------------------------------|--------------------------------------------------------|
| Getting started & install        | [bouine.org/docs/getting-started](https://bouine.org/docs/getting-started/) |
| Architecture reference           | [`docs/architecture.md`](docs/architecture.md)        |
| Configuration reference          | [bouine.org/docs/configuration](https://bouine.org/docs/configuration/) |
| Migration from NGINX             | [`docs/migration/nginx.md`](docs/migration/nginx.md)   |
| Migration from Varnish           | [`docs/migration/varnish.md`](docs/migration/varnish.md) |
| Contributing                     | [`CONTRIBUTING.md`](CONTRIBUTING.md)                  |
| Changelog                        | [`CHANGELOG.md`](CHANGELOG.md)                        |
| Security policy & disclosure     | [`SECURITY.md`](SECURITY.md)                          |
| Discussions & community          | [GitHub Discussions](https://github.com/bouine-cache/bouine/discussions) |

---

## License

[Apache License 2.0](LICENSE).

## Contributing

All contributors are bound by [`AGENTS.md`](AGENTS.md). Humans should
start at [`CONTRIBUTING.md`](CONTRIBUTING.md); AI agents start at
[`AGENTS.md`](AGENTS.md). Security issues go through
[`SECURITY.md`](SECURITY.md), never public issues.

## Community

- **[GitHub Discussions](https://github.com/bouine-cache/bouine/discussions)** —
  ask questions, share configs, and discuss design decisions.
- **[GitHub Issues](https://github.com/bouine-cache/bouine/issues)** —
  bug reports and feature requests (use the issue templates).
- **[Security advisories](https://github.com/bouine-cache/bouine/security/advisories/new)** —
  private vulnerability reporting.
