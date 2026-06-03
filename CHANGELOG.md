# Changelog

All notable changes to bouine are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Release notes for tagged versions are also generated from
[Conventional Commits](https://www.conventionalcommits.org/); this file is
the curated, human-readable summary.

## [Unreleased]

### Changed
- Migrated `cloudflare-go` v2 → v4 (maintained major; identical purge surface).
- Documentation corrected for open-source release (project layout, Makefile
  targets, security reporting channel).

## [1.0.0] - 2026-06-XX

First public release. A horizontally-scalable, observability-first HTTP/1.1
+ HTTP/2 reverse-proxy cache, designed for Kubernetes.

### Added

- **Cache engine (RFC 9111)** — freshness, `stale-while-revalidate`,
  `stale-if-error`, heuristic caching, `Vary` canonicalisation, conditional
  requests (ETag / Last-Modified), `CDN-Cache-Control`, `must-understand`,
  negative caching, jittered TTLs, soft-purge (refresh).
- **Storage (L2)** — sharded in-RAM hot tier with SIEVE eviction and a
  lock-free ban check on the Get path; mmap-backed warm tier with a
  write-ahead index log and background tombstone compaction; background
  eviction sweeper.
- **Clustering (L5)** — `memberlist` gossip membership, consistent-hash ring
  with bounded loads, peer fetch (HTTP/2 over mTLS), purge/ban broadcast,
  anti-entropy reconciliation, versioned wire protocol. Strong / eventual /
  full-replication consistency modes.
- **Origin (L4)** — connection pool, active + passive health checks, hedged
  requests, request collapsing, circuit breaker, per-pool upstream TLS
  (mTLS, custom CA, SPKI pinning).
- **Control plane (L6)** — `net/http` admin API: purge, ban, refresh,
  config reload, cluster peers, stats, health/readiness, Prometheus,
  pprof. Bearer-token / mTLS auth on write endpoints.
- **Operator dashboard** — embedded `templ` + htmx UI (throughput, cache
  breakdown, cluster ring, invalidation log), session-cookie auth.
- **Observability (L7)** — Prometheus metrics, OpenTelemetry traces (OTLP),
  structured `slog` access logs, pprof.
- **Cloudflare integration** — propagates purge/ban/refresh to the
  Cloudflare Cache API.
- **CLI** — `serve`, `purge`, `ban`, `refresh`, `cluster peers`, `version`.
- **Go SDK** — `pkg/bouineapi` typed client for the admin API.
- **Kubernetes** — Helm chart (StatefulSet, headless Service, PDB,
  topology spread, NetworkPolicy), graceful shutdown sequence.

### Performance

- Zero-allocation hit path (`allocs/op = 0` on `HotStore_Get_Hit`,
  `SIEVE_Access`, `Evaluate_Hit`), benchmark-gated in CI.

### Compliance

- **340/365 (93.2%)** on
  [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests).

### Not yet implemented (deferred — see `PLAN.md`)

- Prefetching (Link preload / sitemap crawler).
- HTTP/3 (client- and origin-facing).
- VCL-compatible shim.
- Data-plane authentication and per-route rate limiting.
- AI traffic-analysis insights.

[Unreleased]: https://github.com/thylong/bouine/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/thylong/bouine/releases/tag/v1.0.0
