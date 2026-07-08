# Changelog

All notable changes to bouine are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Release notes for tagged versions are also generated from
[Conventional Commits](https://www.conventionalcommits.org/); this file is
the curated, human-readable summary.

## [Unreleased]

## [0.1.2] - 2026-06-15

### Added
- New operator dashboard **Performance** page collecting latency telemetry:
  p50 / p90 / p99 and average latency KPIs, a latency-distribution
  histogram, a latency-over-time chart (p99 + average), and derived health
  signals — Apdex score, SLO compliance bars (≤10ms / ≤100ms / ≤1s), and
  tail-latency ratios.

### Changed
- De-cluttered the dashboard **Overview**: the latency-distribution chart
  moved to the new Performance page and the stale-serving panel was removed.
- Moved the **Cloudflare CDN** status block from the Overview to the
  **Invalidation** page, where CDN propagation is operationally relevant.
- The chart release no longer claims the repository "Latest" pointer; the
  application release always owns it, so the documented install one-liner
  never resolves to a binary-less chart release.

## [0.1.1] - 2026-06-15

### Added
- Operator dashboard overview now reports a real latency distribution:
  p50 / p90 / p99 percentiles and a log-scale histogram chart, replacing the
  previous running-max p99 approximation. Latency is recorded on the
  alloc-free request hot path.
- Overview stale-serving panel surfacing stale-served and revalidation rates
  per minute and the stale share.
- Route table now shows the request methods and per-route cache features
  (TTL overrides, query-param stripping, max object size, etc.).
- Full-replication clusters get a dedicated dashboard replication panel
  (objects/bytes sent and received, throughput, last activity).
- Build version is shown in the dashboard sidebar.

### Changed
- Dashboard tagline no longer describes bouine as a "reverse-proxy".
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

[Unreleased]: https://github.com/bouine-cache/bouine/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/bouine-cache/bouine/releases/tag/v1.0.0
