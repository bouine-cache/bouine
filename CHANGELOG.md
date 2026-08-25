# Changelog

All notable changes to bouine are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Release notes for tagged versions are also generated from
[Conventional Commits](https://www.conventionalcommits.org/); this file is
the curated, human-readable summary.

## [Unreleased]

### Added
- Streaming origin responses with pipelined peer fetch.
- TCP_QUICKACK on accepted connections for reduced latency.

### Changed
- Migrated the entire data plane from `net/http` to `fasthttp`, achieving
  zero-allocation hit path and exceeding pre-migration benchmark performance.
- Pre-computed cache-control flags, status lines, Date formatting, and Vary
  values for hot-path performance.
- Byte-based `ParseCacheControl`, `isInvalidating`, and header comparisons
  throughout the cache pipeline.
- `SetCanonical` for ETag and X-Cache headers on revalidate/error paths.
- Lazy string conversion in `RequestInfo` via `[]byte` fields.

### Removed
- HTTP/2 support (fasthttp is HTTP/1.1 only).
- `net/http` from all production code and test files.

### Fixed
- Capped per-stream tee buffer to prevent OOMKill under slow-origin, derived
  cap from `GOMEMLIMIT`.
- Capped `fasthttp.Server.Concurrency` and reduced `MaxConnsPerHost` to curb
  FD exhaustion and status-0 errors.
- Resolved all fasthttp migration conformance regressions.
- Used short `MaxIdleConnDuration` in peer fetch tests to prevent goroutine
  leak timeout.
- Resolved test deadlock and suppressed CodeQL request-forgery false positives.

## [0.4.3] - 2026-08-21

### Added
- Nightly build workflow.
- Cloudflare invalidation propagation with batching, circuit breaker, DLQ,
  and multi-token support.

### Changed
- Go toolchain bumped from 1.26.6 to 1.27.0.
- Per-segment incremental compaction to eliminate periodic latency spikes.

### Fixed
- Cluster: gated storage behind ownership in strong mode for 3x fleet cache.
- Bench: skipped unit tests in bench-gate to prevent CI timeout.
- Multiple nightly workflow failures resolved across lint, fuzz, docker, and
  stress-test jobs.

## [0.4.2] - 2026-08-19

### Added
- Cachaner eviction policy with pluggable selection.
- Evictor package for pluggable eviction policies.

### Changed
- Dashboard accessibility improvements for WCAG 2.0 AA.
- DCO sign-off required on all commits.
- Docker build context trimmed via `.dockerignore`.
- CI jobs (conformance, bench, integration) run in parallel.

### Fixed
- Stayin-alive log levels corrected, WAL snapshot written on close.
- Build env vars, debug stripping, and reproducible builds.
- Decoupled hot SIEVE eviction from warm tombstone.
- Poll `/readyz` instead of `/healthz` in integration driver.
- Test coverage increased across dashboard, server, admin, `pkg/api`, and
  `cmd` packages.

## [0.4.1] - 2026-08-17

### Fixed
- Go stdlib CVE fixes (toolchain bumped to 1.26.6).
- Static-file handler: removed global MIME table mutation, fixed range
  double-open, added mtime ETag fallback.
- Platform `pwritev`: completed short writes and returned a typed error
  from the non-Linux stub.
- Admin API: wired `MaxBodyBytes` through config and rejected empty batch
  URLs (adversarial input hardening).

### Added
- Homebrew formula and automated update workflow.
- CodeQL code-scanning workflow for Go.
- Codecov coverage reporting and badge.
- Fuzz targets for cache key, Vary, storage codec, and cluster codec.
- Goroutine-leak detection and storage-corruption tests.
- TLS and HTTP/2 integration tests.

### Changed
- Test coverage push: cache 67.8% → 91.1%, server 34.1% → 65.2%,
  dashboard 27.8% → 58.4%, observability 55.0% → 79.8%, platform 9.1% →
  100% (non-Linux).
- Merged standalone `_extra_test.go` / `coverage_test.go` files into
  existing test files.
- Auto-rebase CI hardened (PAT, mergeStateStatus UNKNOWN, self-hosted
  runner no-op).

## [0.4.0] - 2026-08-11

URL normalization, 128-bit cache keys, admin API & CLI additions,
Helm/Grafana expansion. A release candidate (`v0.4.0-rc1`) was published
on 2026-08-11 with the same content.

### Added
- **URL normalization** for cache-key canonicalisation (maximises hit
  ratio across equivalent URLs).
- **128-bit cache key** via a single XXH128, replacing the previous
  double-hash scheme.
- **Admin API:** `GET /v1/stats`, `GET /v1/config`, and
  `GET /v1/debug/cachecheck` (cache-key debugging endpoint).
- **CLI:** `config validate`, `config schema`, shell completion commands,
  and `surrogate_key` support on purge/ban/refresh.
- **Config:** environment-variable interpolation in config files.
- **Helm chart:** ServiceAccount, Ingress, PodMonitor, extraVolumes,
  NOTES.txt, values schema, GPG signing for Artifact Hub,
  PrometheusRule CRD with alerting rules, chart tests, and values
  examples.
- **Grafana:** storage, cluster, and ops dashboards.
- **OpenAPI 3.0** spec for the admin API.
- Warm-tier disk-exhaustion runbook.
- Community section, contributors section, architecture diagram, and
  pronunciation guide in README.
- CodeQL and benchmark/conformance/integration/chaos CI gates enabled.

### Changed
- **Breaking (SDK):** removed `bouineapi.Client.Reload` and `ReloadResult`
  (see `[Unreleased]` above). The admin `POST /v1/config/reload` endpoint,
  the dashboard "Reload config" button, and `ReloadFn` are gone; bouine
  applies config via rolling pod restart.
- Simplified config by dropping dead fields and inferring others.
- Test assertions migrated to testify (ADR-0028).
- Linux CI jobs moved to self-hosted runners.
- Cosign-signed Docker images in the release workflow.
- Docker images signed; base image and dependencies bumped for CVE fixes.

### Fixed
- Cache: purge now deletes Vary variants stored under composite keys;
  `Vary:*` is non-matchable per RFC 9111 §4.1; restored stale-on-error
  fallback without SIE window; stopped value-copying
  `api.Object`'s `atomic.Pointer[[]byte]`; owned SWR background
  goroutines to prevent use-after-close on shutdown.
- Cluster: capped `HandoffQueueDepth` upper bound in `cluster.New`;
  eliminated data race in gossip drop metric; detached broadcast context
  from engine lifecycle and shared fetcher TLS transport; increased
  handoff queue depth.
- Storage: prevented warm.Compact racing with concurrent Puts; prevented
  tombstone queue overflow with a dedicated drain goroutine; report
  dropped tombstones in drain goroutine.
- Admin: exempted `/v1/peer/metrics` from auth; cleared per-request write
  deadline on `/drain` so the preStop hook survives; dropped no-op
  config-reload feature.
- Origin: split active/passive health counters, fixed ejection dead under
  load.
- Server: added HTTP smuggling defenses to `h1parser`.
- Observability: prevented attacker-controlled `route` label via inbound
  header.
- Dashboard: closed reflected-XSS in `apiOK`/`apiError` via templ.
- SDK: capped error body, added default timeout, removed dead `Stable`
  types.
- Bench: raised `Handler_CacheMiss_Cacheable` gate to 58.
- Six low-effort correctness and security fixes from status review.
- Anonymised internal infrastructure leaks and added benchmark disclaimer.
- Corrected inaccurate NGINX and Varnish migration guides.

## [0.3.7] - 2026-07-16

### Changed
- Hot tier Go heap moved into mmap to reduce GC pressure at scale.

## [0.3.6] - 2026-07-16

### Fixed
- `serializeHead` was too memory-intensive when no cache hits were present.

## [0.3.5] - 2026-07-15

### Changed
- Major warm-tier performance improvements (fewer allocations, faster
  compaction).

## [0.3.4] - 2026-07-16

### Fixed
- Bounded concurrent connections to prevent FD exhaustion under load.

### Changed
- Pre-serialized response headers in the HTTP/1.1 fast path.

## [0.3.3] - 2026-07-15

### Fixed
- Zero-allocation hit-path regression on the fast path.

## [0.3.2] - 2026-07-15

### Fixed
- Performance regression on the fast path introduced in v0.3.0.

## [0.3.1] - 2026-07-15

### Fixed
- Race condition and deadlock in the cluster peer-fetch path.

## [0.3.0] - 2026-07-14

### Changed
- Major performance improvements: RPS-per-core closure across 9 optimisation
  phases to close the gap with other HTTP caches. Includes fast-path tricks
  for the hit path.

## [0.2.7] - 2026-07-13

### Changed
- Drastically improved eviction efficiency in the SIEVE hot-tier implementation.

## [0.2.6] - 2026-07-13

### Fixed
- Cluster joining regression introduced in v0.2.5.

## [0.2.5] - 2026-07-13

### Fixed
- Startup failure under WAL pressure and lock contention.

## [0.2.4] - 2026-07-12

### Changed
- Performance tuning to match expected RPS targets.

## [0.2.3] - 2026-07-12

### Changed
- Massive performance tuning, including Linux-specific optimisations.

## [0.2.2] - 2026-07-11

### Added
- Advanced refresh feature for high-cardinality and low-TTL scenarios
  (refresh-before-expiry with configurable thresholds).

## [0.2.1] - 2026-07-11

### Fixed
- In-flight crashes on origin errors (nil-pointer on cancelled upstream
  responses).

## [0.2.0] - 2026-07-08

### Changed
- Repository transferred to `bouine-cache` org and `bouine.org` domain.
- Async WAL fsync to eliminate goroutine serialisation.
- Warm-tier eviction (SIEVE) implemented with proper cross-tier coordination.
- Warm-tier metrics and dashboard panels exposed.

### Fixed
- WAL init wired to `OpenAsync` and flushed on `Close`.
- SIEVE `Delete` leak, `Put` overwrite stats, `Compact` pool drain.
- Compaction temp-store eviction, `stats.bytes` recomputed from index after
  compact.

## [0.1.25] - 2026-07-07

### Fixed
- WAL slow startup under large segment count.

## [0.1.24] - 2026-07-06

### Changed
- Improved refresh feature to increase cache hit ratio.

## [0.1.23] - 2026-07-06

### Changed
- Major improvements to the refresh (soft-purge) feature.

## [0.1.22] - 2026-07-06

### Changed
- Peer-fetch performance improvements.

## [0.1.21] - 2026-07-05

### Fixed
- Release automation fix (no code change).

## [0.1.20] - 2026-07-05

### Added
- pprof endpoints on the admin port.

### Changed
- Object size reporting accuracy (`objSize` honesty).
- `hot_store_bytes` documentation.

## [0.1.19] - 2026-07-03

### Added
- Refresh-before-expiration feature.
- Source metrics for cache objects.
- Warm-tier blob eviction.

### Fixed
- Eviction vs anti-entropy interaction.

## [0.1.18] - 2026-07-03

### Changed
- RAM and heap enhancements, autoscaling improvements.

## [0.1.17] - 2026-07-02

### Added
- Custom binary codec for gossip serialisation (lower CPU + bandwidth).
- Admin auth and rate limiting hardening.

## [0.1.16] - 2026-07-02

### Added
- Anti-entropy metrics and fix.

### Changed
- Go dependency updates, cache memory improvement.

## [0.1.15] - 2026-07-01

### Fixed
- Warm-tier fix, cache RAM stability improvements.

## [0.1.14] - 2026-07-01

### Fixed
- Cache and OOM fixes.

### Changed
- Insights dashboard improvement.

## [0.1.13] - 2026-07-01

### Fixed
- Same fixes as v0.1.12; bumped to force Docker pull (stale layer cache).

## [0.1.12] - 2026-07-01

### Fixed
- Cache OOM improvement (bounded inline eviction).
- Dashboard improvements.

### Fixed
- Cache correctness fixes.

## [0.1.11] - 2026-06-30

### Added
- Dashboard improvements, full-mode replication reconciliation.

### Fixed
- Multiple bug fixes across cluster and dashboard.

## [0.1.10] - 2026-06-29

### Fixed
- Multiple critical fixes across cache, cluster, and storage.

## [0.1.9] - 2026-06-26

### Fixed
- OTLP endpoint scheme stripping (final OpenTelemetry fix).

## [0.1.8] - 2026-06-26

### Fixed
- Negative duration rejection in route cache and pool policies.
- OpenTelemetry configuration fix.

## [0.1.7] - 2026-06-26

### Fixed
- Latency histogram merging across peer summaries.
- Multiple bug fixes.

## [0.1.6] - 2026-06-25

### Added
- `exclude_headers` config field to exclude request headers from
  Vary-based cache keys.

## [0.1.5] - 2026-06-25

### Fixed
- Long URL issue (stack buffer bumped to 4 KB, long-URL benchmark added).

## [0.1.4] - 2026-06-25

### Fixed
- Peer-fetch latency measurement (around the RPC, not after it).
- Multiple performance improvements and fixes.

## [0.1.3] - 2026-06-19

### Fixed
- `ttl_default` now honored for responses with no freshness headers.

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

## [0.1.0] - 2026-06-10

First tagged feature release of bouine — a cloud-native HTTP cache in Go
(RFC 9111 compliant, zero-alloc hit path, gossip clustering, no external
K/V store).

### Added

Five new per-route configuration capabilities for production caching:

- **Method-based route matching (`match.methods`)** — restrict a route to
  specific HTTP methods (e.g. `[GET, HEAD]`), so reads and writes on the same
  path can have independent cache policies and pools.
- **Upstream path rewriting (`request.strip_prefix`)** — strip a path prefix
  before forwarding to the upstream (e.g. `/api/v1/users` → `/users`). The
  cache key keeps the original path, so routes never collide.
- **Object size limit (`cache.max_object_size`)** — skip caching responses
  larger than a configured size; they are still proxied, so large downloads
  don't evict useful entries.
- **Cache-key query stripping (`cache.key.strip_query_params`)** — drop
  tracking/analytics params (`utm_source`, `fbclid`, …) from the cache key
  while still forwarding them to the origin — eliminating cache fragmentation.
  Zero added allocations on the hit path.
- **Per-route `ttl_override`** — decouple bouine's storage lifetime from the
  upstream `Cache-Control` forwarded to a downstream CDN.

### Changed

- **Breaking:** Responses carrying `Set-Cookie` are **no longer cached by
  default**, matching nginx's `proxy_cache` behaviour and preventing
  session-cookie replay across users. Opt back in per-route with
  `cache.allow_set_cookie: true` (which caches but strips `Set-Cookie` from
  the stored copy). Operators who intentionally cached such responses must
  add this field.

## [0.0.9] - 2026-06-09

Helm and documentation public release. Pre-release covering open-source
readiness: documentation sync, security hardening, dependency audit,
Helm chart publishing to Artifact Hub, and CI pipeline.

### Added
- Helm chart published to Artifact Hub.
- Gitleaks allowlist and dependency license audit.
- Storage metrics, exemplars, trace propagation, dashboard panels.
- Hot store panels to RED dashboard.
- Cloudflare CDN invalidation propagation.
- Eventual and full consistency modes for clustering.
- SLO documentation, rolling-restart runbook, OTel L1 tracing.
- Conformance +21 tests, broadcast metrics, soak/chaos, Varnish guide.
- Background sweeper + bounded inline eviction.
- Lock-free ban check on hot Get path.
- In-process integration tests (Docker dropped).
- Chaos test suite with 8 scenarios.

### Changed
- Layer renumbering: L3–L9 → L2–L8 after L1+L2 merge.
- Merged L1+L2, replaced collapse with singleflight, dropped prefetch and
  TLS watcher.
- Dropped PROXY protocol, HTTP/3, W-TinyLFU phantom, Experimental struct.
- `cloudflare-go` v2 → v4 migration.
- Docker image location switched to Docker Hub.

### Fixed
- All 16 audit findings (correctness, cluster, observability, maintenance).
- PartialPartition chaos test de-flake.
- Golangci-lint errors from full-scan CI step.

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

- **342/365 (93.7%)** on
  [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests).

### Not yet implemented (deferred — see `docs/architecture.md §1.2`)

- Prefetching (Link preload / sitemap crawler).
- HTTP/3 (client- and origin-facing).
- VCL-compatible shim.
- Data-plane authentication and per-route rate limiting.
- AI traffic-analysis insights.

[Unreleased]: https://github.com/bouine-cache/bouine/compare/v0.4.3...HEAD
[0.4.3]: https://github.com/bouine-cache/bouine/releases/tag/v0.4.3
[0.4.2]: https://github.com/bouine-cache/bouine/releases/tag/v0.4.2
[0.4.1]: https://github.com/bouine-cache/bouine/releases/tag/v0.4.1
[0.4.0]: https://github.com/bouine-cache/bouine/releases/tag/v0.4.0
[0.3.7]: https://github.com/bouine-cache/bouine/releases/tag/v0.3.7
[0.3.6]: https://github.com/bouine-cache/bouine/releases/tag/v0.3.6
[0.3.5]: https://github.com/bouine-cache/bouine/releases/tag/v0.3.5
[0.3.4]: https://github.com/bouine-cache/bouine/releases/tag/v0.3.4
[0.3.3]: https://github.com/bouine-cache/bouine/releases/tag/v0.3.3
[0.3.2]: https://github.com/bouine-cache/bouine/releases/tag/v0.3.2
[0.3.1]: https://github.com/bouine-cache/bouine/releases/tag/v0.3.1
[0.3.0]: https://github.com/bouine-cache/bouine/releases/tag/v0.3.0
[0.2.7]: https://github.com/bouine-cache/bouine/releases/tag/v0.2.7
[0.2.6]: https://github.com/bouine-cache/bouine/releases/tag/v0.2.6
[0.2.5]: https://github.com/bouine-cache/bouine/releases/tag/v0.2.5
[0.2.4]: https://github.com/bouine-cache/bouine/releases/tag/v0.2.4
[0.2.3]: https://github.com/bouine-cache/bouine/releases/tag/v0.2.3
[0.2.2]: https://github.com/bouine-cache/bouine/releases/tag/v0.2.2
[0.2.1]: https://github.com/bouine-cache/bouine/releases/tag/v0.2.1
[0.2.0]: https://github.com/bouine-cache/bouine/releases/tag/v0.2.0
[0.1.25]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.25
[0.1.24]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.24
[0.1.23]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.23
[0.1.22]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.22
[0.1.21]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.21
[0.1.20]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.20
[0.1.19]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.19
[0.1.18]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.18
[0.1.17]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.17
[0.1.16]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.16
[0.1.15]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.15
[0.1.14]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.14
[0.1.13]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.13
[0.1.12]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.12
[0.1.11]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.11
[0.1.10]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.10
[0.1.9]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.9
[0.1.8]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.8
[0.1.7]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.7
[0.1.6]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.6
[0.1.5]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.5
[0.1.4]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.4
[0.1.3]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.3
[0.1.2]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.2
[0.1.1]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.1
[0.1.0]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.0
[0.0.9]: https://github.com/bouine-cache/bouine/releases/tag/v0.0.9
[1.0.0]: https://github.com/bouine-cache/bouine/releases/tag/v1.0.0
