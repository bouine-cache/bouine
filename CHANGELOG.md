# Changelog

All notable changes to bouine are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Release notes for tagged versions are also generated from
[Conventional Commits](https://www.conventionalcommits.org/); this file is
the curated, human-readable summary.

## [Unreleased]

### Fixed
- Data race on shutdown caught by the nightly -race integration run
  (TestTLS_CertRotation): `PeerFetcher.Close` swapped the bare
  `pipelineClients` sync.Map field while concurrent Fetch/Put goroutines
  read it. The client map now lives behind an `atomic.Pointer`;
  `Close` drops it atomically and post-Close RPCs fail fast with a
  "fetcher closed" error (callers already fall back to origin).
  Regression-tested with concurrent Close/Fetch/Put under -race.
- Nightly conformance job failed on GitHub's transient unauthenticated
  clone rate limit ("temporarily limiting some unauthenticated
  downloads"): the cache-tests clone now uses `gh` when available
  (authenticated, as on Actions runners) and retries plain git up to 3
  times with a 30s gap.
- Nightly cluster and dashboard-under-load jobs failed because the
  self-hosted load-test-k8s runner image is missing `make`: both jobs
  now self-heal by installing it when absent (the runner user has
  sudo), keeping the nightly green until the runner image is rebuilt
  with make baked in — after which the steps become no-ops and can be
  dropped.

### Added
- Data-integrity regression net for the hot-store ownership bug class
  (bouine#611): a slow-client body-lifetime race on the standard
  fasthttp hit path (`ServeRequest` — the path that kept corrupting
  after the fast path was disabled) and on the Linux epoll reactor hit
  path; a chaos scenario that validates every payload byte under SIEVE
  eviction churn with a buffer-reusing origin (working set over a 2 MiB
  hot budget, `ClusterOptions.HotMaxBytes` knob added to the driver);
  and load-test scenario §3.7 (payload integrity under eviction churn,
  50k deterministic 64 KiB payloads, per-request boundary/rotating-probe
  checks plus sampled full byte compares, zero-tolerance threshold)
  registered in the nightly suite. All integrity layers verified
  red-capable: reverting the CloneForStorage fix makes each fail.

## [0.5.7] - 2026-09-03

### Fixed
- Hot-store entries aliased the caller's body and header buffers when
  the slab was disabled (the default): `Put` stored the caller's
  `*Object` as-is, so any post-Put mutation or buffer reuse on the
  origin/revalidation/peer-promote paths changed the bytes an in-flight
  fast-path hit writev was serving — clients received well-framed 200
  responses with mutated or reused body bytes (the preprod front-office
  `Cannot read properties of undefined (reading 'forEach')` 500s on
  `/content/page/*`). `Put` now always stores a cache-owned clone
  (`Object.CloneForStorage`: copied body, deep-cloned header map), so
  every body a hit can alias is immutable-after-store and GC-pinned
  for the life of the write. The hit path stays zero-allocation; the
  copy lands on the miss path only. Regression-tested with a
  slow-reading client racing concurrent Put-overwrite, SIEVE eviction
  pressure, and caller buffer reuse.

## [0.5.6] - 2026-09-03

### Added
- Helm chart metadata controls (PR #602): global `commonLabels` /
  `commonAnnotations` applied to every rendered resource, and
  resource-specific labels/annotations for the data-plane Service
  (plus `loadBalancerSourceRanges` and `externalTrafficPolicy` when
  `type: LoadBalancer`), StatefulSet, HPA, NetworkPolicy, PDB,
  PrometheusRule, ServiceMonitor, Ingress, and ServiceAccount. A new
  dedicated `adminService` (ClusterIP by default) splits the admin
  plane (metrics, pprof, `/drain`, admin API) off the data-plane
  Service, so exposing the data plane via LoadBalancer can never
  expose the admin surface. With `autoscaling.enabled: true` the
  StatefulSet no longer renders `spec.replicas` — the HPA owns the
  replica count; GitOps users (e.g. ArgoCD) should add an
  `ignoreDifferences` entry for `/spec/replicas`.
- `admin.idle_timeout` (default 300s, PR #606): keep-alive idle
  timeout for admin-server connections, including cluster peer RPCs
  (`/v1/peer/*`). Previously a hard-coded 30s.

### Fixed
- Cluster peer RPC stale-connection failures (PR #606): the admin
  server's 30s idle timeout reaped keep-alive connections while peer
  clients still held them pooled, so the next peer-fetch/peer-put
  failed with EOF or broken pipe ("error in PipelineClient: EOF" in
  preprod) and fell back to origin, spiking latency and wasting origin
  bandwidth; `fasthttp.PipelineClient` does not retry requests that
  die on a pooled connection. The fix orders the timeouts instead of
  papering over them with retries: the peer client idle default stays
  at 120s, the admin server default rises to 300s so idle peer
  connections survive quiet periods, and config validation rejects any
  explicit `cluster.peer_max_idle_conn_duration >= admin.idle_timeout`
  at load time (plus negative values for either) so operator overrides
  cannot reintroduce the race.
- WAL async drain wrote one O_DSYNC `Write` syscall per entry, so
  draining a full sync channel (4096 entries) at Close issued 4096
  synchronous writes — slow enough on saturated disks to exceed test
  timeouts (CI hung in `TestAsyncDropOnFull` /
  `TestDroppedEntriesResets` cleanup) and avoidable syscall overhead in
  production. Each drain batch is now coalesced into a single durable
  Write on a reused ~168 KiB scratch buffer. The WAL write-duration
  metrics test is also made deterministic via `Sync()` instead of a
  fixed sleep racing the sync-loop ticker.

### Changed
- Helm: the `podAntiAffinity` values key (added in 0.5.3) is removed
  in favor of the raw `affinity` values (explicit affinity takes
  precedence); templates are normalized on `with` statements instead
  of mixed `if`/`with` logic.

## [0.5.5] - 2026-09-02

### Added
- **H1 epoll reactor** (`experimental.h1_reactor`, Linux-only, requires
  `experimental.h1_fast_path`, default off; ADR-0041): a
  single-goroutine event loop per plaintext listener that serves
  batches of cache hits from one `epoll_wait` wakeup — parse, cache
  lookup, and `writev` flush inline on raw fds, with no goroutine
  park/unpark per request. Gate benchmark: 370–405 ns and 0 allocs/op
  per reactor hit. Non-hit traffic (miss, conditional, range, pipelined
  bodies, oversize headers, malformed input) hands off *before any
  response byte is written* to the existing blocking parser with the
  buffered bytes replayed, so fall-through framing, smuggling 400s, and
  SWR semantics are shared, not reimplemented. Bounded by design: 4096
  connections per loop (overflow falls back to the blocking path), a
  bounded handoff-spawn queue, and the loop goroutine as sole owner of
  the epoll set. TLS listeners are never reactor-served. Config
  validation rejects `h1_reactor` without `h1_fast_path` at load time.
  The reactor is enabled in the nightly load-test configuration, so
  benchmark numbers from 0.5.4 and earlier are not comparable.
- Reactor steady-state safety nets, each observable per runbook
  `docs/runbook/51-h1-reactor.md`: a 5-minute write-timeout sweep drops
  clients that stop reading mid-response (they would otherwise pin the
  per-loop connection budget); an idle sweep closes keep-alive
  connections at `listen.idle_timeout` parity with the blocking path;
  async hit metrics use a per-loop ring (drop-newest on overflow,
  counted and logged at shutdown) so the metric hook is never serial
  loop time; and stuck-writer, spawner-saturation, and shutdown-storm
  regression tests pin the contracts.

### Fixed
- `request.strip_prefix` on proxied routes was parsed and validated but
  never applied since the fasthttp-native migration (v0.5.0): origins
  received the full prefixed path (issue #595). The stripped URI is now
  written at every origin-bound request site (foreground and streaming
  miss, bypass, foreground and SWR-background revalidation,
  refresh-before-expiry, POST invalidation), while cache keys, ban
  matching, `X-Bouine-Path`, and `Location` keep the original path per
  the documented contract. `stripPrefixFastHTTP` (static routes) now
  strips path and query together, fixing a dropped query string; the
  boundary rules live in one exported helper shared by both sites.
- Nightly load-test runner: the k6 install had failed with "Permission
  denied" on every nightly since Aug 9 (23 consecutive red runs, no
  performance baseline since the fast path landed) because the container
  runs as uid 1001 against a root-owned `/usr/local/bin`. The install
  now uses sudo and hands ownership to the runner user; a prerequisites
  check fails fast with a clear message.
- Nightly load-test suite reliability: the load-gen container needed
  bash for its scenario drivers, its memory limit OOM-killed every k6
  scenario, it could not write to the `/results` bind mount, and
  compose reused stale per-project images instead of the built ones —
  all now fixed; the restored origin Dockerfile needed its fasthttp
  dependency.

### Changed
- Reactor loop cost work (all measured, gates and cache-tests
  conformance unchanged): hit responses flush via one zero-copy,
  zero-alloc `writev` over the fast path's `net.Buffers` with
  exact-offset resume on partial writes (previously a full-body memcpy
  per hit); redundant `epoll_ctl` re-arming is elided by tracking
  per-connection interest; four O(NHeaders) header re-scans and the
  ~3.3 KB per-request struct memset collapse into a fused header-parse
  pass with a ScanFlags bitmask; hits within a cached wall-clock second
  reuse a fully serialized response head stored on the object.
  FastPath_Hit gate: 181→129 ns (−29%, benchstat p=0.002).
- Reactor keep-alive RTT: a bounded adaptive busy-poll after each
  served batch cut single-client keep-alive p50 from 41.7 to 10.2 µs
  (−75%) and lifted sustained 16-client throughput from 138k to 160k
  RPS (+16%) at equal CPU, with measured zero CPU ticks over an idle
  2 s window (not a busy loop). The spin budget defaults to 80 and is
  operator-overridable via `BOUINE_REACTOR_SPIN_BUDGET` (0 disables) for
  A/B and field rollback.
- The nightly scenario set is trimmed to fit the job budget: the
  20-minute `3.4_working_set_overflow` eviction scenario and the
  duplicated 50k/100k ramp legs are cut from the nightly default
  (restorable ad hoc via `SCENARIOS`/`RATES_OVERRIDE`), and a
  competitor's k6 threshold no longer fails the suite.

## [0.5.4] - 2026-09-01

### Added
- `listen.idle_timeout` (default 120s): one knob for the client-facing
  keep-alive idle timeout, replacing the hard-coded 120s literals
  duplicated across the fasthttp listeners and the H1 fast-path parser
  (which remain as zero-value fallbacks). With an upstream proxy or LB
  in front, keep its keep-alive idle timeout below this value so it
  closes idle connections first.
- `upstream_pools[].connect.max_idle_conn_duration` (default 90s): how long
  idle pooled origin connections are kept. Keep it below any LB idle
  timeout between bouine and the origin (e.g. AWS NLB 350s).

### Fixed
- Helm chart: the HPA rendered `behavior.scaleDown.stabilizationSeconds`,
  a field that does not exist in the `autoscaling/v2` API, so the API
  server rejected the HPA at apply time for any install with
  `autoscaling.enabled: true` (present since chart 0.1.2, issue #582).
  The template now renders `stabilizationWindowSeconds`; the
  `autoscaling.scaleDownStabilizationSeconds` values key is unchanged
  but its default is lowered from 300 to 120 for faster scale-down
  reaction.
  Rendered chart manifests are now validated against Kubernetes strict
  schemas (kubeconform) on every commit that touches the chart.
- The `upstream_pools[].connect.*` settings (`timeout`, `keep_alive`,
  `max_connections`, `response_header_timeout`) were validated but silently
  ignored by the origin client (issue #579; `hedge_timeout` remains
  reserved for future use). They now flow into the shared origin fasthttp
  client. Zero values keep the previous hard-coded defaults (10s dial,
  30s keep-alive, 64 conns/host, 30s response header), so no config
  change is needed.
- Origin connections are now pooled per upstream pool instead of per route
  handler: `connect.max_connections` is enforced once per origin host even
  when several routes share a pool, and repeated handler construction no
  longer replaces the pool's client.

## [0.5.3] - 2026-08-31

### Added
- Slow-origin overload shedding (issue #562): foreground misses that
  cannot acquire an origin-fetch slot within the new
  `fetch_wait_timeout` (default 100ms, validated max 1s) shed instead of
  parking without bound — a stale object in scope is served stale
  (RFC 5861-style), otherwise the client gets 503 + `Retry-After: 1`,
  distinct from the 502 origin-failure mapping. Singleflight and
  inflight-stream followers un-park with the leader's shed result. The
  new `bouine_fetch_shed_total` counter exposes the shed rate for
  alerting.
- Helm: expanded StatefulSet controls — pod annotations/labels,
  affinity and raw podAntiAffinity, nodeSelector, tolerations,
  priorityClassName, dnsPolicy/dnsConfig, updateStrategy.type,
  podManagementPolicy, warm-volume-claim labels/annotations, optional
  persistentVolumeClaimRetentionPolicy, and ServiceMonitor
  relabelings/metricRelabelings.

### Fixed
- Slow-origin livelock (issue #562): foreground miss paths parked on
  the per-route origin-fetch semaphore with a dead cancellation arm
  (`context.Background()`), so arrival rate above drain rate piled
  request goroutines without bound until the pod entered a
  non-recovering livelock. Slot acquisition now tries non-blocking
  first (zero allocs), then a timer-bounded wait before shedding.
- H1 fast path (experimental.h1_fast_path): five latent correctness
  gaps closed, then enabled in the loadtest configuration — every
  previous nginx/varnish/envoy comparison had accidentally measured
  the slow middleware path with the zero-alloc hit parser unused.
  - Fast-path StaleHit now triggers stale-while-revalidate background
    revalidation (an `onStale` hook wired by the engine); previously
    stale objects served via the fast path never refreshed.
  - Fall-through request bodies are no longer truncated when they span
    multiple TCP reads: the fallback handler re-parses the request
    from a buffered prefix + the live socket with full framing
    (Content-Length, chunked, trailers, Expect: 100-continue).
  - Bytes pipelined after a cache-hit request are consumed by the
    fallback handler instead of being silently discarded.
  - Requests with headers larger than the 16 KiB parser buffer are
    served via the fallback handler instead of being dropped.
  - Ambiguous framing (Content-Length + Transfer-Encoding, duplicate
    Content-Length) is rejected with 400 and connection close per
    RFC 9110 §6.6.2 instead of being served.
- h1parser keep-alive idle timeout raised from 10s to 120s to match
  the fasthttp listener (visible in k6 as elevated reconnection time).
- Fast-path hits report `route=_default` in Prometheus labels (was the
  empty string, taking the slow WithLabelValues fallback path).
- h1parser clock now uses `platform.CoarseNow` on Linux, matching the
  dataplane middleware (~2-4ns vs ~25-40ns per call).

### Added
- Experimental epoll reactor for batch cache-hit serving
  (`experimental.h1_reactor`, Linux only, requires `h1_fast_path`,
  default off; ADR-0041). One goroutine per listener multiplexes all
  hit-path connections — one `epoll_wait` wakeup serves a batch
  instead of one goroutine park/unpark per request, which is the
  residual structural gap to nginx's worker event loop. Misses,
  conditional requests, ranges, pipelined bodies, and oversize
  headers hand off to the existing blocking parser path unchanged.
  Enabled in the loadtest configuration as the measured increment on
  top of the blocking-path fast-path numbers the nightly runner
  established; if nightly numbers don't move, the flag goes back off.

### Fixed (reactor review round)
- Handed-off connections no longer leak their fd: the blocking-parser
  goroutine spawned at handoff now closes the connection when Serve
  returns (previously every miss/handoff pinned its fd in CLOSE_WAIT
  forever, burning the fd table at miss-heavy traffic).
- Reactor shutdown is wired: ctx cancellation closes the listener and
  stops the loop, Listener.Shutdown drains in-flight handed-off
  requests via a WaitGroup, and the accept loop survives transient
  Accept errors (EMFILE, ECONNABORTED) instead of dying permanently —
  previously the loop spun forever after cancellation and the shutdown
  sequencer closed the store under live handed-off requests.
- The reactor's idle budget is now per-request (measured from the
  request's first byte), closing the slowloris hole where a client
  dribbling one byte per interval kept resetting a last-byte-based
  clock forever.
- The idle sweep no longer kills connections that are mid-flush to a
  slow client: writers are governed by the write safety net, not the
  read idle budget.
- `Connection: close` on a cache hit is honored: the fast path emits
  the close trailer (RFC 9110 §9.6) and both the blocking parser and
  the reactor close the connection after the response instead of
  parking it for the full 120s idle window.
- The reactor's first hit no longer pays a redundant `epoll_ctl MOD`:
  registration records the armed interest mask, so the common
  full-flush case issues zero `epoll_ctl` syscalls per request, as the
  code comments always claimed.
- Fast-path hits reuse a per-second composed response head cached on
  the object: status line + static + dynamic headers are a pure
  function of the object, unix second, and composition inputs, so hits
  inside a cached second skip per-hit header appends entirely (the
  last real per-hit CPU in the fast path after the parser work).
- The reactor's per-hit scratch re-zero (~4 KiB copy per request,
  duplicating the reset parseBuffer already does) and a dead
  never-read `writeVecOffs` field were removed.
- The two broken epoll tests were fixed or deleted: the keep-alive
  test actually serves two hits on one connection now, and the
  miss-handoff test that asserted nothing is replaced by one that
  proves the handoff serves the miss and closes the connection.

### Changed
- `bench/loadtest/config/bouine.yaml` enables
  `experimental.h1_fast_path` so proxy comparisons exercise the
  production hit path. Nightly loadtest results from 0.5.2 and earlier
  were measured without it and are not comparable.
- `listen.max_connections` now ships enabled (4096 default config,
  8192 production / 16384 HA Helm values): under HTTP/1.1 a parked
  handler holds its connection, so the cap puts a hard ceiling on the
  goroutine pile even if parking is ever reintroduced. Idle keep-alive
  connections hold a slot too.
- Helm: the duplicate PodMonitor was removed in favor of the
  ServiceMonitor, whose default scrape interval relaxed from 15s to
  60s; default topologySpreadConstraints now use ScheduleAnyway with
  both zone and hostname keys.
- `request_duration_seconds` no longer enables Prometheus
  native-histogram bucketing: the sparse-bucket math cost per Observe
  was called out in the hit-path plan, and no dashboard queries native
  histograms (all PromQL uses classic `_bucket` series).

## [0.5.2] - 2026-08-30

### Added
- `fetch_timeout` is now actually enforced in production: the previous
  context-based timer never reached the transport, so every origin
  fetch ran under a fixed 60s fallback regardless of configuration.
  Foreground and streaming fetches now use kernel-level connection
  deadlines; background fetches use a deadline context so shutdown
  cancellation still works.
- `make bump-go-stamp` updates the GO_VERSION_STAMP that keys the CI
  prek cache (see below).
- Integration test for graceful shutdown over TLS (the protocol-
  independent intent previously covered by the deleted HTTP/2 test).

### Fixed
- Hit-path tail latency: the h1parser allocated a ~4KB request struct
  per request (99.3% of allocation volume under load), driving ~63 GC
  cycles/s. Requests now reuse a per-connection scratch struct —
  measured allocation drop of ~466× and GC cycles from 1074 to 5 per
  load window, p99 −10%.
- Data race in streaming singleflight: followers could read the
  leader's response headers while the pooled fasthttp response was
  being reset for reuse (visible in the chaos suite under `-race`).
- Data race in stale-while-revalidate: the background revalidation
  goroutine read request method/URI/host bytes from the connection's
  recycled request buffers after the handler returned.
- Integration driver: cross-node tests failed with `lookup testhost:
  no such host` — the driver dialed the Host-header override instead
  of only overriding the header.
- `Vary: *` on any field line now blocks storage: the Map-based check
  read only the first header value and missed stars on later lines
  (caught by cache-tests vary-syntax-empty-star-lines).
- Multi-line `Cache-Control` and `Vary` response headers now combine
  per RFC 9110 §5.2 in the miss-path cacheability check.
- Prometheus label classification: the middleware fallback path now
  reports the real HTTP method instead of squashing it to "OTHER".
- Nightly stress test (k8s): the kubeconfig step hard-failed with
  `base64: invalid input` when the secret was stored as raw YAML; it
  now accepts either encoding and fails with an actionable message.
- False-positive goleak failures from fasthttp background goroutines.
- Deleted the accidentally resurrected HTTP/2 integration tests: the
  data plane is HTTP/1.1-only by ADR-0034, so multiplexing and h2c
  tests cannot pass by design.

### Changed
- MISS-path allocation cuts (measured on the cacheable-miss benchmark:
  24 → 13 allocs/op, −30% CPU; no-store miss −14%; Vary miss −13%;
  data-plane middleware 4 → 0 allocs/op with access logging off):
  transient origin bodies are transferred by buffer ownership instead
  of a full-body copy; non-cacheable misses no longer build a header
  map (byte-level cacheability precheck); unique per-object header
  values (X-Bouine-Path/Host) skip global interning; the singleflight
  leader table is sharded instead of a sync.Map; the access-log-only
  cacheKey user value is stored only when key logging is enabled.
- Benchmark gate `H1Parse_Get` now exercises the full production parse
  path so per-request allocations fail the zero-alloc budget.
- The `Handler_CacheMiss_Cacheable` allocation budget is 13 (was 23
  pre-optimization; briefly 24 while `pkg/unique` header interning
  added an entry-node allocation per miss).
- CI prek hook-environment cache re-enabled: its cache key is now
  Go-version-stamped via `.pre-commit-config.yaml`, so a Go bump can
  no longer serve a stale golangci-lint binary (the failure that
  originally forced the cache off). A new prek hook enforces the
  stamp stays in sync with go.mod.

## [0.5.1] - 2026-08-26

### Added
- Stress-test diagnostics metrics for WAL, warm tier, origin, and peer
  fetch subsystems (15 new Prometheus metrics).
- `hot_store_max_bytes` and `warm_store_max_bytes` gauges for fill-ratio
  computation.
- `request_queue_depth` and `peer_fetch_active` gauges for CPU starvation
  and in-flight RPC detection.

### Changed
- Reordered Go struct fields across 58 files to minimize padding waste
  (govet fieldalignment), and enabled the fieldalignment linter to
  prevent regressions.
- Zero-copy header interning with `unsafe.String` in `FromFastHTTP` —
  header keys and values are converted to strings without allocation,
  then interned via `unique.Make`.
- `warm_mmap_page_faults_total` renamed to
  `warm_mmap_resident_page_delta_total` to accurately describe the
  mincore-based resident-page delta it measures.
- WAL `WriteTotal` now counts all write attempts including drops; drop
  rate is `drops / writes`.
- `classifyConnError` helper distinguishes timeout/refused/reset/error
  instead of hardcoding generic labels.

### Fixed
- Keep-alive preserved after cache miss in h1parser — the connection is
  no longer terminated on every fallthrough, reducing connection churn
  under mixed hit/miss workloads.
- Reduced `SetReadDeadline` syscalls: deadline is set once at connection
  start and refreshed lazily (only when remaining time drops below 2s).
- `Connection: close` propagation from request to response per
  RFC 9110 §7.6.1.
- `Compact()` early return now observes compaction duration even for
  no-op compactions.
- `TestLog_QueueDepth` now asserts the correct queue depth instead of
  `>= 0`.
- `TestMetrics_ObserveDurationZero` no longer uses a pointless
  `time.Sleep`.

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

[Unreleased]: https://github.com/bouine-cache/bouine/compare/v0.5.6...HEAD
[0.5.6]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.6
[0.5.5]: https://github.com/bouine-cache/bouine/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/bouine-cache/bouine/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.3

[0.5.2]: https://github.com/bouine-cache/bouine/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/bouine-cache/bouine/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/bouine-cache/bouine/compare/v0.4.3...v0.5.0
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
