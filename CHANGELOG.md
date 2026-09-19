# Changelog

All notable changes to bouine are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Release notes for tagged versions are also generated from
[Conventional Commits](https://www.conventionalcommits.org/); this file is
the curated, human-readable summary.

## [Unreleased]

### Added

- `routes[].cache.negative_ttl` now accepts a per-status map, mirroring
  Cloudflare's "Cache TTL by status code": `negative_ttl: {404: 1m,
  5xx: 10s, 410: 0}`. Keys are a single error status ("404") or a
  class ("4xx", "5xx"); exact codes are limited to 400-599 (2xx/3xx
  entries are rejected as configuration errors). An exact code shadows
  its class ("blanket + exception": `5xx: 10s, 503: 30s`). Values are
  durations; zero explicitly disables caching for that status. The
  policy only applies when the origin sends no explicit freshness, and
  RFC 9111 blocking directives still win.
- docs: trim code comments across the hot-path packages (cache, storage,
  server, cluster, origin) to why-only statements: restatements of the
  next line or the signature deleted, RFC-clause citations and
  non-obvious decisions kept. Adds the reusable
  `docs/agents/code-comment-condensation.md` prompt.

### Changed

- **One negative-caching key.** The scalar `negative_ttl: 30s` and the
  new map form are the same setting written two ways; the scalar is
  shorthand for the default error set (404/405/410/501). There is no
  separate `status_ttl` key and no fallback interaction: the map form
  is the complete policy.
- **Negative-caching TTL precedence.** A negative-caching policy now
  outranks `ttl_default` and heuristic freshness (Last-Modified) — but
  only for the statuses it covers. Previously, a route with both
  `negative_ttl` and `ttl_default` cached 404s for the `ttl_default`
  duration, and an error response echoing `Last-Modified` could be
  heuristic-cached for far longer than the operator-configured negative
  TTL. If you relied on the old shadowing, remove the negative-caching
  entry for the affected statuses. Statuses the policy does not cover
  keep the pre-existing resolution unchanged: heuristic freshness
  (Last-Modified) still outranks `ttl_default`.
- **Refresh exclusion is policy-driven.** Objects with an error status
  covered by the negative-caching policy are never proactively
  refreshed, regardless of how they were cached. Statuses outside the
  policy (including all 2xx/3xx) refresh normally.
- docs: condense documentation without losing knowledge — fixed stale
  HTTP-stack facts in `README.md` and `docs/architecture.md`, removed
  `full` cluster-mode content (removed in ADR-0025) from the
  cluster-modes runbook, deduplicated the runbook index, replaced the
  duplicated ADR-0016 draft in the refresh-before-expiry plan with a
  pointer, condensed the refresh-prioritization revision history, and
  repaired a dangling plan reference in ADR-0023.
- docs: remove completed one-time migration plans with no inbound
  references (`transfer-to-bouine-cache-org`,
  `cluster-local-cache-mode`), resolve duplicate ADR numbers by
  reassigning changelog-automation to ADR-0047, the kubeconform hook to
  ADR-0048, and PurgeEvent.VaryKey to ADR-0049, and complete the ADR
  index in `docs/decisions/README.md` (including marking the removed
  0002/0003 records).

## [0.5.21] - 2026-09-17

### Changed
- **Unified RFC 9111 evaluation (issue #589)** — the three hand-maintained
  copies of the cache decision state machine (`Evaluate` on the header.Map
  path, `evaluateFast` on the fasthttp Peek path, `evaluateFromRaw` on the
  H1 fast path) and the three Vary variant-key computations (`VariantKey`,
  `VariantKeyFast`, `variantKeyFromRaw`) were collapsed into one shared
  `evaluate` core and one generic `variantKeyCore`. The H1 fast path gains
  the stale-if-error, validator-aware no-cache, and heuristic-freshness
  branches its mirror had drifted to lack; its variant-key overflow now
  falls back to the allocation path instead of silently returning the
  primary key (which could select the wrong variant).
### Removed
- **Dead scaffolding from completed ADR migrations (issue #593)** —
  zero-caller leftovers of ADR-0015/0034/0036 removed: the `server`
  package's fast-path type aliases, the `reportFastPathError` no-op stub
  and its `errCh` plumbing, the never-implemented
  `api.FastPathHandlerCtx` interface, the `NewPeerFetcher` /
  `NewPeerFetcherWithLogger` convenience constructors (use
  `NewPeerFetcherWithConfig`), the pre-ADR-0015 `Bouine-Issuer` /
  `Bouine-Seq` / `Bouine-Issued-At` / `Bouine-Method` header constants,
  `header.Map.SetValues`, and the `mergeHeaderValues` wrapper (callers now
  use `header.Map.GetAll` directly). `InternKeyCanonical`/`InternValue`
  were unexported to `internKeyCanonical`/`internValue` (used only inside
  `pkg/header`).
- **`experimental.fasthttp_migration` config flag** — the ADR-0034
  migration is complete and the flag gated nothing; it is ignored if
  still present in existing configs.

### Added
- **Regex path rewriting (`request.path_rewrite`)** — a per-route regex
  rewrite applied to the origin-bound request path, the nginx
  `rewrite ... break` / Varnish `regsub` equivalent. `match` (Go RE2 —
  linear time, no ReDoS) and `replace` (`$1` index and `$name` capture
  references, `${1}x` braced disambiguation) are compiled once at config
  load and applied exactly once on every origin-bound fetch: miss,
  bypass, invalidating methods (POST/PUT/DELETE), foreground
  revalidation, SWR background revalidation, refresh-before-expiry, and
  stream fetches. Hardening, each pinned by tests: the query string is
  split off before matching and re-appended unchanged (the pattern can
  never swallow `?signature=...`), only the first match is replaced
  (nginx semantics), a relative result is discarded (the origin request
  line is never corrupted), output is capped at 16 KiB (blocking
  `$1$1$1` amplification), pattern and template are capped at 512 B,
  raw control bytes and raw request-target bytes (space, `?`, `#`) are
  rejected in the template, and every `$reference` in the template must
  resolve against the pattern's capture groups at config load — Go's
  Expand would otherwise silently expand an unknown reference to the
  empty string (the `$1x` typo is a lookup of group "1x"; a padded
  index like `$01` is a name lookup, not group 1). The `${VAR}` config
  interpolation only applies to env-var-shaped names, so `${1}x` loads
  exactly as written. Mutually exclusive with `request.strip_prefix`
  (validation rejects both). The cache key, ban matching, purges, and
  all client-facing surfaces keep the original public path, so
  invalidation addresses the URLs clients request. The hit path is
  untouched: zero allocs/op gates hold, and non-matching URIs pass
  through at 4 ns / 0 allocs.
- `cache.key.include_headers` (issue #632) — a per-route allow-list of
  request headers that participate in the variant key exactly as if the
  origin had listed them in `Vary`: the include list is unioned with the
  response's `Vary` at object-build time (never a replacement), so the
  hit path, the H1 fast path, and every peer variant gate work
  unchanged. A request header absent from the request hashes as an
  empty value (one variant), matching RFC 9111 Vary semantics. Use it
  when the origin varies by a header (e.g. `Accept-Language`) but does
  not send `Vary`. Validation (trimmed entries, compared as stored)
  rejects `*` (padded or not), whitespace-only entries, non-token
  entries (commas, spaces — RFC 9110 §5.1), case-insensitive
  duplicates, entries also present in `exclude_headers`, and lists
  longer than 16 entries. Routes without
  an include list keep the zero-allocation passthrough, so miss-path
  alloc budgets are unchanged (ADR-0046). The flagship config example
  in `docs/architecture.md` now parses under the strict decoder — a
  regression test extracts and validates it on every run. 304
  revalidation recomputes `VaryKey` from the merged union so the
  stored `VaryValue`/`VaryKey` pair can never skew across a Vary
  change, and a `Vary: *` 304 blanks `VaryKey` (fail-safe: failed
  hits, never wrong bodies).

### Fixed
- `request.strip_prefix` on cache-enabled static routes was applied
  twice on every origin fetch: the strip/rewrite wrappers were wired
  into the upstream handler chain that the cache handler invokes, on
  top of the cache handler's own origin-bound URI rewriting. A public
  path carrying the prefix twice (`/api/api/f`) was stripped down to
  `/f` instead of `/api/f`. The upstream handed to the cache handler is
  now the bare static handler; the cache handler's origin-bound URI
  rewriting is the single application point, and the in-process bypass
  fallback applies it in place. The same restructure keeps
  `request.path_rewrite` single-application on cached static routes
  (non-idempotent patterns were silently applied twice).
- `${VAR}` environment-variable interpolation in config files now
  applies only to env-var-shaped names (a letter or underscore, then
  letters, digits, or underscores). Braced runs that are not plausible
  environment variable names — digit-leading sequences such as `${1}`
  (a path_rewrite capture-group reference) — were silently replaced
  with the value of an env var that cannot exist, usually the empty
  string. They now survive the loader verbatim.
- H1 fast-path hits are attributed to the route's `upstream_pool`
  (issue #696). The fast path was built from the bare store with no
  route knowledge, so every fast-path hit — local and, with
  `experimental.h1_fast_peer_path`, peer-fetched — was recorded with
  `upstream_pool="_default"`; enabling `h1_fast_path` fleet-wide made
  per-pool dashboard series vanish wholesale (a prod incident read as
  "peer fetch stopped working" while the service was healthy). The
  fast path is now built per route from that route's cache handler and
  selected by a router-backed wrapper (`RoutedFastPath`), so hits
  carry the route's configured pool and run under the route's
  `cache.key` policy (fewer `exclude_headers`/`include_headers` Vary
  fall-throughs). Three incidental fixes land with it: per-route
  `onStale` wires SWR through the route's own upstream (the old
  store-level wiring used the first refresh-enabled handler for every
  route, and nothing when no route configured `refresh_before_expiry`);
  the peer branch is no longer inherited by handler construction (it
  is applied explicitly behind `h1_fast_peer_path`, never under the
  reactor); and requests matching no route or a non-cached route fall
  through to the slow path instead of being served store-level ghost
  hits. Route resolution adds 0 allocs/op and ~22 ns per hit
  (`BenchmarkGate_RoutedFastPath_Hit`).
- The cluster's peer PipelineClient diagnostics no longer bypass the
  structured log pipeline. Every "error in PipelineClient(...)" line
  from fasthttp's pipeline worker — dial refusals, EOFs, broken pipes,
  timeouts — went to fasthttp's raw stderr logger and landed in log
  shippers as unstructured info-level lines (the entire content of the
  prod-eu log export on 2026-09-16: 59 entries during a single
  rolling-restart window, 41 of them bouine's own retired-address
  parking). The per-peer PipelineClient is now built with the
  client-side FastHTTPLogger adapter: records are tagged
  `component=cluster` and classified by transport error — routine
  teardown (EOF, broken pipe) and retired-address drain log at Debug,
  degraded peers (connection refused, timeouts, anything unrecognized)
  log at Warn. Peer fetch/put on the hit path is unchanged (0 allocs/op
  bench-gate maintained); the adapter only runs on the worker's error
  path.

## [0.5.20] - 2026-09-16

### Added
- The H1 fast path can now serve peer-fetched objects without falling
  through to the slow path (issue #636, experimental). On a strong-cluster
  node, a plain-key miss on the fast path asks the key's ring owner first
  and serves the peer's object directly through `FastPathHandler` with
  `X-Cache-Source: peer` — removing one full parser/handler round-trip
  per peer hit — without storing (no ring placement to enforce) and
  without allocating. The transient fields the wire codec drops
  (CacheControl flags, HasDate) are re-derived so freshness
  evaluation matches local hits, and on any peer error the fast path
  falls through to the slow path's shed/origin machinery unchanged.
  The branch requires the new `experimental.h1_fast_peer_path` flag
  (default off, rejected at load time without `h1_fast_path`), is
  not wired under the epoll reactor (TryHit must never block on
  network I/O), and logs an error at startup when requested but
  unavailable (cluster not in strong mode) instead of silently
  no-oping. Two gating benchmarks pin the budgets:
  `FastPath_PeerHit` at 0 allocs/op and `FastPath_PeerHitVary`
  (the RFC 9111 §4.1 variant gate) at 7 allocs/op. An end-to-end
  integration test (3-node ring, fixed-Host key) proves non-owner
  requests are served with zero origin traffic. On a definitive
  owner miss the fast path flags the request so the slow path skips
  its identical second owner lookup + peer RPC and goes straight to
  origin; errors and variant-gate rejections keep the slow-path
  retry, and policied routes keep their retry too since the slow
  path's gate computes a different VaryKey.
- `bouine_peer_fetch_shed_total` — peer fetch/put RPCs no longer
  park indefinitely when the concurrency semaphores are
  saturated (see the saturation fix below): a shed is counted, and
  the queue wait that led to it is observed in
  `peer_fetch_queue_wait_seconds` (previously only successful
  acquisitions landed in the histogram, so a fully-shedding queue
  disappeared from the metrics).
- `bouine_rewarm_fill_total` — background refills scheduled after a
  shed foreground miss (see the saturation fix below) are counted
  next to `bouine_fetch_shed_total`.

### Fixed
- A 3-node miss-storm stress run (surrogate-key purge of 80% of a
  360k keyspace at T+15m, 6k req/s, fast path OFF) exposed two
  compounding saturation behaviors and one operational trap. All
  three are closed; rationale and alternatives in ADR-0045
  (bounded-peer-shed-and-rewarm):
  1. Peer-fetch/put semaphore waits are now bounded. A caller with
     an undelined context (the fast path passes
     context.Background to stay zero-alloc) previously parked
     forever when all slots were busy — every keep-alive
     connection goroutine parked in the queue (4,386 parked
     goroutines at end of run vs 225 on the slow-path arm). Slot
     acquisition now uses the same two-stage shape the origin
     fetch path got in issue #562: non-blocking send, then a
     100ms timer, then shed with `ErrPeerFetchShed`. Clients keep
     their 503/stale protection; the fast path already falls
     through on any peer error.
  2. A shed miss no longer loses the refill. After a purge ban,
     every shed foreground miss fetched and stored nothing, so
     the miss storm pinned the hit ratio at the shed equilibrium
     (~7% for the rest of the run, 461k sheds) — each shed was a
     lost refill and re-warm never converged. A shed miss now
     schedules a bounded background refill on a dedicated pool
     (32 per handler, separate from the foreground fetchSem it
     was just shed from), singleflight-collapsed with foreground
     fetches and drained on Close. Clients keep their
     503/stale protection; the store re-warms anyway.
  3. The lazy-ban list TTL is now configurable via
     `cluster.ban_ttl` (default 24h, validated >= 1s when set).
     The TTL was a hard 24h constant, so a typo in a
     surrogate-key ban poisoned the hit ratio for a full day.
     RFC 9111 §4.4 exempts post-ban copies and the reaper reclaims
     pre-ban ones, so cache-lifecycle invalidations are safe at
     minutes scale.
- The fast-path owner-miss hint is only honored on routes with no
  KeyPolicy. The fast path builds keys without the route's
  KeyPolicy (it is constructed per-engine from the shared store),
  so on a policied route its miss was computed under a different
  key and proved nothing about the slow path's key — honoring it
  silently converted peer hits into origin fetches on routes with
  query-param stripping. A variant-gate rejection now also flags
  the request so a nil-policy slow path skips the deterministically
  rejected duplicate peer RPC and goes straight to origin;
  reqHeaderMapFromRaw right-trims OWS so both parsers provably
  agree (parity test gained a trailing-OWS case).
- The Helm chart again accepts an empty `config.listen.https` (and any
  empty listen address). The 0.5.19 schema patterns and the
  `bouine.listenPort` helper turned the app's documented "empty string
  disables the plane" form — the shape used by `make test-k8s-setup` and
  by every deployment that terminates TLS at an upstream proxy/LB — into
  an install-time error, leaving rendered manifests failing with
  "Does not match pattern" in CD pipelines. An empty address now disables
  the derived wiring with it: the StatefulSet containerPort, the
  data-plane Service port (its named targetPort would otherwise dangle),
  the NetworkPolicy rule, and the NOTES port listing. Non-empty values
  are validated exactly as before, and the kubeconform gate now renders
  the https-disabled variant on every chart change so this cannot ship
  silently again. Downstream consumers pinned to chart 0.5.18 for this
  reason can unpin on the next chart release.
- Non-200 access-log entries are emitted at Info instead of Warn.
  Warn made every error response look like a system degradation to
  log-based alerting. Error entries stay unsampled in the
  middleware and now follow the sampled logger's 1-in-N Info
  sampling like all access-log entries.
- Flaky/failing tests: the shared fasthttptest helper bounds Close
  with a 2s deadline so a stuck fasthttp graceful-shutdown drain
  (a never-idle peer-fetch pipeline client connection) falls
  through to the listener close instead of hanging the whole
  internal/cluster package (a CI run hit the 2-minute package
  timeout); TestDoHedged_NoGoroutineLeak now runs sequentially so
  the process-wide goroutine baseline is not inflated by parallel
  tests spinning up real fasthttp servers and clients.

## [0.5.19] - 2026-09-16

### Added
- Peer-fetch variant-assertion rejections are now counted, not just
  logged. PR #630 added the RFC 9111 §4.1 variant-assertion gate on
  both peer-fetch sides, but operators could not distinguish a rare
  foreign variant from a broken cluster.
  `bouine_peer_fetch_variant_mismatch_total{side="server|consumer"}`
  counts rejections in the peer-fetch handler (server side) and in
  the cache handler (consumer side, wired via an OnPeerVariantMismatch
  callback so the cache package keeps no cluster dependency). The
  metric carries only the side label (§9 cardinality budget, pinned
  by a unit test); a sustained non-zero rate indicates a mixed-version
  fleet or a peer serving wrong-variant content. Runbook alert guidance
  added.

### Fixed
- The origin client now uses a 64 KiB read buffer, matching the
  data-plane and admin servers, instead of fasthttp's 4 KiB default.
  Origin responses whose header block exceeds 4 KiB — product-page's
  `/compare/` responses carry a `Cache-Tag` header with one product
  UUID per variant (~4-5 KiB on phone comparisons) — failed response
  header parse with `ErrSmallBuffer` on every attempt: the
  idempotent retry replayed the same deterministic parse error five
  times and the request surfaced as a 502 (~1 rps on prod-eu,
  exclusively on `/product-page/compare/*`, observed from the
  product-page routing rollout on 2026-09-16).
- Cached static routes (cache.enabled: true) no longer return 502
  "no fast client configured" exactly when the cache cannot answer.
  These routes wire the staticfile handler as Upstream and no
  FastClient, so every fetch path that only consulted the fast client
  failed on cold MISS, no-cache/no-store BYPASS, revalidation, POST
  invalidation, and SSE. The miss fetch, the bypass paths, and SSE now
  fall back to the Upstream handler, replaying the request into a
  scratch fasthttp RequestCtx bounded by fetch_timeout; the scratch
  response is converted to the shared fetchResult shape with an
  exact-size body copy so singleflight followers and storage never
  alias the scratch buffer.
- Four config surfaces that were parsed, validated, and documented
  while having zero effect on behavior now work — or fail loudly
  instead of silently. request/response header_set + header_remove
  rewrite headers on every origin fetch and on every emitted response
  (hit, stale, revalidated, miss, bypass, SSE), covering proxied and
  static routes; health.passive.eject_for restores passively ejected
  targets once the window elapses, with a restore counter label and
  automatic re-ejection for still-broken targets; connect.hedge_timeout
  fires a duplicate idempotent (non-SSE) request after the delay and
  returns the first response, replacing the dead net/http-era
  HedgeClient (deleted with it). In the chart, containerPorts, the
  NetworkPolicy's post-DNAT ports, and the listen-address schema
  patterns now derive from config.listen, so an override keeps
  routing, probes, and policy coherent instead of blackholing traffic.
- The documented header-rewrite contract ("every response this
  handler emits") is now honored on the failure paths: 502s from
  unreachable origins, no-fast-client bypasses, shed 503s, and 304
  conditional responses previously skipped
  response.header_set/header_remove. Every emit site routes through
  the same rewrite path, pinned by a test on the 502 case; the
  origin-ejection runbook documents the new eject_for restore path
  and the origin_restores_total source label.
- Two latent data races reported by -race are closed, each with a
  regression test verified to fail against the pre-fix code.
  Cluster.metrics was assigned by SetMetrics after memberlist.Create
  had already started the gossip dispatch, reconcile, and logging
  goroutines that read it; the field is now an atomic.Pointer[Metrics]
  (the same pattern the package's slogAdapter already used). And
  hot.Get's slow path incremented the stored object's Hits under
  the shard write lock while the warm-sync cycle read the same pointer
  with no lock held; the increment is now an atomic add and the
  encoder reads atomically — the field stays a plain uint64 so JSON
  shape and struct copies are unchanged, and the hit path still
  pays zero allocs. The clone helpers (CloneForReturn,
  CloneForRefresh) read Hits atomically too: both receive store.Get
  pointers, and the revalidation path raced the same increment from
  the other side.
- The peer-fetch queue-wait histogram records the abandoned wait
  when a queued caller cancels — exactly the case the 150ms budget
  produces. The wait was previously observed only after the semaphore
  slot was acquired, so the saturation signal the metric exists to
  surface (prod-eu, 2026-09-12) was invisible whenever the queued
  caller gave up, and TestPeerFetcher_QueueWaitMeasuredWhenSaturated
  flaked when the observe-vs-cancel race lost.
- PurgeEvent.VaryKey is documented as metadata. The field's comment
  claimed a non-empty value scoped the purge to one variant, but every
  receive path (gossip, HTTP peer-purge, batch endpoints) purges the
  primary key plus all locally tracked variants, and a scoped delete
  built from the field would silently no-op — the assertion hex the
  field carries cannot be composed into a variant store key, which
  would leave stale variant bodies serving. Receivers stay RFC 9111
  §4.2.4-conformant (resource-wide invalidation); the decision is
  recorded in ADR-0045, and new unit and integration tests pin the
  purge-all receive path.

### Removed
- cache.key.canonicalize_path, whose listener-level wiring never
  landed: configs setting the knob now fail at parse time with the
  strict loader instead of being silently accepted and ignored.

### Security
- The upstream-replay scratch requests no longer trip CodeQL's
  go/request-forgery model. The three hand-rolled replay sites
  (SetRequestURIBytes on user-derived request URIs) now replay via
  fasthttp's Request.Copy and derive the replayed URI from the parsed
  URI object — the same accessor shape as every other origin-bound
  sink in the package, with the strip-prefix rewrite documented at the
  sink. No behavior change: URI, host, headers, body, and conditional
  headers replay identically, and doFetchBg drops a duplicate
  request-build its caller already performed.

## [0.5.18] - 2026-09-14

### Added
- All duration metrics now expose the native sparse-bucket histogram
  representation alongside their classic `_bucket` series. Six
  remaining classic-only histograms (cloudflare_purge, startup,
  peer_fetch, warm_compaction, wal_write, origin_request_duration)
  join the request_duration_seconds histogram that already had it.
  Native series keep bucket cardinality bounded while resolving
  latencies across the full range — no more 5-second-wide buckets
  hiding multi-second stalls on peer-served fetches. None of the
  converted histograms are on the cache-hit path, so the zero-alloc
  hit-path budget is unchanged (pinned by the existing
  BenchmarkGate_HistogramObserve_Native benchmarks at 0 allocs/op);
  per-package tests pin the native schema on each metric, and the
  runbook documents how to query the series in PromQL.

### Fixed
- After a rolling restart, a pod's cached fasthttp PipelineClient
  for a peer's pre-restart address re-dialed the dead IP every ~3s
  for the life of the process (observed on prod-eu: ~60
  "error in PipelineClient" log lines per minute toward addresses
  dead since the restart). The v0.5.17 breaker could not stop it: it
  gates new Fetch/Put submissions, and none reach the healed ring.
  The Cluster now retires an address as soon as it stops being
  current — pruned from the ring, or the peer restarted at a new
  address: the cached pipeline client is evicted and the stale
  worker's dial parks until the fetcher closes. One parked goroutine
  per retired address replaces the perpetual dial-restart loop, and
  a later request for a returned address transparently gets a fresh
  client.
- Retirement is now lifted only where the address is provably
  current again. A Fetch holding a stale owner PeerInfo captured
  before a ring change could race RetireAddress and re-arm the
  dial-restart loop — resurrecting the zombie the retire mechanism
  exists to park. getPipelineClient no longer clears the retired
  mark; the Cluster lifts it from addPeer via a new PeerFetcher
  UnretireAddress callback (registered alongside SetOnPeerRetired),
  so a stale-owner fetch fails fast and falls back to origin while
  the dead address stays dead.
- Peer fetches were submitted with the caller's context — a
  *fasthttp.RequestCtx carrying no deadline — so hung RPCs fell
  back to the transport's 60s DoTimeout default, and only the
  pipeline client's 500ms ReadTimeout eventually killed them. On
  prod-eu this pinned fetch-semaphore slots behind RPCs slow to fail
  against dead addresses and surfaced as 1-2.5s peer-served "HIT"
  latencies (prod-eu logged ~3600 dial i/o timeouts/hour to pod IPs
  dead since the previous day's restart, each pinning a fetch slot;
  zero successful fetches above 500ms). All peer-fetch and peer-put
  RPCs are now bounded by the 500ms budget regardless of the
  caller's context — a shorter caller deadline is honoured, anything
  looser is capped — and the peer dial timeout drops from 2s to
  200ms: healthy intra-cluster dials complete in single-digit
  milliseconds, so the old budget only ever fired against dead
  addresses.
- The fetch RPC duration histogram started after the fetch-semaphore,
  so fetches queued behind slow-to-fail RPCs were invisible — the
  blind spot that let the peer-stall above go undiagnosed for a day
  (peer-served "HITs" sat in the 1-2.5s request-duration bucket while
  every peer-fetch metric looked healthy). A new
  bouine_peer_fetch_queue_wait_seconds histogram observes the
  semaphore wait, with a runbook note on reading it against the RPC
  histogram.
- The eager ban-scan coalescing window is now anchored at scan
  completion. It was previously recorded at the registration
  timestamp captured before the scan, so the scan's own duration ate
  the window — under CPU contention a slow first scan could shrink
  the window to zero and the next registration paid a full eager
  pass. Anchoring at completion keeps the window whole for slow
  scans in production (large stores under load); exposed by the new
  concurrent-registration hammer test under -race.

## [0.5.17] - 2026-09-11

### Changed
- Every ban registration previously rebuilt the full compiled snapshot:
  a fresh 1024-entry list copy plus the three host/path/surrogate maps —
  about 624 KB of garbage per registration once the list sits at
  banListCap, which is the production steady state (cache-lifecycle
  registers 100+ surrogate-key bans/s over a list saturated at the cap;
  bursts reach 130/s for minutes). With GOGC=200 the heap fills to 3x
  live before GC, so these rebuild bursts showed up as periodic
  working-set spikes to 13-16 GB on every prod-eu pod during ban storms
  (measured: heap_alloc 6.5 → 13.4 GB in ~4 min at the 08:05 UTC
  storm, heap objects flat — large transient buffers, not object
  growth). Registration now only marks the snapshot dirty and mutates
  the list in place — re-issued patterns refresh the existing entry,
  expired bans drop, and the cap eviction shifts in place during one
  scan — and the compile happens on the next snapshot() read, so a
  batch of N registrations costs one O(list) compile instead of N.
  Enforcement is unchanged: the first lookup after registration reads
  a fresh snapshot and sees the new ban (no staleness window).
  Measured (darwin/arm64, list saturated at banListCap=1024): register
  (refresh) 95 µs / 624 KB / 32 allocs → 6.8 µs / 128 B / 1 alloc;
  register (evict+append) 88 µs / 624 KB → 6.9 µs / 128 B. The hit
  path is untouched: BenchmarkGate_HotStore_Get_Hit_Bans stays at
  0 allocs/op, and new bench gates pin the batching itself.

### Fixed
- Since the v0.5.16 rolling restart, a pod could keep routing
  peer-fetch RPCs to a peer address that died with the restart: every
  fetch paid the full RPC timeout while fasthttp's pipeline worker
  hot-restarted the dial (2s dial timeout + 1s throttle), logging
  "error in PipelineClient" roughly every 3 seconds for 12+ hours.
  Two complementary fixes: a background reconcile pass (default every
  30s, `ReconcileInterval`) now re-derives the peer set from
  memberlist's live member view — entries absent from the live set are
  pruned even when no push/pull merge arrives, and live members whose
  recorded address no longer matches their memberlist metadata are
  refreshed in place — healing both a missed NotifyLeave and a missed
  NotifyUpdate after a peer restart; and a per-address failure breaker
  in the peer fetcher blacklists an address after three consecutive
  transport failures for a 30s cooldown, so Fetch and Put return
  immediately and the handler falls back to origin instead of
  stalling on a dead address. The cooldown expiry allows one re-probe
  and a retained failure count re-trips the breaker; a successful
  round trip (including a 404 miss) resets the count, and canceled
  caller contexts never count as peer failures. A new
  `bouine_peer_addr_blacklisted` gauge surfaces tripped addresses.

## [0.5.16] - 2026-09-10

### Changed
- Pure surrogate-key bans previously walked every shard under a write
  lock to find the few tagged entries — ~550 µs per ban at 50K entries,
  measured — work entirely redundant with the O(1) lazy check (the ban
  snapshot's surrogates set, landed in 0.5.15) that evicts matching
  entries on lookup and with the TTL reaper that collects entries never
  accessed again. Surrogate-only bans now skip the eager scan entirely:
  enforcement is identical (a banned entry is never served) and memory
  reclaim moves from invalidation time to the reaper's pass (bounded by
  entry TTL + SWR + SIE). Host/path bans and multi-condition bans
  carrying a surrogate key plus patterns keep the coalesced scan.
  `Ban` now returns 0 for surrogate-only bans instead of the
  eager-match count. Measured (darwin/arm64, 50K entries): surrogate
  ban ~550 µs → ~300 ns (~1800x faster), 8 allocs; hit path, put path,
  and alloc budgets unchanged. This targets the production invalidation
  workload — 100% surrogate-key bans, ~110K distinct bans/day bursting
  to ~18/s during storms over ~500K hot entries — where each scan
  held shard write locks for an O(entries) pass, the suspected driver
  of storm-window HIT p99 spikes (245-323 ms vs 130 ms average).

## [0.5.15] - 2026-09-10

### Changed
- With many active bans, every cache hit previously walked the lazy
  ban list and evaluated each ban predicate against the object's host,
  path, and surrogate keys — 3.8 µs at 256 bans and 10.6 µs at the
  1024-entry cap, measured, taxing hot and warm hits alike for the
  full 24 h banTTL even after traffic stopped. Ban registration now
  compiles the list into an immutable snapshot: literal hosts, paths,
  and surrogate keys become set lookups carrying the per-ban exemption
  time (RFC 9111 §4.4), anchored prefixes (^api\., ^/blog/) become
  HasPrefix checks with the full predicate applied only on a prefix
  hit, and genuinely regex or multi-condition bans keep the linear
  predicate walk. The rejection is sound: a miss in every set rules
  out all literal bans, and multi-condition bans never join the sets
  because a host hit alone would be a false positive. Measured:
  hit at 256 bans 3.8 µs → 21 ns (-99.45%), hit at 1024 bans
  10.6 µs → 22 ns (-99.79%), flat in ban count; opaque regex bans
  unchanged by design; zero allocations on all paths. At 30k RPS with
  a full ban list this removes ~0.3 core of pure ban-walk CPU.
- The TTL reaper now prunes expired lazy bans each tick (30 s default)
  and rebuilds the ban snapshot, so a quiet period after a ban storm
  stops taxing hits within one reaper interval instead of the full
  24 h banTTL.

### Fixed
- Admin-API invalidations (purge/ban/refresh) are recorded in the
  ops history ring so operators can audit recent invalidation
  activity from the dashboard.
- The dashboard no longer claims h2c or HTTP/3 support on the data
  plane (HTTP/1.1 only, ADR-0034).
- Ban feedback on the dashboard no longer implies a coalesced scan
  was a no-op.
- The insights page polls every 15 seconds and surfaces origin
  fetch shedding as a high-severity insight.
- Avg peer-fetch latency is wired and cumulative counters are
  relabeled correctly on the cluster page.
- Cloudflare status fields are wired into the dashboard CF card.
- The routes TTL column shows ttl_override, the config viewer shows
  recently added knobs, and the cluster page shows the effective hop
  limit default.
- The 24H range tab that displayed only 6h of data was dropped.

## [0.5.14] - 2026-09-09

### Changed
- Invalidation storms no longer fan out one HTTP POST per event per
  peer. Purge and refresh events now coalesce into count-prefixed
  batch frames (new msgTypes 4/5) flushed by a bounded batcher on 256
  events, a 10 ms interval, or Close. Events arriving on an idle
  queue still flush synchronously, preserving the purge API's
  fan-out-before-return guarantee. Receivers dedup by per-issuer
  monotonic Seq, collapsing the double delivery and post-partition
  replays. A 1000-key purge burst in a 3-peer cluster now produces a
  handful of batched POSTs instead of 3000, with gossip frames
  reduced ~256x per batch; apply-side work is halved under storms.
  Ban events stay unbatched (rare, immediacy dominates). See ADR-0044
  for the latency trade-offs and fallback semantics.
- The `/v1/purge/batch` endpoint no longer purges each URL
  independently: one store delete plus one full cluster broadcast and
  one Cloudflare propagation per URL was replaced by a single local
  purge pass, one batched fan-out via the broadcaster's batch frame
  (ADR-0044), and per-URL Cloudflare propagation for successfully
  purged entries only. A 1000-URL batch previously fired 1000
  broadcasts (3000 peer POSTs in a 3-node cluster); now it produces a
  single batched fan-out.
- Ban scans now coalesce across concurrent callers and dedup
  identical bans, reducing redundant cache walks and duplicate ban
  entries when multiple invalidations target overlapping key ranges
  simultaneously.

## [0.5.13] - 2026-09-09

### Fixed
- Concurrent cache-miss callers could crash a pod with "index out of
  range [1] with length 1": collapsed fetches share one fetch result
  across all singleflight callers, but that result kept a live pointer
  into a pooled fasthttp response that every caller released. Multiple
  releases of the same pooled response corrupted fasthttp's response
  pool, so two requests could hold the same response at once, one
  parsing origin headers into it while the other iterated its headers.
  The fetch now detaches headers into an owned map and releases the
  pooled response exactly once; each singleflight caller also gets a
  private header-map clone, closing a related latent race where
  concurrent callers mutated one shared header map.
- An HPA scale-down could leave a dead peer permanently stuck in the
  consistent-hash ring: push/pull resurrected the node during the
  convergence window, so every surviving peer ended up with the same
  stale ring, whose digest matched and therefore skipped the pruning
  path. The dead peer kept receiving peer-fetch RPCs (dial timeouts)
  until a rolling restart broke digest symmetry. The prune now runs
  unconditionally (only the add-missing-peers path is gated on the
  digest comparison), and re-adding a known peer removes its existing
  virtual nodes first, so resurrecting a peer no longer accumulates
  duplicate ring entries across scale-up/down cycles.

### Added
- 2.5 s, 5 s, and 10 s tail buckets on the `request_duration_seconds`
  and `peer_fetch_duration_seconds` Prometheus histograms (and the
  matching in-process latency bounds): both previously capped at 1 s,
  collapsing all slow misses and hung fetches into a single +Inf
  bucket, making a 1.01 s miss indistinguishable from a 30 s miss via
  PromQL. Series count per tuple grows 13 → 16 and stays well under
  the cardinality budget.
- The admin API now emits an OpenTelemetry server span for
  invalidation calls (`POST /v1/ban`, `/v1/refresh`): the admin server
  previously ran uninstrumented, so a distributed trace initiated by
  cache-lifecycle ended at the caller's client span. The admin handler
  chain joins the caller's trace via the propagated W3C traceparent
  (`bouine.admin` span), and a test-only tracing helper lets other
  packages' tests assert on exported spans (PR #653, 06eb7be).

### Dependencies
- fasthttp v1.73.0 → v1.74.0 (replaces the indirect Brotli dependency
  with a pure-Go RFC 7932 implementation), golang.org/x/sync →
  v0.23.0, golang.org/x/sys → v0.48.0, and the genproto api/rpc pins
  to their 2026-09-08 revisions. All gate benchmarks stay within
  their allocs/op budgets.

## [0.5.12] - 2026-09-08

### Fixed
- Peer fetch could serve the Vary resolver body as a peer HIT: the
  follow-up to the cross-variant fix (#630) left a second hole in strong
  cluster mode. A non-owner that misses locally peer-fetches the key
  owner for the PRIMARY key with a blank variant assertion (it cannot
  know the Vary list yet); the owner's only stored entry is the
  primary-key Vary resolver, whose body belongs to whichever variant
  filled first. Both of the prior gates skip in this flow: the
  requester's assertion is blank and the storage codec never serialized
  `VaryValue` (always empty over the wire), so `servePeerHit`'s
  recompute could not run. Observed in preprod on the doorman
  `/content/` route: an `it-IT` request filled from origin, then an
  `fr-FR` request on another pod got `Content-Language: it-IT` as a
  peer HIT. Two complementary fixes: the owner never serves a Vary
  resolver body from a peer fetch (blank `VaryKey`, non-empty
  `VaryValue` is answered with a miss), and the storage codec now
  serializes `VaryValue` (version 4) so the cross-variant recompute
  gate works on peer-delivered objects; v3 blobs decode unchanged
  (warm-tier entries survive the rolling upgrade). New 3-node
  integration test sweeps fill-node × request-node crossings and fails
  on main (PR #641, fa0c1e7).

## [0.5.11] - 2026-09-08

### Fixed
- Peer fetch could serve another variant's cached body as a HIT: in
  strong cluster mode a non-owner that misses locally peer-fetches the
  key's owner, which could return the primary-key Vary resolver (the
  first fill's body) for a request selecting a different variant — the
  handler re-checked only freshness, never the Vary dimension. Observed
  in production on the doorman route, where per-market content differs
  only in request headers. Two complementary gates close the hole: the
  requesting node stamps its variant assertion on the peer-fetch RPC
  (the previously ignored `VaryKey` field) and, on receipt, recomputes
  the variant dimension for the actual request, rejecting a foreign
  body as a miss (origin fallback); the peer honors the assertion,
  missing with 404 when the only stored entry under the key is another
  variant's or the resolver's, and the resolver entry is stored with a
  blank `VaryKey` so protocol-strict peers classify it correctly.
  Regression tests replay the cross-market incident end to end and pin
  the protocol semantics in both directions; miss-path alloc budget
  unchanged (PR #630, e98b7da).

### Added
- `cluster.peer_fetch_concurrency` (default 4, capped at 128): bounds
  the peer-fetch/peer-put semaphore, previously hardcoded to 4. In
  strong-mode clusters most cache hits are peer hits (non-owner pods
  fetch from the key owner), so the semaphore sits on the hot path and
  its fixed value queued requests behind in-flight fetches, adding tail
  latency under load; production could not raise it without a code
  change. Config-loader validation rejects negative and >128 values;
  the knob and its relationship to `peer_max_conns_per_host` are
  documented in ADR-0039 (PR #627).
- Per-route origin timeout: `routes[].cache.fetch_timeout` is now the
  authoritative origin-wait bound and can exceed the pool-wide
  `connect.response_header_timeout`. Previously the origin client baked
  `response_header_timeout` into its `ReadTimeout`, and fasthttp
  composes the effective read deadline as `min(per-request deadline,
  client.ReadTimeout)` — silently capping every route at the pool knob
  (default 30s), so a slow endpoint could never be given more time
  without raising the wait for every other route on the pool. Routes
  without an explicit `fetch_timeout` now inherit
  `connect.response_header_timeout` (same effective default as
  before); the pool knob is also validated to stay below the 5-minute
  data-plane safety net, mirroring `fetch_timeout` (ADR-0043, PR #624).

## [0.5.10] - 2026-09-08

### Fixed
- Vary variants were keyed on the first `Vary` field line only (fasthttp
  `Map.Get`), so an origin sending `Vary: Accept-Encoding,Accept-Language`
  plus `Vary: BM-Market` produced a variant key that dropped `BM-Market` —
  a request differing only in that header was served the wrong market's
  cached body. RFC 9110 §5.2 makes Vary a list-based field: multi-line
  values are equivalent to one comma-joined value. The joined read (GetAll)
  is now used everywhere a Vary value is observed (store keys, star
  detection already joined, 304 recompute); `MergeHeaders304` replaces
  `Vary` wholesale instead of writing per-line entries that corrupted
  stored multi-line values. Miss-path alloc budget unchanged; regression
  tests cover variant isolation and 304 merge (PR #628, ca4aec4).

## [0.5.9] - 2026-09-08

### Added
- `listen.read_timeout` config option (default 30s): bounds how long
  reading a single request's header and body may take on data-plane
  connections. Previously hard-coded. It is the slowloris defense —
  raise it for slow mobile clients or large uploads. Validated to stay
  below the 5-minute data-plane safety-net WriteTimeout, and exposed in
  the Helm chart values (`config.listen.read_timeout`).

## [0.5.8] - 2026-09-04

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
- `bouine_request_duration_seconds` minted ~9,600 idle series per
  configured route at startup and attributed every request to
  `_default`, because the router published the route as a request
  header the metrics middleware never read (Grafana Cloud showed the
  metric at ~20% of total scrape cardinality, bouine#607). Attribution
  now rides process-local fasthttp UserValues and reports the serving
  route's upstream pool — a small config-bounded set that still answers
  "which upstream is slow or 5xx-ing" — with pool-less and unmatched
  traffic landing on `_default` (a static-file route name can no longer
  leak into the `upstream_pool` label). The duration histogram drops
  the `source` axis (correlated with `cache_result`; no consumer
  queries it) and the 2.5/5/10s tail buckets (a cache's latency mass
  sits far below 1s; hung-fetch tails are already 5xx counts on
  `bouine_requests_total`), shrinking the histogram footprint ~90%.
  The dashboard rings keep per-route attribution, so the routes panel
  loses nothing.
- Singleflight followers of unbuffered streams (non-hinted SSE and
  Vary variant-overflow responses) got a 200 with an empty body: the
  leader streamed unbuffered but still published a body-less result,
  so followers waited out the leader's entire stream (up to
  `fetch_timeout`) for nothing — silent data loss for concurrent
  clients of the same URL. The leader now releases followers at
  header time with `ErrStreamUnshareable` and each follower fetches
  its own response outside singleflight (ADR-0042).

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
- Server-Sent Events now stream end to end. Requests announcing
  `Accept: text/event-stream` (the WHATWG client contract) are served
  as live unbuffered streams — never cached, never singleflight-
  collapsed, fetch slot released at header time, each event flushed
  as it arrives — including POST-based SSE (the dominant AI API
  shape), whose invalidation semantics are preserved (purge at header
  time on 2xx/3xx). A dedicated origin-pool client converts the
  absolute read deadline into per-read idle semantics (10-minute
  budget) and the H1 fall-through write path re-arms its write
  deadline per Write, so live streams survive indefinitely while
  dead peers and stalled clients are still cut. Non-hinted SSE
  responses stream unbuffered too but stay bounded by `fetch_timeout`
  (ADR-0042). Also fixes two latent bugs the work surfaced:
  `doFetchStream` dropped request bodies entirely, and `streamBypass`
  delivered streamed bodies in 4 KiB batches.
- The request-duration histogram is exposed natively (client_golang
  dual representation): classic `_bucket` series stay for
  layout-agnostic consumers, while the sparse-bucket native form lets
  Grafana Cloud / Mimir `histogram_quantile` work without
  materializing bucket series server-side. Resolution factor 1.1,
  capped at 80 sparse buckets with a 1h minimum reset window
  (adversarially probed: 10k distinct latencies hold the spread at 44
  buckets). The runbook documents the `metric_relabel_configs` recipe
  that drops the classic bucket series from scrapes and the one-field
  rollback. Zero-alloc gates `BenchmarkGate_HistogramObserve_Native`
  and `..._Distinct` pin 0 allocs/op; enabling the feature exposed a
  pre-existing `strconv.Itoa` per hit in `RecordHit`, now replaced by
  the existing statusStrings table.

### Changed
- The `method` label is gone from the data-plane RED metrics: no
  dashboard or SLO query used it, response bytes and status already
  answer everything consumers plot, and dropping the axis shrinks the
  pre-resolved slot table (the reactor metrics ring record is now
  entirely stable handler-owned strings). The access log keeps the
  method: logs are per-request, not label spaces.
- Go toolchain bumped from 1.27.0 to 1.27.1 (go.mod, CI workflow
  envs, digest-pinned `golang:1.27.1-bookworm` build image, prek
  GO_VERSION_STAMP refreshed) and all direct and indirect dependencies
  rolled forward.

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

[Unreleased]: https://github.com/bouine-cache/bouine/compare/v0.5.21...HEAD
[0.5.21]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.21
[0.5.20]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.20
[0.5.19]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.19
[0.5.18]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.18
[0.5.17]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.17
[0.5.16]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.16
[0.5.15]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.15
[0.5.14]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.14
[0.5.13]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.13
[0.5.12]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.12
[0.5.11]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.11
[0.5.10]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.10
[0.5.9]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.9
[0.5.8]: https://github.com/bouine-cache/bouine/releases/tag/v0.5.8
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
