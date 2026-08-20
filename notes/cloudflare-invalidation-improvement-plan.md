# Plan: Improve Cloudflare Invalidation Propagation

## Context

There are two separate Cloudflare propagation paths:

1. **bouine-cache** (`cfPropagator` in `cmd/bouine/cmd/cloudflare.go`): Inline
   propagation from bouine admin API (purge/ban/refresh) → CF. Async by
   default, retry with jitter, rate-limit handling, metrics, and a
   `GET /v1/cloudflare/status` endpoint.

2. **cache-lifecycle** (`purge.Service` in `pkg/purge/service.go`): Event-driven
   — consumes cache-tag-expired events, batches per market, purges bouine
   first (best-effort), then CF by tags. Per-market zone IDs, rate limiting,
   batching.

**bouine will ultimately replace cache-lifecycle.** As a first step, bouine's
`cfPropagator` becomes the single component that calls the Cloudflare API, and
cache-lifecycle is migrated to call bouine's `cfPropagator` instead of its own
CF client.

## Pain Points Identified

- bouine fires one CF API call per purge/ban/refresh — hitting CF rate limits
  under burst traffic.
- Failed CF purges (after retries exhausted) are silently lost — no retry
  queue or DLQ.
- No circuit breaker — during CF outages, every invalidation hammers the API.
- Compound bans (host AND path) are skipped entirely — CF cache stays stale.
- bouine uses a single zone ID (no per-market support).
- Duplicated CF client logic across bouine-cache and cache-lifecycle.
- Skipped propagations are counted but not comprehensively observable.

## Improvement Plan

### A. Batching + Deduplication (highest priority)

**Goal:** Reduce CF API call volume to stay within rate limits.

- Coalesce pending purges into batched CF API calls.
  - `PurgeSingleFile` supports up to 30 URLs per call.
  - Tag/prefix/host purges are inherently batchable.
- Deduplicate identical URLs/tags/prefixes/hosts before sending.
- Time-windowed batching: flush every N ms or when batch is full.
- Dedup also applies to retry storms — a URL that failed and is re-requested
  during the retry window is merged, not duplicated.

**Files affected:**
- `cmd/bouine/cmd/cloudflare.go` (cfPropagator)
- `internal/cloudflare/client.go` (Client)
- New: `cmd/bouine/cmd/cloudflare_batcher.go` (batcher logic)
- `internal/config/config.go` (batch config)
- `deploy/helm/bouine/values.yaml` + `values.schema.json` (batch config)

### B. Multi-API-Key Rate Limit Spreading

**Goal:** Multiply effective rate limit budget by using multiple CF API tokens.

- Support multiple CF API tokens in config (list).
- Rotate across requests to spread the rate limit budget across tokens.
- Each token has its own rate limit quota; round-robin or LRU selection.
- On 429 from one token, mark it as rate-limited and rotate to the next.

**Files affected:**
- `internal/cloudflare/client.go` (Client — multi-token support)
- `internal/config/config.go` (multi-token config)
- New: `internal/cloudflare/token_pool.go` (token rotation logic)
- `cmd/bouine/cmd/engine.go` (initCloudflare — pass multiple tokens)
- `deploy/helm/bouine/values.yaml` + `values.schema.json`

### C. Circuit Breaker

**Goal:** Fail-fast during CF outages to reduce API pressure and resource waste.

- Open after N consecutive failures.
- Fail-fast for a cooldown period (e.g. 30s).
- Half-open probe: periodically try a single request to check recovery.
- Closed: normal operation.

**Files affected:**
- New: `internal/cloudflare/circuit_breaker.go`
- `cmd/bouine/cmd/cloudflare.go` (integrate circuit breaker into dispatch)
- `internal/observability/dataplane.go` (circuit state metrics)

### D. Persistent Retry Queue (DLQ) — designed to not increase CF load

**Goal:** Survive transient CF outages without losing invalidations, without
increasing CF API request volume.

- Failed purges go into a bounded in-memory queue (or optional persistent
  store).
- Retry loop deduplicates against the queue before re-sending.
- Exponential backoff with jitter, capped retry count.
- Queue depth is observable (metric + admin status).
- DLQ retries respect the batcher — they go through the same batching/dedup
  pipeline as new purges.

**Files affected:**
- New: `internal/cloudflare/retry_queue.go`
- `cmd/bouine/cmd/cloudflare.go` (enqueue on failure, background retry loop)
- `internal/config/config.go` (DLQ config: max queue size, max retries)
- `internal/admin/server.go` (queue depth in status endpoint)
- `internal/observability/dataplane.go` (DLQ metrics)

### E. Over-Purge for Compound Bans

**Goal:** Ensure CF cache is invalidated for compound bans (host AND path).

- `PropagateForBan` with both `PathRegex` + `HostRegex` → issue
  `PurgeByPrefixes` AND `PurgeByHostnames` (broader invalidation, OR
  semantics).
- This over-purges (purges all paths matching the prefix on all hosts, AND
  all hosts matching the hostname on all paths) but is safer for consistency
  than skipping.

**Files affected:**
- `cmd/bouine/cmd/cloudflare.go` (PropagateForBan — change compound ban
  handling)
- `internal/cloudflare/mapper.go` (add MapCompoundBan function)

### F. Comprehensive Failure Observability

**Goal:** Every failure scenario gets a distinct metric label and structured
log.

Failure scenarios to instrument:
- `skipped_compound_ban` — compound ban (being changed to over-purge in E)
- `skipped_path_metachar` — path regex contains metacharacters
- `skipped_path_suffix` — path regex is a suffix anchor
- `skipped_host_metachar` — host regex contains metacharacters
- `skipped_host_anchor` — host regex contains anchors
- `skipped_empty` — empty input
- `rate_limited` — after retries exhausted
- `circuit_open` — fail-fast during outage (from C)
- `dlq_enqueued` — purge enqueued in retry queue (from D)
- `dlq_dropped` — retry queue full, purge dropped (from D)
- `dlq_retried` — purge retried from DLQ (from D)
- `dlq_expired` — purge expired from DLQ after max retries (from D)
- `network_error` — network-level failure
- `server_error` — 5xx from CF
- `client_error` — 4xx from CF (non-rate-limit)
- `token_rotated` — API token rotated due to rate limit (from B)
- `batch_flushed` — batch flushed to CF
- `batch_deduped` — duplicate purges deduplicated in batch

**Files affected:**
- `internal/observability/dataplane.go` (new metrics)
- `cmd/bouine/cmd/cloudflare.go` (emit metrics for each scenario)
- `internal/admin/server.go` (expand status endpoint)

### G. Migration Path for cache-lifecycle

**Goal:** cache-lifecycle calls bouine's cfPropagator instead of its own CF
client.

- bouine exposes an HTTP endpoint that cache-lifecycle calls instead of
  the CF API directly.
  - Endpoint: `POST /v1/cloudflare/propagate` — accepts tags, URLs, prefixes,
    hosts.
  - bouine's cfPropagator handles batching, retry, circuit breaker, etc.
- cache-lifecycle's `purge.Service` replaces `cfClient.PurgeByTags` with a
  call to bouine's propagate API.
- cache-lifecycle's CF client, retry, and error code can then be deprecated.

**Files affected:**
- bouine: `internal/admin/server.go` (new endpoint), `api/openapi.yaml`
- cache-lifecycle: `pkg/cloudflare/client.go` (replace with bouine client),
  `pkg/purge/service.go` (call bouine instead of CF)

## Execution Order

1. **Batching + Deduplication** (A)
2. **Multi-API-Key Rotation** (B)
3. **Circuit Breaker** (C)
4. **Persistent Retry Queue / DLQ** (D)
5. **Over-Purge for Compound Bans** (E)
6. **Comprehensive Failure Observability** (F)
7. **cache-lifecycle Migration** (G)

## Quality Gates

- After each improvement: run Linus review 3 times, adjust codebase based on
  feedback each time.
- Test coverage must be above 85%.
- Open a single PR with all improvements stacked.
