# PLAN.md — Active Roadmap

Active tasks and backlog for bouine. Phases 0–6 (incl. 3.5, 4.5) are
**complete and tagged**. Architecture reference lives in
[`docs/architecture.md`](docs/architecture.md).

---

## Pending Work

### Phase 7 — Simplification (2 tasks remain)

All other §7.x items are done. Two remain:

#### Extract `runner` from `cmd/bouine/cmd/engine.go`

`engine.go` is 548 LOC (target ≤ 300). `builder.go` already extracts
`buildPools`, `buildRouter`, `buildStore`, `buildCluster`. The lifecycle
methods (`run`, `startListeners`, `startAdmin`, `startHealthChecks`) have not
been separated. Move them into a `runner` type in
`cmd/bouine/cmd/runner.go`. `engine.go` becomes a thin wiring layer.

#### Refresh benchmark baseline

After the engine.go reduction, re-run `make bench` and overwrite
`bench/results/baseline.txt` so `make benchstat` diffs are meaningful.

**Exit criteria:**
- `cmd/bouine/cmd/engine.go` ≤ 300 LOC.
- `bench/results/baseline.txt` updated; no regression on `make benchstat`.
- `golangci-lint unused` and `staticcheck U1000` zero findings.
- `make bench` gates pass; `make conformance` score unchanged.

---

### Phase 6.5 — Dashboard UX gaps

Reference: `docs/assets/dashboard-reference.html`.

Effort: **S** < 30 min · **M** 1–3 h. Data: ✅ available · ⚠️ partial wiring needed.

#### Global / Layout

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-L1 | Sidebar footer: add live `peers N/N`, `hit X%`, `req/s N` — currently shows only node name | S | ✅ |
| G-L2 | Header pill: render "N peers healthy / stale peers" dynamically instead of static `● dashboard` | S | ✅ |
| G-L3 | Move time-range selector (1H / 6H / 24H) into the tabs bar on every page, not just Overview | S | ✅ |
| G-L4 | Light-mode active nav: add `box-shadow: inset 2px 0 0 var(--a)` (only dark-mode variant exists) | S | ✅ |
| G-L5 | Sort icons: replace static `↕` text with `th.sortable` + `asc`/`desc` CSS classes | M | ✅ |
| G-L6 | Add `.br` CSS class (red/danger button) for destructive actions | S | ✅ |
| G-L7 | Tier bar label row: add `.tier-bar-wrap` + `.tier-bar-label` flex row above `.tier-bar` | S | ✅ |
| G-L8 | Move theme-toggle to `position:fixed; bottom:1rem; right:1rem` | S | ✅ |

#### Overview page

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-O2 | Route table: add Pool and TTL columns; join `config.Route` with `RouteStat` | M | ⚠️ |
| G-O3 | Hot & Warm store tile: add warm tier data + evictions/min | M | ⚠️ |
| G-O4 | Replace Quick Purge tile 3 with compact circular SVG ring (120×120); move Quick Purge to Invalidation | M | ✅ |
| G-O5 | Peer rows: render `DataAddr`, `AdminAddr`, `JoinedAt` — currently only name + live/stale | S | ✅ |
| G-O6 | Chart: change second dataset from "hits" to "errors" | S | ✅ |

#### Routes page

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-R1 | Subtitle: show configured route count `N configured routes · polled every 10s` | S | ⚠️ |
| G-R2 | Add config columns (Pool, TTL, SWR, SIE, neg_ttl, stayin_alive, jitter) | M | ⚠️ |
| G-R3 | Add two bar charts: "hit ratio by route (6h)" and "req/min by route" | S | ✅ |
| G-R4 | Search bar placeholder: `Filter by prefix or pool…` | S | ✅ |

#### Cluster page

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-C2 | Peer table: replace columns with `Node \| Data addr \| Admin addr \| Weight \| Joined \| Status` | S | ✅ |
| G-C3 | Ring stats box: virtual nodes/real, load factor, hop limit, peer fetch timeout, protocol version | S | ⚠️ |
| G-C4 | Replace horizontal band ring SVG with circular donut (220×220, `stroke-dasharray` arcs) | M | ✅ |
| G-C5 | Peer fetch stats box: peer hits/misses (6h), avg hop count, fan-out timeout | S | ⚠️ |

#### Invalidation page

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-I1 | Move Quick Purge form here from Overview tile 3 | S | ✅ |
| G-I2 | Ban form: add `header_regex` field alongside host/path | S | ✅ |
| G-I3 | Recent invalidation history (last 10 ops, in-memory audit ring) | M | ⚠️ |

#### Config page

| ID | Description | Effort | Data |
|----|-------------|--------|------|
| G-CF1 | Show current effective config as syntax-highlighted YAML (read-only) | M | ✅ |
| G-CF2 | Reload: show diff of changed keys after successful reload | M | ⚠️ |

---

### Phase 8 — AI traffic analysis (post-v1.0)

Streaming analytics layer and ML-assisted suggestion engine in a separate
goroutine pool with a strict CPU budget.

- Ingest sampled access logs into embedded DuckDB / Parquet for ad-hoc
  queries.
- Feature extraction: hit/miss patterns per route, TTL utilisation, top
  cacheable-but-uncached URLs, Vary blow-up, Cookie pollution.
- ML layer (initially heuristics, then a small ONNX-served model) suggests
  TTL changes, Vary pruning, candidate prefetch URLs, ban predicate strategies.
- "Suggestions" inbox in the dashboard with one-click apply.
- All AI features **opt-in** and off by default.
- **Exit:** ≥ 5 actionable suggestion types; no measurable impact on
  data-plane p99.

---

### Phase 9 — Production load testing & competitive benchmarking

Scripts live under `bench/loadtest/`. Targets: bouine HEAD, NGINX 1.27.x,
Varnish 7.6.x, Envoy 1.32.x — all on the same hardware with the same
upstream.

**Single-node scenarios:**
1. Throughput ramp — 90% hits / 10% misses, 1k → 100k RPS.
2. Cache-hit-only baseline — 100% hits to 10k pre-warmed keys at 50k RPS.
3. Cache-miss storm — 100% unique / no-store at 10k RPS.
4. Working-set overflow — 64 MiB cache vs 3.2 GiB working set.
5. Mixed realistic — 60% hit / 15% miss / 10% stale / 5% revalidate /
   5% bypass / 3% vary / 2% error at 10k RPS for 300s.

**Multi-node scenarios:**
1. Horizontal scaling — 1/2/3/5/10 nodes, fixed per-node load.
2. Gossip convergence — 5-node cluster, kill + restart one node under load.
3. Rolling update — 3-node StatefulSet under 10k RPS.

**Stress scenarios:**
1. Connection exhaustion — 1k → 50k concurrent connections, 1 req/conn.
2. Large body streaming — 10 MB bodies at 1k RPS.
3. Vary blow-up — 1 URL × 1000 distinct `Vary: X-Test` values.

**Exit criteria:**
- Results in `bench/loadtest/results/` with charts and percentile tables.
- bouine canonical RPS ≥ Varnish across the full competitive matrix.
- All multi-node scenarios pass end-to-end.

---

## Backlog (post-v1.0)

Features deliberately deferred. Each graduates to the roadmap above when
it gets a phase entry, a threat-model row, and an ADR under
`docs/decisions/`.

1. **Data-plane auth & authorization** — JWT, basic-auth, OAuth introspection.
   v1.1 candidate.
2. **Per-route request-rate limiting** — connection caps exist; per-route
   token bucket deferred. v1.1 candidate.
3. **Built-in ACME / autocert** — cert-manager covers K8s; integrated client
   may ship as opt-in module. v1.1 candidate.
4. **Multi-tenant routing scopes** — per-tenant storage quota, admin tokens,
   metrics namespacing. Operators run one deployment per tenant today.
5. **Forward-proxy mode** — reverse cache only in v1.0.
6. **gRPC caching** — passthrough only.
7. **WebSocket caching** — never; passthrough only.
8. **HTTP/3** — deferred until demand materializes; `quic-go` removed.
9. **Pluggable storage backends** — RAM + mmap embedded only; plugin
   interface (NVMe-direct, Optane, object storage) is a future candidate.
10. **Backup / restore of ban list** — v1.1 candidate.
11. **Emergency stale-mode** — serve stale beyond `stale-if-error` for
    allow-listed routes during origin outages. v1.1 candidate.
12. **VCL shim** — parser + lowering pass for VCL 4.1 subset into native
    config tree. `/internal/vcl` reserved. v1.1+ when migration demand
    justifies it.
13. **Dashboard multi-tenancy** — single-tenant operator-facing in v1.0/v1.1.
14. **Federated multi-cluster** — cross-cluster federation / regional tiering.
15. **ESI-lite** — `<esi:include>` support; most architectures prefer CDN-
    layer or client-side composition. v1.1+ if demand materializes.
16. **Surrogate keys** — `Surrogate-Key` header indexed for ban. Operators
    use host/path/method predicate bans today. v1.1 candidate.
17. **Multi-layer cache (cache-of-caches)** — hierarchical invalidation,
    parent-fetch protocol, loop detection via `X-Bouine-Tier`. v1.2+.
18. **Always-warm (pre-warm on eviction)** — background re-fetch from origin
    when an object is evicted. Per-route opt-in `cache.always_warm`. v1.1+.

---

## Definition of Done (v1.0)

- [ ] Phase 7 complete — `engine.go` ≤ 300 LOC, `golangci-lint unused` /
      `staticcheck U1000` zero findings.
- [x] Hit-path allocs/op = 0 on the canonical benchmark.
- [x] cache-tests score ≥ Varnish (current: 93.2 %).
- [x] Canonical benchmark within 10 % of Varnish RPS, CI-gated.
- [x] No orphaned / unwired production packages.
- [x] Threat model zero unaddressed `Txx` without a §18 deferral.
- [x] Helm chart published.
- [x] Migration guides (NGINX, Varnish) in `docs/migration/`.
- [ ] `bench/results/baseline.txt` refreshed after phase 7.
- [ ] Phase 6.5 UX gaps closed (or formally deferred to v1.1).
