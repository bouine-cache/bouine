# Metric Cardinality Reduction Plan

> One-line summary: cut `bouine_request_duration_seconds` from ~20 % of total
> Grafana Cloud metric consumption to <1 % by fixing route attribution,
> dropping labels no dashboard reads, creating series lazily, and only then
> (optionally) switching the hot-path histogram to a native histogram.

## Motivation

Grafana Cloud cardinality management shows `bouine.*` at ~20 % of total
metric consumption, with `bouine_request_duration_seconds` alone at 20 % of
total cardinality (source: cardinality-management dashboard, 24h window).
The metric is a classic `prometheus.HistogramVec` with five labels
(`method`, `status`, `cache_result`, `source`, `route`) and 13 explicit
buckets. Each active label tuple costs **16 series**: 13 `_bucket` + the
implicit `+Inf` bucket + `_sum` + `_count`.

Four compounding causes:

1. **Eager pre-resolution creates series nobody reads.**
   `PreResolveRoutes` (`internal/observability/dataplane.go:407`) calls
   `WithLabelValues` for every 3×8×5×5 = 600 tuple per configured route at
   startup, via `buildRouteMetrics` (`dataplane.go:427`). That is
   600 × 16 = **9,600 series per route before a single request arrives**.
   Grafana Cloud bills active series, not samples.
2. **Route attribution is probably broken.** The router sets a request
   header (`internal/server/router.go:131`) but the middleware reads a
   UserValue (`internal/observability/dataplane.go:762`); no production
   code path sets that UserValue (the only occurrence is a test,
   `dataplane_source_test.go:119`). All traffic is therefore attributed to
   `_default` today. Consequence: the moment someone "fixes" the wiring,
   cardinality multiplies by the route count. The wiring fix and the label
   bounding must land in the same release.
3. **Unbounded `method` and `status` on the fallback path.**
   `recordFastHTTPMetrics` (`dataplane.go:858-860`) and `RecordHit`
   (`dataplane.go:914-915`) pass raw method tokens and exact status codes
   into `WithLabelValues`. `statusString` (`dataplane.go:697-713`) covers
   codes 100–599 (~500 possible values, no class bucketing), and any method
   token passes through verbatim (arbitrary tokens on 404s mint new series).
4. **Full cross-product of `cache_result` (6) × `source` (5)** exists per
   route and per status/method.

Today's practical cost: every configured route + `_default` pre-creates
9,600 series each (e.g. 8 routes + `_default` = 86,400), plus dynamic
fallback tuples from cause 3 on top. That is the ~20 %.

## Goals

- `bouine_request_duration_seconds` ≤ 10 k series total across all pods
  (AGENTS.md §9 budget), target < 5 k.
- Keep the current RED dashboards (`deploy/grafana/bouine-red.json`), SLO
  expressions (`docs/operations/slo.md`), and
  `deploy/helm/bouine/templates/prometheusrule.yaml` alerts working without
  query changes.
- No measurable hit-path CPU regression (the hit path is tuned to the
  nanosecond; any metric-path change is benchmark-gated).

## Non-Goals

- Removing or renaming the metric.
- Touching `bouine_origin_request_duration_seconds` in this plan (bounded by
  config: `pool`, `target` = configured `host:port`, `status` raw code; no
  dashboard uses it; revisit separately if Grafana shows it growing).
- Other `bouine_*` metrics (counters/gauges are comparatively cheap);
  revisit after phase 1 if `bouine.*` is still > 5 % of consumption.

## Design

### Phase 0 — Fix route attribution, take a baseline (blocks phase 1)

- Live check first: `count by (route) (bouine_requests_total)` in prod.
  Expect everything on `_default`, given the wiring.
- Fix: the router sets `ctx.SetUserValue(header.XBouineRoute, re.labelVal)`
  at `internal/server/router.go`. The request-header write was dropped:
  nothing read it and the origin pool forwards request headers verbatim
  (`origin/pool.go` VisitAll), so it leaked internal route names upstream.
- Land in the same release as phase 1. Fixing attribution alone multiplies
  series by the number of routes; bounding alone leaves the `route` axis
  dead weight.
- Record baseline series counts per metric in the tracking issue for the
  after-comparison.

### Phase 1 — Shrink the histogram label space (the main win)

**1.1 Drop `method` from the histogram.** No dashboard, SLO, or alert
splits latency by method (`bouine-red.json` has zero `method` matchers;
`slo.md` and `prometheusrule.yaml` split only by `cache_result`). GET/HEAD
dominate, and the interesting method signal (POST invalidations) belongs on
`bouine_requests_total`, which keeps the exact method. This honors the
rationale already documented at `dataplane.go:838-843` ("squashing uncommon
methods would destroy the method dimension") per-metric: `requests_total`
keeps raw methods, the histogram drops the dimension. Rewrite that comment
to state the per-metric contract so the code stops contradicting itself.

**1.2 Collapse `status` to response classes on the histogram.** Replace
exact codes with `2xx/3xx/4xx/5xx` (plus the existing `"0"` for unknown).
All consumers aggregate over statuses anyway. The pre-resolved `statuses`
table shrinks 8 → 4 (`dataplane.go:430`) and the ~500-code fallback tail
disappears. `bouine_requests_total` keeps the exact code: without buckets,
an extra status code costs 1 series instead of 16, and exact codes matter
for error budgets. The two metrics legitimately carry different status
label values; document it in the metric help text.

**1.3 Cap routes; do not squash derived labels.** Config validation caps
routes at 32 (`internal/config/config.go:389-399`). Do NOT normalize
unknown routes to `_default`: once phase 0 lands, the router only ever sets
`labelVal` derived from config (`router.go:66-81`), so unknown routes
cannot occur; and `bouine-red.json:800` groups by `route`, so squashing
`_catch-all`/`host:pathPrefix` into `_default` would silently change what
that panel shows.

**1.4 Lazy pre-resolution.** `buildRouteMetrics` must not call
`WithLabelValues` at startup. Array slots fill on first observation via
atomic pointer slots: the first observer calls `WithLabelValues` and
stores the returned child in the slot; concurrent first-touches of the
same slot are benign because `WithLabelValues` returns the same child for
the same label tuple, so both stores write identical pointers. Idle routes
then cost zero series, hot tuples keep the array fast path, and the
startup eager-allocation (9,600 series/route) disappears.

Resulting label space per route: 4 classes × 6 results × 5 sources =
120 tuples × 16 = **1,920 series/route ceiling**, with lazy creation only
for tuples actually observed. Realistic active set (2–3 classes, 2–4
results, 2–3 sources per route): a few hundred series per route, low
single-digit thousands total. Label combinations stay under the 10 k
AGENTS.md §9 budget with margin.

### Phase 2 — (optional, data-driven) trim further

Only if Grafana still flags the metric after phase 1:

- Drop `source` from the histogram (it correlates with `cache_result`;
  `requests_total` keeps it). 120 → 24 tuples/route.
- Or drop the status class (hit/miss latency is already dominated by
  `cache_result`).

Decide from the phase-1 cardinality breakdown: cut whichever label axis
actually carries the most series for the least query value.

### Phase 3 — Native histograms (do last, gated)

The stakeholder recommendation is correct in principle: sparse buckets
replace the 13 classic buckets. But a native histogram only shrinks the
bucket axis; the label axis (phases 0–1) dominates the current cost. And
client_golang **always emits classic buckets too** unless `Buckets: nil`
and `NativeHistogramBucketFactor > 1` are set (client_golang
`prometheus/histogram.go:573-585`, `pickSchema` at `:1467`). Without
phases 0–1, switching changes nothing for the classic `_bucket` queries
every consumer runs today.

**Prerequisite (blocking):** confirm how Grafana Cloud meters native
histograms, per active series (then one native histogram ≈ 1 active
series: huge win) or per active bucket (then schema 8 over a 0.5 ms–10 s
latency spread is up to ~115 potential buckets before
`NativeHistogramMaxBucketNumber` force-merges and silently degrades
resolution, i.e. potentially no win at all). Get the answer from the
Grafana Cloud billing documentation in writing before writing code. If
billing is per bucket, phase 3 saves ~nothing and phases 0–1 are the whole
fix.

Steps once unblocked:

1. Confirm the ingestion path (Prometheus `enable-native-histograms` /
   remote-write 2.0 / OTLP) on our Grafana Cloud instance.
   `client_golang v1.24.1` already supports it.
2. Dual emit: `NativeHistogramBucketFactor: 1.1` (schema 8),
   `NativeHistogramMaxBucketNumber: 100`, classic buckets kept.
3. Migrate consumers off `_bucket` series (`bouine-red.json`,
   `docs/operations/slo.md`, `prometheusrule.yaml`) to native-histogram
   PromQL (`histogram_count` / `histogram_fraction` / `histogram_quantile`
   over native syntax). Exact queries per the consumer audit.
4. Drop classic buckets (`Buckets: nil`). Verify hit-path `Observe` cost
   with the existing hit-path benchmarks before and after; abort if the
   regression exceeds 2 ns/req (the hit-path p99 plan already flagged
   native-math cost on every Observe).

## Alternatives Considered

- **Only do phase 3 (native histogram) as recommended.** Insufficient as a
  first step: classic buckets stay unless `Buckets: nil` + factor > 1, the
  label axis is untouched, and the billing model is unverified. Rejected
  as a first step.
- **Drop the histogram, keep only `requests_total`.** Loses the DP-1/DP-2
  latency SLOs (`docs/operations/slo.md`) and the RED dashboards. Rejected.
- **Sampling observations** (1-in-N): does not reduce series count (series
  exist even with sparse samples), only sample volume, and biases
  quantiles at low traffic. Rejected.
- **Metric relabeling at scrape time** (Helm ServiceMonitor
  `metricRelabelings` hook): fast stopgap if a mitigation must ship before
  the code change rolls out; loses data and does not fix the source. Keep
  as fallback only.

## Rollout

1. Phases 0 + 1 in one release (attribution fix and label bounding
   together); watch the Grafana cardinality dashboard for 24 h.
2. Phase 2 only on phase-1 data.
3. Phase 3 behind `--enable-native-histograms` (default off) until the
   billing + ingestion prerequisites are confirmed.
4. Update AGENTS.md §9 with the final label contract.

## Benchmarks

- `go test ./internal/observability/...` (label-index tests exist).
- Hit-path benchmarks (RecordHit hook at
  `internal/server/fp_conn.go:39`, h1parser/reactor call sites) before and
  after lazy pre-resolution and any phase 3 change.
- New `TestMetricCardinalityBudget`: synthetic mixed workload, count active
  series from the registry, assert ≤ 10 k combinations (AGENTS.md §9
  promises this test exists; it does not yet).
- Acceptance: Grafana Cloud cardinality dashboard shows
  `bouine_request_duration_seconds` < 5 k series over 24 h after rollout.

## Open Questions

1. (Blocks phase 3) Grafana Cloud native-histogram billing: per series or
   per bucket?
2. (Blocks phase 3) Native-histogram ingestion support on our Grafana
   Cloud instance (scrape config / remote-write version)?
3. Phase 2: which label to drop, decided on phase-1 data.
