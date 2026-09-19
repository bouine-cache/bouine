# Native histograms for bouine duration metrics

## What changed

All bouine duration histograms are now registered with
`NativeHistogramBucketFactor: 1.1` and `NativeHistogramMaxBucketNumber: 80`.
The affected metrics are:

- `bouine_request_duration_seconds` (data-plane RED)
- `bouine_cloudflare_purge_duration_seconds` (Cloudflare purge API latency)
- `bouine_startup_duration_seconds` (process startup)
- `bouine_peer_fetch_duration_seconds` (cluster peer-fetch RPCs)
- `bouine_warm_compaction_duration_seconds` (warm-tier compaction)
- `bouine_wal_write_duration_seconds` (WAL drain-and-sync)
- `bouine_origin_request_duration_seconds` (origin response time)

Prometheus exposition contains BOTH:

- the classic `_bucket`/`_sum`/`_count` series (unchanged, all existing
  dashboards/alerts keep working), and
- the native sparse-bucket representation (schema 3) on the same metric
  family, which Grafana Cloud/Mimir can query directly with
  `histogram_quantile()` without materializing per-bucket series.

## Getting the cardinality win

The native form does not reduce scrape cardinality by itself:
client_golang always emits the classic series too. The win requires
dropping the classic `_bucket` series server-side. Add to the scrape
config for bouine pods:

```yaml
metric_relabel_configs:
  - action: drop
    regex: bouine_(request_duration_seconds|cloudflare_purge_duration_seconds|startup_duration_seconds|peer_fetch_duration_seconds|warm_compaction_duration_seconds|wal_write_duration_seconds|origin_request_duration_seconds)_bucket
    source_labels: [__name__]
```

Keep `_sum`/`_count` (average latency) and the native histogram (all
quantiles). After the relabel is in place, each active label tuple costs
`_sum` + `_count` + sparse buckets (~1-45 depending on traffic spread,
capped at 80) instead of 16 classic bucket series per tuple.

## Series arithmetic with the traffic_class axis (ADR-0047)

The `traffic_class` label multiplies each family's per-tuple ceiling by
`1 + #configured classes` (the `unclassified` fallback plus the
configured set; the config cap is 8, so the multiplier tops out at 9).
With no classes configured the multiplier is exactly 1 — the label is
present but single-valued, so deployments without the feature see no
cardinality change.

Per-family worst case at 33 pools (incl. `_default`) × classSlots:

| Family | Per (pool, class) | At classSlots 9 |
|---|---|---|
| `bouine_requests_total` | 7 status × 5 results × 5 sources = 175 | 15 575 |
| `bouine_request_duration_seconds` (classic `_bucket`) | 6 classes × 5 results = 30 tuples | 2 670 tuples → 42 720 classic series |
| `bouine_response_bytes_total` | 5 results × 5 sources = 25 | 2 025 |

Two operator guidelines follow:

- **`bouine_requests_total` crosses the AGENTS.md §9 10 000-series line
  whenever `pools × (1 + #classes) > 57`** (33 pools × 3 slots is
  already 17 325). This is a documented exception (ADR-0047): the
  overage is opt-in and the label set is closed. If you are above the
  line, reduce classes, split fleets, or apply the drop pattern above
  (`metric_relabel_configs` can drop whole classes on this family too).
- **The histogram's classic `_bucket` series are the dominant cost**
  and the existing `metric_relabel_configs` drop rule already removes
  them; the native sparse form multiplies identically but costs
  `_sum` + `_count` + ≤80 sparse buckets per tuple.

## Upgrade note: series identity reset

Adding the `traffic_class` label changes every data-plane series'
identity (Prometheus treats a label-set change as a new series). At
the upgrade boundary, `rate()` over `bouine_requests_total` /
`bouine_request_duration_seconds` / `bouine_response_bytes_total` will
show a one-window gap per series while old and new series coexist.
Recording rules and alerts that aggregate these families need no
change — the gap is transient and self-heals after one scrape
interval; dashboards drawn across the boundary may show a visual
discontinuity at the deploy timestamp.

## Cost

- Native `Observe` was benched at 0 allocs/op with a ~19 ns vs ~6 ns
  classic cost on the warm path (client_golang v1.24.1, darwin/arm64);
  gated by `BenchmarkGate_HistogramObserve_Native`.
- Sparse buckets self-compact when the cap would be exceeded (resolution
  halves). Under adversarial spread (10k distinct latencies over
  0.5 ms-1.5 s) the compaction settled at 44 buckets.
- The 2.5/5/10s classic tail buckets are retained so slow misses and
  hung-fetch tails are distinguishable in `histogram_quantile` queries;
  anything beyond 10s lands in the `+Inf` overflow bucket.

## Rollback

Native support is a field on the histogram constructor. To revert,
delete the three `NativeHistogram*` fields from any of the affected
histogram constructors (listed above); the classic representation is
unaffected and no query changes.
