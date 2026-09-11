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
