# Native histogram for bouine_request_duration_seconds

## What changed

The request-duration histogram is now registered with
`NativeHistogramBucketFactor: 1.1` and `NativeHistogramMaxBucketNumber: 80`.
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
    regex: bouine_request_duration_seconds_bucket
    source_labels: [__name__]
```

Keep `_sum`/`_count` (average latency) and the native histogram (all
quantiles). After the relabel is in place, each active label tuple costs
`_sum` + `_count` + sparse buckets (~1-45 depending on traffic spread,
capped at 80) instead of 11 classic bucket series per tuple.

## Cost

- Native `Observe` was benched at 0 allocs/op with a ~19 ns vs ~6 ns
  classic cost on the warm path (client_golang v1.24.1, darwin/arm64);
  gated by `BenchmarkGate_HistogramObserve_Native`.
- Sparse buckets self-compact when the cap would be exceeded (resolution
  halves). Under adversarial spread (10k distinct latencies over
  0.5 ms-1.5 s) the compaction settled at 44 buckets.
- The 2.5/5/10s classic buckets were dropped earlier in this PR; hung
  fetches are tracked as 5xx counts on `bouine_requests_total`.

## Rollback

Native support is a field on the histogram constructor. To revert,
delete the three `NativeHistogram*` fields in
`internal/observability/dataplane.go` (`NewDataPlaneMetrics`); the
classic representation is unaffected and no query changes.
