# bouine — SLO / SLI Reference

This document defines the Service Level Objectives (SLOs) and corresponding
Service Level Indicators (SLIs) for a production bouine deployment. Numbers are
validated against the load-test benchmark suite (see `bench/loadtest/`) running
on a 3-node StatefulSet with 4 vCPU / 4 GiB per pod.

---

## Data-plane SLOs

| SLO ID | Objective | SLI | Measurement window | Alert threshold |
|--------|-----------|-----|--------------------|-----------------|
| DP-1 | p99 hit-path latency ≤ 1 ms at 50 kRPS | `histogram_quantile(0.99, rate(bouine_request_duration_seconds_bucket{cache_result="HIT"}[5m]))` | 30-day rolling | > 2 ms for 5 min |
| DP-2 | p99 miss-path latency ≤ 50 ms at 10 kRPS | `histogram_quantile(0.99, rate(bouine_request_duration_seconds_bucket{cache_result="MISS"}[5m]))` | 30-day rolling | > 100 ms for 5 min |
| DP-3 | Cache hit rate ≥ 80 % on mixed workload | `rate(bouine_requests_total{cache_result="HIT"}[5m]) / rate(bouine_requests_total[5m])` | 24-hour rolling | < 60 % for 10 min |
| DP-4 | Error rate (5xx) ≤ 0.1 % | `rate(bouine_requests_total{status=~"5.."}[5m]) / rate(bouine_requests_total[5m])` | 30-day rolling | > 1 % for 2 min |
| DP-5 | Zero 5xx during rolling restart | Same as DP-4 measured during `kubectl rollout` window | Per-release | Any 5xx in window |
| DP-6 | 99.9 % monthly availability | `1 - (error_minutes / total_minutes)` | 30-day calendar | < 99.5 % trailing 7d |

---

## Cluster SLOs

| SLO ID | Objective | SLI | Measurement window | Alert threshold |
|--------|-----------|-----|--------------------|-----------------|
| CL-1 | p99 purge propagation ≤ 1 s across 3-node cluster | Measured by `5.5_purge_broadcast` scenario: time from purge API call to all peers returning 404 on the purged key | Per-release | > 2 s |
| CL-2 | Peer-fetch hit rate ≥ 70 % in steady state | `rate(bouine_peer_fetch_hits_total[5m]) / (rate(bouine_peer_fetch_hits_total[5m]) + rate(bouine_peer_fetch_misses_total[5m]))` | 1-hour rolling | < 50 % for 15 min |
| CL-3 | Ring convergence after node loss ≤ 30 s | Gossip `NotifyLeave` timestamp to first successful request served from replacement node | Per chaos run | > 60 s |
| CL-4 | Zero object loss during rolling restart | Verified by the rolling-restart chaos scenario: no `cache_result="HIT"` → `cache_result="MISS"` transitions for warm keys | Per-release | Any warm-key miss in window |

---

## Admin-plane SLOs

| SLO ID | Objective | SLI | Measurement window | Alert threshold |
|--------|-----------|-----|--------------------|-----------------|
| AP-1 | `/healthz` p99 latency ≤ 5 ms | `histogram_quantile(0.99, rate(bouine_admin_request_duration_seconds_bucket{path="/healthz"}[5m]))` | 30-day rolling | > 20 ms for 5 min |
| AP-2 | Hot-reload completes in ≤ 500 ms | Time from SIGHUP / POST `/v1/config/reload` to new config active | Per-config change | > 2 s |

---

## Reference configuration for SLO targets

The targets above assume the following minimum pod spec:

```yaml
resources:
  requests:
    cpu: "2"
    memory: 2Gi
  limits:
    cpu: "4"
    memory: 4Gi
env:
  - name: GOMEMLIMIT
    value: "3GiB"
  - name: GOGC
    value: "100"
```

And the following hot-tier configuration:

```yaml
storage:
  hot_max_bytes: 2GiB
  eviction: sieve
```

Latency targets degrade gracefully when `hot_max_bytes` is reduced: expect
+20–40 % p99 per halving of the hot tier.

---

## Error budget

| SLO | Monthly target | Error budget (minutes/month) |
|-----|---------------|------------------------------|
| DP-6 (availability) | 99.9 % | 43.8 min |
| DP-1 (hit latency) | 99.9 % | 43.8 min |
| DP-4 (error rate) | 99.9 % | 43.8 min |

Budget is consumed by: deployment windows, chaos tests, upstream incidents.
Budget alerts fire at 50 % burn rate over 6 h or 100 % burn rate over 1 h.

---

## Prometheus alert rules (reference)

```yaml
groups:
  - name: bouine-slo
    rules:
      - alert: BouineHitLatencyHigh
        expr: |
          histogram_quantile(0.99,
            rate(bouine_request_duration_seconds_bucket{cache_result="HIT"}[5m])
          ) > 0.002
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "bouine hit-path p99 > 2 ms (SLO DP-1)"

      - alert: BouineErrorRateHigh
        expr: |
          rate(bouine_requests_total{status=~"5.."}[5m])
          / rate(bouine_requests_total[5m]) > 0.01
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "bouine 5xx rate > 1 % (SLO DP-4)"

      - alert: BouineHitRateLow
        expr: |
          rate(bouine_requests_total{cache_result="HIT"}[5m])
          / rate(bouine_requests_total[5m]) < 0.60
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "bouine cache hit rate < 60 % (SLO DP-3)"

      - alert: BouinePeerFetchHitRateLow
        expr: |
          rate(bouine_peer_fetch_hits_total[5m])
          / (rate(bouine_peer_fetch_hits_total[5m]) + rate(bouine_peer_fetch_misses_total[5m]))
          < 0.50
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "bouine peer-fetch hit rate < 50 % (SLO CL-2)"
```

---

*Last reviewed:* $(date -u +%Y-%m-%d)
