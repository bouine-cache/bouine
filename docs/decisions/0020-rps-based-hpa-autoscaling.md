# ADR-0020: RPS-based HPA autoscaling

- **Status**: Proposed
- **Date**: 2026-07-07
- **Deciders**: bot-group-staffeng

## Context and Problem Statement

The HPA for bouine uses CPU `AverageValue` as its sole scaling signal.
For a reverse-proxy cache this is problematic:

1. **Cache misses are I/O-bound**, not CPU-bound. A pod waiting on an
   origin response burns near-zero CPU, so high miss rates do not
   trigger scale-up when they should (if the origin is slow).
2. **Eviction churn produces CPU noise** unrelated to real traffic.
   SIEVE eviction under a tight memory budget (512 MiB) runs list
   manipulation under shard locks, spiking CPU and triggering
   spurious scale-ups.
3. **Anti-entropy backfill** on new pods joining the cluster in `full`
   mode produces CPU spikes that the HPA interprets as load, creating
   a positive feedback loop: scale up → backfill → CPU spike → scale
   up more.

In full cluster mode, adding pods does not increase cache capacity
(every pod stores every object). Extra pods increase gossip overhead
and backfill churn. The HPA should scale on real request load, not
CPU side effects.

## Decision Drivers

- Prod-eu HPA flapped between 3–10 pods over 3 days on ~225 RPS
- Hit rate dropped from 64% to 31% partly due to pod churn
- CPU is a noisy proxy for cache load

## Considered Options

### Option 1: Keep CPU-only, tune thresholds

Raise `averageValue` and add scale-up stabilization. Already done as
an immediate fix (bouine-config PR #147), but CPU remains a noisy
signal.

### Option 2: Add RPS-based Pods custom metric

Use a `type: Pods` metric backed by a Prometheus adapter exposing
`bouine_requests_per_second`. The HPA scales when per-pod RPS exceeds
a threshold (e.g. 100 RPS/pod). CPU remains as a secondary metric for
safety.

### Option 3: Switch to External metric via KEDA

Use KEDA with a Prometheus trigger. More powerful (queue-based,
scaled-to-zero) but adds an operator dependency. Overkill for a
StatefulSet cache that should never scale to zero.

## Decision Outcome

Chosen: **Option 2** — RPS-based Pods custom metric, with CPU as
secondary.

### Template changes (`hpa.yaml`)

- Added `rpsTrigger` block rendering a `type: Pods` metric with
  configurable `metricName` (default `bouine_requests_per_second`)
  and `averageValue` (default `100`).
- Added `scaleUp` behavior with `stabilizationSeconds` and
  `selectPolicy: Min` to prevent rapid scale-up on transient spikes.
  The `scaleUpStabilizationSeconds` value was already defined in
  values but never rendered by the template.
- Both `rpsTrigger` and `cpuTrigger` are optional and independent.
  When both are enabled, the HPA uses whichever demands more pods.

### Prerequisites for RPS-based scaling

A Prometheus adapter (`k8s-prometheus-adapter` or
`prometheus-adapter`) must be installed in the cluster and configured
to expose the `bouine_requests_per_second` custom metric from the
`bouine_requests_total` Prometheus counter. A sample adapter rule:

```yaml
- seriesQuery: 'bouine_requests_total{namespace!="",pod!=""}'
  resources:
    overrides:
      namespace: {resource: "namespace"}
      pod: {resource: "pod"}
  name:
    matches: "^(.*)_total"
    as: "${1}_per_second"
  metricsQuery: 'sum(rate(<<.Series>>{<<.LabelMatchers>>}[2m])) by (<<.GroupBy>>)'
```

### Enabling RPS-based scaling in an environment

```yaml
autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 5
  rpsTrigger:
    metricName: bouine_requests_per_second
    averageValue: "100"
  cpuTrigger:
    targetType: AverageValue
    averageValue: "3000m"
  scaleUpStabilizationSeconds: 120
  scaleDownStabilizationSeconds: 600
```

## Consequences

- **Positive**: Stable scaling signal tied to real traffic; no
  feedback loop from eviction churn or backfill.
- **Positive**: CPU remains as a safety net for genuine CPU
  saturation (e.g. TLS handshake storms, malformed-request floods).
- **Negative**: Requires Prometheus adapter installation and
  configuration. Environments without the adapter cannot use
  `rpsTrigger` (the HPA will fail to resolve the metric).
- **Negative**: RPS threshold must be tuned per environment. 100
  RPS/pod is a conservative default for bouine's workload.
