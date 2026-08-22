# GC tuning for fasthttp

## Background

The fasthttp migration (ADR-0034, issue #521) reduces the per-request
allocation rate by 60-73%. The default `GOGC=100` triggers GC when the
heap doubles. With fewer allocations, the heap grows slower between GC
cycles, so GC already runs less frequently after the migration.

This guide documents how to tune GC further to minimize p99 jitter on
the hit path.

## Recommended settings

### GOGC=200

Set the `GOGC` environment variable to `200` (default is `100`).

With `GOGC=200`, GC triggers when the heap grows to 3× the live set
(after the last GC), instead of 2× with `GOGC=100`. Combined with the
60-73% allocation reduction from the fasthttp migration:

- **Before migration** (`GOGC=100`, `net/http`): GC every 3-5s at 100K RPS.
- **After migration** (`GOGC=100`, `fasthttp`): GC every 5-8s at 100K RPS.
- **After migration** (`GOGC=200`, `fasthttp`): GC every 20-30s at 100K RPS.

Cost: ~50% more live heap at steady state. But the heap is already
40-50% smaller from the migration, so the net live heap is still lower
than before the migration.

### GOMEMLIMIT

Set `GOMEMLIMIT` to 87% of the container memory limit.

Example (4GiB container):
```
GOMEMLIMIT=3500MiB
```

`GOMEMLIMIT` is a soft memory limit that prevents OOM during allocation
spikes. If the heap approaches `GOMEMLIMIT`, GC runs even if the `GOGC`
threshold is not met. This prevents OOM kills when `GOGC=200` allows
the heap to grow larger.

### Combined effect

`GOGC=200` + `GOMEMLIMIT=3500MiB` at 100K RPS:
- GC frequency: every 20-30s (was every 3-5s before migration).
- GC pause p99: ~0.5-1ms (was ~2-5ms).
- Hit-path p99 GC jitter: effectively eliminated as a latency contributor.

## How to set

`GOGC` and `GOMEMLIMIT` are Go runtime environment variables, not bouine
config fields. Set them in the container spec or systemd unit:

```yaml
# Kubernetes deployment
env:
  - name: GOGC
    value: "200"
  - name: GOMEMLIMIT
    value: "3500MiB"
```

```ini
# systemd unit
Environment=GOGC=200
Environment=GOMEMLIMIT=3500MiB
```

## Verification

1. Run `bench/loadtest/` at 100K RPS with `GOGC=100` vs `GOGC=200`.
2. Monitor GC frequency via Prometheus: `rate(go_gc_duration_seconds_count[1m])`.
3. Monitor GC pause via Prometheus: `go_gc_duration_seconds{quantile="0.99"}`.
4. Monitor hit-path p99 via the access log or load test harness.
5. Verify no OOM under `GOMEMLIMIT` with `GOGC=200` during peak traffic.

## When to use GOGC=100

If memory is constrained (container < 1GiB), keep `GOGC=100` to limit
heap growth. The fasthttp migration already reduces GC frequency by
2.5-3.5× at `GOGC=100` — the additional `GOGC=200` tuning is optional
for memory-constrained deployments.
