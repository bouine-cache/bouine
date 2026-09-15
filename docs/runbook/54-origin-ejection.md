# Origin target ejection and restore

How targets leave the pool and how they come back, and which metric tells
you which path restored a target.

## Ejection sources

| Source | Knob | Effect |
|---|---|---|
| Passive 5xx | `health.passive.consecutive_5xx` | Eject after N consecutive 5xx or connection errors |
| Active probes | `health.active.*` | Eject after `unhealthy_threshold` failed probes |
| Manual admin | `POST /v1/...` MarkHealthy path | Restores only |

## Restore sources

`bouine_origin_restores_total{source}` tells you which path restored a
target:

| Label | Path |
|---|---|
| `active` | Active probe reached `healthy_threshold` successes |
| `manual` | Operator / admin API marked the target healthy |
| `eject_for` | The `health.passive.eject_for` window elapsed |

## `health.passive.eject_for`

When set, a passively ejected target is restored automatically once the
window elapses (reaper scans every second; restore latency is at most
`eject_for + 1s`). The restore is blind: the target receives real traffic
again. A target that is still broken re-ejects after `consecutive_5xx`
fresh errors — the error counter is zeroed on restore, so the threshold
counts new failures only. Expect a broken target with a short window to
oscillate; that is the knob's contract, and the operator opted in.

Without `eject_for` (or an active health check), an ejected target stays
out until a manual restore — the dashboard's "ejected targets never
rejoin" insight fires for that configuration.

Alert on `rate(bouine_origin_ejections_total[5m]) > 0` together with
`rate(bouine_origin_restores_total{source="eject_for"}[5m])`: sustained
re-ejection after window restores means the origin is unhealthy, and the
oscillation doubles origin traffic on the failing target (each restore
sends one more request before the next ejection).
