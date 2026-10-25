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

### Tuning ejection live (hot config)

Both passive-ejection knobs are hot-config, scoped per pool
(`bouine config get/set pool:<name>.health.passive.…`):

- `pool:<name>.health.passive.consecutive_5xx` — the ejection trigger.
  Fully hot: the fetch path reads the atomic threshold per request.
  Raise it when a flapping origin ejects too eagerly, drop to `0` to
  disable passive ejection on that pool.
- `pool:<name>.health.passive.eject_for` — the restore window. Hot-
  changeable only when the boot config set `eject_for` (the reaper
  goroutine starts once, at boot); shrinking it restores ejected
  targets sooner, `0` pauses restores. A pool without a boot-time
  `eject_for` rejects runtime sets with a precise error — set it in
  YAML and restart.

```bash
bouine config set pool:api.health.passive.consecutive_5xx 10 --dry-run
bouine config set pool:api.health.passive.consecutive_5xx 10
```

Applied sets fan out to every live peer (strong mode) and report the
acknowledged count; they stay ephemeral — persist any recovery value
in YAML before the next deploy (`bouine config diff` shows the
drift).

## stayin_alive grace retention (ADR-0051)

While every target of a pool is ejected, the TTL reaper withholds expired
entries from `stayin_alive` routes on that pool — the route keeps serving
stale through the outage. `bouine_hot_store_reaper_grace_holds_total`
counts each withheld entry per reaper pass (30 s default):

- **Non-zero during an ejection + no restores** on a stayin_alive pool:
  grace retention working as designed.
- **Sustained non-zero while all pools report healthy**: a stale
  ejection — restore the target (active probe or MarkHealthy) and the
  reaper collects the withheld entries on its next pass.
- **stayin_alive routes on pools without `consecutive_5xx` and without
  active health checks**: no ejection signal exists, so grace cannot
  engage — configure at least one health signal (both is best:
  `consecutive_5xx` ejects, `eject_for` restores automatically).

Once the pool has a healthy target again, the next reaper pass reaps all
withheld expired entries on the normal schedule — no manual cleanup.
Capacity (SIEVE hot-tier pressure, warm disk budget) still applies during
grace: held entries can be demoted to the warm tier, not lost.
