# Origin target ejection and restore

How targets leave the pool and how they come back, and which metric tells
you which path restored a target.

## Ejection sources

| Source | Knob | Effect |
|---|---|---|
| Passive 5xx | `health.passive.consecutive_5xx` | Eject after N consecutive 5xx, counted only once successful fetches on the target have stopped for the 5s success window (#839) |
| Passive connection errors | `health.passive.consecutive_5xx` | Eject after N consecutive transport failures — never excused by recent successes (the origin is not answering) |
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

## The passive 5xx success window (#839)

A 5xx *response* from the origin proves the target is alive, so it only
drives passive ejection once the target has stopped completing
successful fetches for 5 seconds (`passive5xxCountWindow`): an origin
that answers 200s on any path while returning 5xx on another is never
ejected, no matter how the completions interleave. `consecutive_5xx`
then reads as "eject if the origin keeps 5xxing *and its successes
have stopped*". Consequences for operators:

- A healthy origin serving one 5xx-returning path (rate limiting, an
  error endpoint) alongside healthy traffic no longer needs its
  `consecutive_5xx` raised to survive bursts.
- A genuinely failing origin — successes stop, 5xx keep coming —
  ejects within the 5 s window plus `consecutive_5xx` more 5xx
  completions. Expect ejection ~5 s later than pre-#839.
- `bouine_origin_passive_errors_total` still counts every observed 5xx
  response; only the ejection streak is window-gated.
- Connection errors are never excused by recent successes — a target
  that stops answering at the transport level ejects by the consecutive
  count alone.
- An origin whose *only* origin-bound path legitimately returns 5xx
  still ejects once `consecutive_5xx` 5xx responses complete (no
  successful fetch ever lands). If that origin is healthy by design,
  disable passive ejection on that pool
  (`pool:<name>.health.passive.consecutive_5xx 0`) and rely on active
  probes.

## `health.passive.eject_for`

When set, a passively ejected target is restored automatically once the
window elapses (reaper scans every second; restore latency is at most
`eject_for + 1s`). The restore is blind: the target receives real traffic
again. A target that is still broken re-ejects after `consecutive_5xx`
fresh errors — the error counter is zeroed on restore, so the threshold
counts new failures only. Expect a broken target with a short window to
oscillate; that is the knob's contract, and the operator opted in.

An unset `eject_for` resolves to the **built-in 30s default** on pools
with `consecutive_5xx` configured: leaving the window unset used to keep
an ejected target out until a manual restore, which on a single-target
pool without an active probe meant *until restart* — one transient error
burst (measured: 22 clustered connection errors) ejected the origin for
the rest of the run and every subsequent miss returned an instant 502.
To disable automatic restore on a passive pool, set a very long window;
there is no eject-forever sentinel anymore. Pools without `consecutive_5xx`
never eject, so their window stays unset.

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
  changeable when the boot config started the reaper — an explicit
  `eject_for`, or `consecutive_5xx` with the window left unset (the
  30s built-in default runs the reaper). Shrinking it restores ejected
  targets sooner, `0` pauses restores. A pool booted without passive
  health rejects runtime sets with a precise error — set it in YAML and
  restart.

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
