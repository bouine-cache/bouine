# 62 — Config reload (experimental)

Apply config changes to a running daemon without a restart
(`POST /v1/config/reload`, `bouine config reload`, issue #817, phase 1).
Off by default: the daemon must run with
`experimental.config_reload: true`.

---

## What it does

1. Re-parses and validates the file (`config.Load`: strict YAML,
   defaults, `${VAR}` interpolation against the daemon's
   environment). On any failure: **nothing changes** — the daemon
   keeps running on the current config.
2. Applies the **hot-appliable subset** through the same knob registry
   as `bouine config set` (atomic per knob): global knobs
   (`cluster.ban_ttl`, `url_ring_sample_rate`) and per-pool health
   knobs (`pool:<name>.health.passive.consecutive_5xx`,
   `pool:<name>.health.passive.eject_for`).
3. Reports three sections — the operator must never believe a field
   applied when it did not:
   - `applied` — knobs changed, old → new values
   - `unchanged` — knobs already at the file value
   - `NEEDS RESTART` — cold-only fields that differ (listeners, TLS,
     storage, cluster, routes, pool membership, ...) and knobs that
     refused (e.g. `eject_for` on a pool whose boot config left it
     unset, so no reaper exists)

## Procedure

```bash
export BOUINE_SERVER=127.0.0.1:9000 BOUINE_TOKEN=<token>

bouine config reload                      # daemon's startup file
bouine config reload --file /etc/bouine/config.new.yaml
```

Prefer a dry review first: `bouine config diff <running.yaml>
<candidate.yaml>` previews the same changes without touching
anything.

## Rollback

Operational, not a command: reload the previous (git-tracked) file.

```bash
git -C /etc/bouine checkout config.yaml
bouine config reload
```

## Caveats

- **Per-node.** Reload does not coordinate the cluster: run it
  against each node (or roll). `bouine config set`, by contrast,
  fans applied knobs out to every live peer — a `set` on one node
  followed by `reload` on another can race; the reload's file value
  wins on its node. In Kubernetes the deployment mechanism stays the
  rolling restart.
- **Drift after reload.** `bouine config diff` compares live values
  against the *last-loaded* file. An in-place edit (the normal
  workflow) is exactly what a restart would load, so the answer is
  truthful. After an out-of-path reload (`--file /tmp/other.yaml`), a
  restart still loads the daemon's original path — reload the
  original path (or move the new file onto it) before restarting.
- **Phase 2 pending.** Route table and upstream pool target swaps are
  phase 2 (separate PR): those changes always land in
  `NEEDS RESTART` today.
- **The flag itself is cold.** Changing `experimental.config_reload`
  in the file is reported as needing a restart.

## Observability

- `bouine_config_reloads_total{result}` — `applied`, `noop`,
  `rejected`.
- slog audit on every attempt: path, result, per-section counts
  (never values). The admin write rate limit applies.
- The ops history (dashboard invalidation page) records each reload.
