# 62 — Config reload (experimental)

Apply config changes to a running daemon without a restart
(`POST /v1/config/reload`, `bouine config reload`, issues #817+#951,
phases 1+2). Off by default: the daemon must run with
`experimental.config_reload: true`.

---

## What it does

1. Re-parses and validates the file (`config.Load`: strict YAML,
   defaults, `${VAR}` interpolation against the daemon's
   environment). On any failure: **nothing changes** — the daemon
   keeps running on the current config.
2. Applies the **hot-appliable subset**, atomically per class:
   - hot knobs through the same registry as `bouine config set`
     (global `cluster.ban_ttl`, `url_ring_sample_rate`, per-pool
     `health.passive.*`);
   - **routes and pool membership/targets** (phase 2, #951) via one
     atomic snapshot swap: added pools get their probe/reaper
     goroutines started at swap, common pools get a fresh target list
     (unchanged addresses keep their health state), removed pools are
     closed, changed routes get rebuilt handlers (closed at swap,
     unchanged routes keep their handler objects), and the router
     switches to the new immutable route table with one atomic load per
     request.
3. In a strong-mode cluster, fans the reload out to every live peer
   (`POST /v1/peer/config/reload`, admin token) carrying the
   generation counter + SHA-256 of the **resolved** config — and
   reports each peer's ack (`applied` / `current` / `stale_config` /
   error). A peer whose `${VAR}`-interpolated environment resolves the
   same file differently reports `stale_config`.
4. Reports the sections — the operator must never believe a field
   applied when it did not:
   - `applied` — knobs changed, old → new values
   - `unchanged` — knobs already at the file value
   - `result` — `applied_all` (key-form change acked by every live
     peer), `applied_local_only` (applied here, some peer did not
     ack), or `applied`
   - `peers` — per-peer fan-out acks
   - `gen` — the config generation after the reload (compare across
     peers for the generation-skew alarm)
   - `NEEDS RESTART` — cold-only fields that differ (listeners, TLS,
     storage, cluster, `connect.*`, `health.active.*`, `static.root`,
     ...) and anything that refused

## Procedure

```bash
export BOUINE_SERVER=127.0.0.1:9000 BOUINE_TOKEN=<token>

bouine config reload                      # the daemon's startup file
```

The daemon only ever reloads its **boot config file**: edit that file
in place (the normal ConfigMap-mount workflow) and reload.
`bouine config reload --file <other.yaml>` is deprecated and rejected
— a daemon never loads a different path at runtime (issue #951
decision 5). `bouine config diff --file a.yaml b.yaml` stays: it is
read-only and touches no daemon.

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

- **Cluster-coordinated.** One reload fans out to every live peer and
  waits for acks; the result field tells you whether the fleet
  converged (`applied_all`) or only this node applied
  (`applied_local_only`). A peer that missed the fan-out (down,
  partitioned) is caught up when it rejoins: a live member pushes the
  current `(generation, hash)` to it on join. Residual misses are
  caught by the **generation-skew alert**: compare `config_gen` in
  `GET /v1/status` across peers and alert when they diverge.
- **Key-form gate.** Route edits that change the cache-key form
  (`cache.key.*`) — including adding, removing or renaming a route —
  are applied only when every live peer acks; otherwise the result is
  `applied_local_only` and the divergence is visible in `peers`.
  A key-form change never silently splits ring ownership.
- **No implicit purge.** A route policy change does not purge stored
  entries: key-form changes orphan them (fail-safe miss). Purge
  explicitly by surrogate key after a key-form change.
- **Kubernetes note.** With the chart's `checksum/config` annotation a
  ConfigMap change still rolls pods — enabling `config_reload` in K8s
  only pays off if that annotation is removed.
- **Drift after reload.** `bouine config diff` compares live values
  against the *last-loaded* file. An in-place edit (the normal
  workflow) is exactly what a restart would load, so the answer is
  truthful.
- **The flag itself is cold.** Changing `experimental.config_reload`
  in the file is reported as needing a restart.

## Observability

- `bouine_config_reloads_total{result}` — `applied`, `applied_all`,
  `applied_local_only`, `noop`, `rejected`.
- `config_gen` in `GET /v1/status` / `bouine status` — the monotonic
  config generation; alert on skew across peers.
- slog audit on every attempt: path, result, per-section counts
  (never values). The admin write rate limit applies.
- The ops history (dashboard invalidation page) records each reload.
