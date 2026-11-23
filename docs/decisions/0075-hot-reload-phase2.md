# ADR-0075: Hot reload phase 2 — route table + pool target swap with cluster fan-out

- **Status**: Accepted
- **Date**: 2026-11-23
- **Deciders**: @bouine-core
- **Phase**: config reload phase 2 (issue #951, extends #817/#815)
- **References**: issue #951 (locked decisions 1-13), ADR-0050 (shared kernels), `internal/server/router.go`, `internal/origin/pool.go`, `internal/cluster/broadcast.go`, `cmd/bouine/cmd/configreload_phase2.go`, `docs/runbook/62-config-reload.md`

## Context and Problem Statement

Phase 1 (#817) made a small knob set hot-swappable behind
`experimental.config_reload`. Operators still need a restart to change
the route table or an upstream pool's targets — the two config surfaces
that change most often in practice. Swapping them live has two hard
problems: the data plane reads the route table and pool targets on
every request (a swap must be race-free and allocation-free on the hit
path), and in a cluster a diverged fleet fails safe (miss) but burns
origin traffic with no alert — so key-form changes need every live peer
to agree before they apply.

## Decision

1. **Unified immutable snapshot.** Route table + pool registry + last-applied
   config + generation counter live in one `configSnapshot` behind a
   single `atomic.Pointer` on the run state. `Router.matchRoute` does
   one atomic load per request and returns a `*routeEntry` into the
   immutable table, so in-flight requests during a swap are safe and
   the hit path keeps allocs/op = 0. The snapshot is the single source
   of truth for drift reporting (the phase-1 `hotCfg` atomic is folded
   in).
2. **Swap sequence** (atomic or not at all — any build error leaves
   the running snapshot untouched): (a) construct added pools,
   (b) swap target lists on pools present in both snapshots under each
   pool's `mu.Lock` (fresh `[]*Target` slice, never mutating the old
   backing array; unchanged addrs reuse their `*Target` so eject/error
   state survives; `pick()` holds `RLock` across the element read),
   (c) build handlers for changed routes against the correct pool
   objects, (d) one atomic pointer store. Unchanged routes reuse the
   existing routeEntry (handler + fastPath objects identical).
3. **Close-at-swap.** Handlers of changed routes are `Close()`d at
   swap (the scheduler stops, pending refreshes drop, in-flight ones
   are joined); swapped-out handlers join a draining set that engine
   shutdown also joins. Unchanged routes never rebuild, so a reload
   does not drop every pending refresh fleet-wide.
4. **Pool goroutine lifecycle.** A pool added by a reload gets its
   active-probe and eject-reaper goroutines started at swap under the
   supervised group; a removed pool gets `Close()`
   (`CloseIdleConnections`) — in-flight fetches complete on their own
   because after the atomic swap no route can reference it
   (`config.Validate` rejects unknown pools). `connect.*` and
   `health.active.*` settings stay cold (baked at boot).
5. **Cluster fan-out.** `BroadcastConfigReload(gen, sha256)` posts to
   every live peer's `/v1/peer/config/reload` (admin bearer token, like
   `/v1/peer/config/hot`), synchronously collecting per-peer acks
   (`applied` / `current` / `stale_config` / error). The payload is the
   generation counter plus the SHA-256 of the **resolved** config
   (canonical marshal after `Load`+`Validate`), never the file bytes —
   `${VAR}` interpolation is per-daemon, so identical mounted files can
   resolve differently and only the resolved hash catches that. Peers
   re-read their own boot path and apply only if `gen > localGen` AND
   their resolved hash matches; mismatch replies `stale_config`. The
   peer endpoint never re-broadcasts (loop guard with the gen counter).
6. **Key-form gate.** Route edits changing any cache-key-form field
   (`include_host`, `verbatim_encoding`, `include_headers`,
   `exclude_headers`, `cookie_presence`, `cookie_values`,
   `strip/keep_query_params`, `strip_query_prefix`, `strip_empty_params`,
   `dedup_query_params`) — including route add/remove/rename, which
   change the (identity -> key form) set — are refused unless every
   live peer acks. Results distinguish `applied_all` /
   `applied_local_only` / `rejected`.
7. **Rejoin catch-up.** A peer that misses a fan-out is converged via
   `NotifyJoin`: a live member pushes the current `(gen, hash)` to the
   joiner over the same endpoint. The residual alarm is the
   generation-skew alert on `StatusNode.ConfigGen`.
8. **Endpoint restriction.** `POST /v1/config/reload` only reloads the
   boot config path; a differing non-empty `path` gets 400. CLI
   `--file` is deprecated the same way. `bouine config diff --file`
   stays (read-only).
9. **Route identity uniqueness.** `config.Validate` requires a unique
   effective identity per route (explicit `name`, else derived name +
   full match predicate), which name-keyed diffing needs and which also
   bounds `route`-label cardinality. **Migration note:** configs with
   colliding derived names now fail validation — rename one route.
10. **No implicit purge** on route policy change; operators purge by
    surrogate key explicitly (runbook 62).

## Consequences

- The hit path stays zero-alloc: one atomic load replaces the slice
  read, and the swap assigns a fresh slice under a lock, never mutating
  the old one.
- A reload that fails to build leaves the running daemon untouched and
  reports the error (per-class atomicity).
- Key-form changes cannot silently diverge the fleet: they need every
  live peer's ack, and `${VAR}` divergence surfaces as `stale_config`.
- Pool add/remove no longer leaks or orphans probe/reaper goroutines.
- New validation failure mode for colliding derived route names (see
  migration note above).
