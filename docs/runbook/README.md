# Operator runbook

This directory holds short, action-oriented documents that operators
read at 3 AM. Each runbook addresses a specific failure mode or
operator task. Per `AGENTS.md §10`, runbooks are updated alongside the
code change that introduces a new failure mode.

Naming convention: `NN-topic.md` where `NN` is a two-digit category:

- `00-` — daily ops (start, stop, reload, drain).
- `10-` — capacity & scaling.
- `20-` — purge & cache invalidation.
- `30-` — cluster operations.
- `40-` — TLS & certificates.
- `50-` — incident response.
- `90-` — postmortems index.

Phase-by-phase delivery:

- Phase 1 → `00-lifecycle.md`, `10-capacity.md`.
- Phase 3 → `20-purge-ban.md`.
- Phase 4 → `30-cluster.md`, `40-tls.md`.
- Phase 4.5 → `50-incidents.md`, `90-postmortems/`.

The full set of runbooks is a phase 4.5 exit criterion (`PLAN.md §15`).
