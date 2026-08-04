# Operator runbook

This directory holds short, action-oriented documents that operators
read at 3 AM. Each runbook addresses a specific failure mode or
operator task. Per `AGENTS.md §10`, runbooks are updated alongside the
code change that introduces a new failure mode.

## Index

| File | Topic |
|---|---|
| [00-lifecycle.md](00-lifecycle.md) | Start, stop, reload, and drain procedures. |
| [10-cluster-modes.md](10-cluster-modes.md) | Verify, diagnose, and switch between `strong`, `eventual`, and `full`. |
| [20-purge-ban.md](20-purge-ban.md) | Purge (exact), ban (predicate), and refresh (soft-purge). |
| [30-rolling-restart.md](30-rolling-restart.md) | Zero-5xx rolling restart in a Kubernetes StatefulSet. |
| [40-memory-accounting.md](40-memory-accounting.md) | Interpreting hot_store_bytes vs heap metrics; capturing pprof profiles. |
| [50-warm-disk-exhaustion.md](50-warm-disk-exhaustion.md) | Diagnosing and mitigating warm-tier disk pressure and ENOSPC errors. |

## Naming convention

`NN-topic.md` where `NN` is a two-digit category:

- `00-` — daily ops (start, stop, reload, drain)
- `10-` — capacity & scaling
- `20-` — purge & cache invalidation
- `30-` — cluster operations
- `40-` — memory & observability
- `50-` — incident response
- `90-` — postmortems index
