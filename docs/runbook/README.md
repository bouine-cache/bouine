# Operator runbook

This directory holds short, action-oriented documents that operators
read at 3 AM. Each runbook addresses a specific failure mode or
operator task. Per `AGENTS.md §10`, runbooks are updated alongside the
code change that introduces a new failure mode.

## Available runbooks

| File | Topic |
|------|-------|
| [`00-lifecycle.md`](00-lifecycle.md) | Start, stop, reload, drain. |
| [`10-cluster-modes.md`](10-cluster-modes.md) | Cluster modes (strong, eventual, full) and capacity. |
| [`20-purge-ban.md`](20-purge-ban.md) | Purge and ban invalidation. |
| [`30-rolling-restart.md`](30-rolling-restart.md) | Rolling restarts and cluster operations. |
| [`40-memory-accounting.md`](40-memory-accounting.md) | Memory accounting and tuning. |
| [`50-warm-disk-exhaustion.md`](50-warm-disk-exhaustion.md) | Warm-disk exhaustion and incident response. |
| [`static-files.md`](static-files.md) | Static file serving. |
