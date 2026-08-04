# 50 — Warm-tier disk exhaustion

How to diagnose and mitigate warm-tier (L1 / disk) disk pressure and
ENOSPC errors.

---

## Overview

The warm tier stores cache objects on disk via mmap-backed segment files.
It has two budgets:

| Budget | Config field | What it limits | Check |
|--------|-------------|----------------|-------|
| Logical bytes | `warm_max_bytes` | Live entry bytes in the index | `OverBudget` — `stats.bytes >= maxBytes` |
| Physical disk | `warm_max_disk_bytes` | Total segment file size (live + dead) | `DiskOverBudget` — `diskBytes() > maxDiskBytes` |
| Free space | `min_free_disk` | Minimum free disk space on the filesystem | `DiskOverBudget` — `freeBytes < minFreeDisk` |

When the logical budget is exceeded, SIEVE eviction runs to reclaim space
from cold entries. If all entries are protected (also in the hot tier),
`Put` rejects with `ErrOverBudget`. When the physical disk budget or free
space threshold is exceeded, `compactLoop` triggers immediate compaction.

Config changes to these budgets require a pod restart — the
`POST /v1/config/reload` endpoint is currently a stub
(`internal/admin/server.go:493`).

---

## Symptoms

### Logs

| Message | Level | Source | Meaning |
|---------|-------|--------|---------|
| `warm tier over budget, skipping warm write` | warn | data-plane `Put` (`tiered.go:468`) | Warm rejected the write; hot tier still serves the object |
| `warm sync: warm put over budget, stopping promotion` | info | warm sync loop (`tiered.go:921`) | Background hot→warm sync stopped for this cycle |

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `bouine_warm_over_budget_total` | counter | `Put` rejections due to `ErrOverBudget` |
| `bouine_warm_disk_bytes` | gauge | Total on-disk size of all segment files (live + dead) |
| `bouine_warm_max_bytes` | gauge | Configured `warm_max_bytes` (0 = unlimited) |
| `bouine_warm_evictions_total` | counter | Entries evicted by SIEVE since boot |
| `bouine_warm_compaction_triggered_total` | counter | `Compact()` calls since boot |

### Dashboard

The `ruleCacheWarmNearFull` insight fires at:
- ≥ 90% — medium severity
- ≥ 95% — high severity

### OS-level

- `ENOSPC` / `no space left on device` in bouine logs, dmesg, or node logs.
- This means both bouine budgets and the filesystem are full — the
  budgets were either misconfigured (too high) or not set.

---

## Diagnosis

### Step 1: Check the budget ratio

```promql
bouine_warm_disk_bytes / bouine_warm_max_bytes
```

If `> 0.9`, the warm tier is near its logical budget. If
`bouine_warm_max_bytes` is `0`, the budget is unlimited — skip to
checking disk-level metrics.

### Step 2: Check if eviction is working

```promql
rate(bouine_warm_evictions_total[5m])
```

- Non-zero rate = eviction is active (healthy under pressure).
- Zero rate + rising `bouine_warm_over_budget_total` = all entries are
  protected (also in hot tier). Eviction cannot reclaim space.

### Step 3: Check if compaction is triggering

```promql
rate(bouine_warm_compaction_triggered_total[5m])
```

If `bouine_warm_disk_bytes` is high but compaction isn't triggering, the
dead-space ratio may be below the compaction threshold (default: 30%
waste). `DiskOverBudget` should trigger compaction immediately if
`warm_max_disk_bytes` or `min_free_disk` is set.

### Step 4: Distinguish logical vs physical pressure

- `OverBudget` (logical): `stats.bytes >= warm_max_bytes`. Live entries
  fill the budget. Eviction or budget increase needed.
- `DiskOverBudget` (physical): `diskBytes() > warm_max_disk_bytes` or
  `freeBytes < min_free_disk`. Segment files (including tombstones)
  fill the disk. Compaction needed.

`bouine_warm_disk_bytes` includes dead bytes (tombstones, superseded
keys). `stats.bytes` (used by `OverBudget`) counts only live entries.
A large gap between the two means compaction is overdue.

---

## Immediate mitigation

1. **Increase PVC size** — `kubectl edit pvc <pvc-name>` or update Helm
   `persistence.size` and roll the StatefulSet. Gives immediate headroom.

2. **Lower `warm_max_bytes`** — reduces the logical budget, forcing
   earlier eviction. Edit the config and restart the pod (config reload
   is not implemented yet).

3. **Set `warm_max_disk_bytes` or `min_free_disk`** — triggers
   compaction sooner via `DiskOverBudget` in `compactLoop`. Requires
   pod restart.

4. **Reduce hot-tier overlap** — if eviction is thrashing (all warm
   entries protected), lowering `hot_max_entries` reduces the number of
   warm entries marked as protected. Requires pod restart.

5. **Wait for compaction** — the `compactLoop` runs every
   `compact_interval` (default 30 min) and triggers immediately when
   `DiskOverBudget` returns true. There is no manual compaction API.

---

## Long-term

- Set `warm_max_bytes` and `warm_max_disk_bytes` to ≤ 80% of PVC size.
- Set `min_free_disk` to a safety margin (e.g. 5 GiB) to catch
  external disk growth (orphaned PVCs, other workloads).
- Verify `bouine_warm_evictions_total` is non-zero under sustained
  write pressure — if it's always zero, either the working set fits
  in the budget or all entries are protected (investigate hot-tier
  overlap).
- Monitor the eviction-to-over-budget ratio:
  - High eviction + low over-budget = healthy (eviction working).
  - Low eviction + high over-budget = all entries protected
    (reduce `hot_max_entries` or increase `warm_max_bytes`).
- Track open issues: #206 (data-plane backpressure), #207
  (`NeedsCompaction` disk-pressure gate).

---

## Monitoring

| Alert | Expression | Duration | Severity |
|-------|-----------|----------|----------|
| Warm near full | `bouine_warm_disk_bytes / bouine_warm_max_bytes > 0.9` | 5 min | warning |
| Warm rejecting writes | `rate(bouine_warm_over_budget_total[10m]) > 0` | 10 min | warning |
| Eviction pressure | `rate(bouine_warm_evictions_total[5m]) > 2 * baseline` | 5 min | info |

---

## Related

- [ADR-0020](../decisions/0020-hot-to-warm-sync.md) — Hot→warm background sync
- [ADR-0023](../decisions/0023-warm-tier-eviction.md) — SIEVE eviction for warm tier
- [ADR-0026](../decisions/0026-sieve-sweep-cap.md) — SIEVE eviction worst-case bound
- Parent issue: [#202](https://github.com/bouine-cache/bouine/issues/202)
- Open sub-issues: [#206](https://github.com/bouine-cache/bouine/issues/206), [#207](https://github.com/bouine-cache/bouine/issues/207)
