# Plan: Decouple Hot and Warm Tier Naming

**Status:** Draft
**Scope:** Rename cross-tier semantic coupling in `internal/storage/hot.go` and
`internal/storage/warm/warm.go` to generic concepts so neither package's public
API references the other tier's name. Only `tiered.go` knows both tiers exist.
No behavioural change, no import changes, no lock-ordering changes — pure rename.

---

## 1. Problem Statement

`hot.go` and `warm/warm.go` never import each other — the architectural boundary
is intact. But both packages expose APIs that *name* the other tier:

- `HotStore.SetWarm(key)`, `HotStore.ClearWarm(key)`, `hotEntry.hasWarm`,
  `shard.warmCount`, `evictPreferWarm()`, `evictionLog.hadWarm` — the hot tier
  knows its backup tier is called "warm".
- `warmLoc.hotResident`, `Store.MarkHotResident(key)` — the warm tier knows its
  acceleration tier is called "hot".
- `HotConfig.OnEvict` doc references "warm tier" and "warm-backed".
- `Store.OnEvict` doc references "hot tier" and `ClearWarm`.

This is semantic coupling: the names make the packages unusable in a context
where the backup tier isn't "warm" (e.g. a future cold tier, or a read-replica
acting as the backup). More immediately, it makes the layered architecture
harder to reason about — a reader of `hot.go` sees "warm" everywhere and
reasonably assumes the packages are coupled at the import level.

The fix is a **pure rename**: replace tier-specific names with generic
concepts that describe *what* the flag means, not *which* tier it refers to.

---

## 2. Renaming Scheme

### Hot tier (`hot.go`)

| Current name | New name | Rationale |
|---|---|---|
| `hotEntry.hasWarm` | `hotEntry.hasBackup` | The flag means "this entry has a backup in a slower tier." The eviction policy uses this to prefer evicting backed entries (cheaper — recoverable). Naming the *fact* (has backup) not the *consumer's reasoning* (cheap to evict) keeps the flag reusable for non-eviction purposes (e.g. log metrics). |
| `shard.warmCount` | `shard.backedCount` | Count of entries with a backup. Used by `evictPreferBacked` to skip the scan when zero. |
| `SetWarm(key)` | `SetBacked(key)` | Public API: mark this entry as having a backup. |
| `ClearWarm(key)` | `ClearBacked(key)` | Public API: unmark — the backup is gone. |
| `evictPreferWarm()` | `evictPreferBacked()` | SIEVE sweep preferring backed entries (cheap to evict). |
| `evictionLog.hadWarm` | `evictionLog.hadBackup` | Log field: did the evicted entry have a backup? |
| `OnEvict` field on `HotConfig` | `OnEvict` (unchanged) | The *signature* is already generic (`func(key api.Key)`). The *doc comment* is rewritten by §2 Tiered store (see below) — it is not generic today: it says "warm-backed", `hasWarm`, "warm tier". |
| `notifyEvict` | `notifyEvict` (unchanged) | Already generic internally. |
| Log attr `"had_warm_backup"` | `"had_backup"` | Eviction log field rename (message `"evicted from hot store"`, emitted from `flushEvictionLogs`). Not part of the AGENTS.md §9 access-log field list. |

### Warm tier (`warm/warm.go`)

| Current name | New name | Rationale |
|---|---|---|
| `warmLoc.hotResident` | `warmLoc.protected` | The flag means "don't evict this — it's actively served by a faster tier." It doesn't matter if that tier is "hot", "L0", or "RAM". |
| `MarkHotResident(key)` | `Protect(key)` | Public API: mark this entry as protected from eviction. |
| `OnEvict` field on `warm.Store` | `OnEvict` (unchanged) | The *signature* is already generic (`func(key uint64)`). The *doc comment* is rewritten by §2 Tiered store (see below) — it is not generic today: it says "hot tier", `hasWarm`, `hotResident`, `ClearWarm`. |
| `pickEvictVictim` skip check `candLoc.hotResident` | `candLoc.protected` | Internal field access. |
| `evictOne` doc: "hot-resident" | "protected" | Comment updates. |
| `evictToFit` doc: "hot-resident" | "protected" | Comment updates. |

### Tiered store (`tiered.go`)

| Current call | New call |
|---|---|
| `t.hot.SetWarm(key)` | `t.hot.SetBacked(key)` |
| `t.hot.ClearWarm(key)` | `t.hot.ClearBacked(key)` |
| `t.warm.MarkHotResident(key)` | `t.warm.Protect(key)` |
| `cfg.Hot.OnEvict` doc | Remove "warm tier" references, say "the backup tier". |
| `w.OnEvict` doc | Remove "hot tier" references, say "the acceleration tier". |

### Doc/comments

Every comment that says "warm tier" in `hot.go` or "hot tier" in `warm.go` is
rewritten to use the generic concept or removed if the code is self-evident.
The ADRs (0020, 0023) keep their tier-specific *architectural* language
("hot tier", "warm tier", "L0", "L1") — they describe the *system*. ADR-0023's
*code-symbol* references (`hotResident`, `MarkHotResident`, `hasWarm`,
`ClearWarm`) are updated to the new code names with a one-line mapping note;
see §3 and §6.

---

## 3. Files Changed

| File | Change |
|---|---|
| `internal/storage/hot.go` | Rename: `hasWarm`→`hasBackup`, `warmCount`→`backedCount`, `SetWarm`→`SetBacked`, `ClearWarm`→`ClearBacked`, `evictPreferWarm`→`evictPreferBacked`, `hadWarm`→`hadBackup`, log attr `"had_warm_backup"`→`"had_backup"`, all comments. |
| `internal/storage/hot_test.go` | Update all references: `SetWarm`→`SetBacked`, `hasWarm`→`hasBackup`, `warmCount`→`backedCount`. |
| `internal/storage/hot_logging_test.go` | Update `SetWarm` call (line 77) and `"had_warm_backup"` log field checks (lines 87, 125). |
| `internal/storage/warm/warm.go` | Rename: `hotResident`→`protected`, `MarkHotResident`→`Protect`, all comments referencing "hot". |
| `internal/storage/warm/warm_test.go` | Update all references: `MarkHotResident`→`Protect`, `hotResident`→`protected`. |
| `internal/storage/tiered.go` | Update calls: `SetWarm`→`SetBacked`, `ClearWarm`→`ClearBacked`, `MarkHotResident`→`Protect`, comment rewrites. |
| `internal/storage/tiered_warm_sync_test.go` | Update `SetWarm` call (line 466). |
| `docs/decisions/0023-warm-tier-eviction.md` | Rewrite code-symbol references: `hotResident`→`protected` (5×), `MarkHotResident`→`Protect` (1×), `hasWarm`→`hasBackup` (2×), `ClearWarm`→`ClearBacked` (3×). Keep architectural terms ("hot tier"/"warm tier"). Add one-line mapping note: `Protect` = "mark as hot-resident", `ClearBacked` = "clear warm-backup flag". |

**Total: 8 files, mechanical rename + doc updates.**

---

## 4. What Does NOT Change

- **No import changes.** `hot.go` still doesn't import `warm`, and vice versa.
- **No lock ordering changes.** Same locks, same acquisition order.
- **No behavioural changes.** The flags mean exactly what they meant before.
- **No struct layout changes.** Same fields, same offsets, same sizes.
- **No ADR *architecture-term* changes.** ADRs describe the system architecture
  using tier names ("hot tier", "warm tier", "L0", "L1") and those terms are
  kept. ADR-0023 *code-symbol* references (`hotResident`, `MarkHotResident`,
  `hasWarm`, `ClearWarm`) are updated to the new code names (`protected`,
  `Protect`, `hasBackup`, `ClearBacked`) with a one-line mapping note — see §6.
- **No `OnEvict` callback signature changes.** Already generic (`func(key)`).
- **No `HotConfig` or `warm.Config` struct shape changes** (beyond field renames).
- **No benchmark impact.** Pure rename, no logic change.

---

## 5. Execution Order

This is a single-commit mechanical rename across ~20 symbols in 8 files.
The compiler is the safety net — edit all files, then build once:

1. **Edit all 8 files** — rename all symbols and comments listed in §2 and §3.
2. **`go build ./...`** — fix any missed references the compiler reports.
3. **`go test -race -count=1 -short ./...`** — verify no regressions.
4. **`golangci-lint run`** — verify no lint issues.
5. **Commit** with message:
   `refactor(storage): rename cross-tier flags to generic eviction concepts`

---

## 6. Risks and Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Missed reference causes build break | Low | `go build ./...` after all edits. Compiler catches every missed reference in one pass. |
| Log consumers depend on `"had_warm_backup"` field name | Low | This is an **eviction log** field (message `"evicted from hot store"`, emitted from `flushEvictionLogs`) — not part of the AGENTS.md §9 access-log field list. The rename is in the same PR. Verified: no consumer outside `internal/storage/` reads the field (only `hot.go:113` emits, `hot_logging_test.go:87,125` asserts). |
| ADR-0023 references renamed code symbols | Certain | ADR-0023 §Algorithm and §Callback reference **four** code symbols: `hotResident` (5×), `MarkHotResident` (1×), `hasWarm` (2×), `ClearWarm` (3×). Rewrite each reference to the new code name (`protected`, `Protect`, `hasBackup`, `ClearBacked`), keeping the architectural terms ("hot tier" / "warm tier") where the ADR is describing the system rather than naming a field. Add a one-line mapping note in ADR-0023: `Protect` = "mark as hot-resident", `ClearBacked` = "clear warm-backup flag". Do not leave stale references behind. |
| `depguard` or `lint` complains | Very low | No import changes. |

---

## 7. Out of Scope

- **`maxWarmEvictSkips` unchanged.** It names the warm tier's *own* skip budget
  (mirrors the hot tier's `maxEvictSkips`), not a cross-tier reference. Renaming
  it would break the intra-tier naming symmetry with `maxEvictSkips`.
- Splitting `tiered.go` into a separate `tiered_evict.go` — not needed.
- Adding an interface between tiered and hot/warm — the callbacks are already
  the interface; adding a formal interface would add indirection with no
  current consumer.
- Renaming the packages themselves (`hot`→`l0`, `warm`→`l1`) — that's a
  repository-wide rename with import cycle implications, not justified by
  this refactor alone.
- Adding a third tier — not on the current roadmap.
- Changing `OnEvict` to a channel or observer pattern — the callback is
  already generic and performant; no reason to change.
