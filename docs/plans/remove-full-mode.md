# Plan: Remove `full` cluster mode

## Motivation

`full` mode does not scale. Memory grows linearly with cluster size (N× working set), bandwidth is `fills/s × avg_size × (N-1)` per node, and the anti-entropy reconciler (472 lines) exists only to repair a replication mechanism that wouldn't exist without the mode. It caused production pod restarts at 150 RPS due to gossip queue overflow. `strong` with `replicas >= 2` provides the same HA guarantee at 1/N the memory and near-zero bandwidth.

## Scope

Remove `full` as a valid `cluster.mode` value and every piece of code, config, tests, docs, and dashboard UI that exists solely to support it. Keep `strong` and `eventual` unchanged.

## Config compatibility

- `cluster.mode: full` in YAML → `config.Validate` returns an error with a migration message pointing to `strong` + `replicas: 2` or `eventual`.
- Remove 4 config fields: `anti_entropy_interval`, `backfill_limit`, `backfill_cooldown`, `churn_threshold`.
- Remove `ClusterModeFull` constant.

---

## Phase 1 — Config & validation (no runtime behavior change)

### 1.1 `internal/config/config.go`
- Delete `ClusterModeFull = "full"` constant (L121) and its doc comment (L119–120).
- Delete the `"full"` line from the `Mode` field doc comment (L134).
- Delete `AntiEntropyInterval` field + doc (L140–143).
- Delete `BackfillLimit` field + doc (L144–149).
- Delete `BackfillCooldown` field + doc (L150–158).
- Delete `ChurnThreshold` field + doc (L159–172).

### 1.2 `internal/config/loader.go`
- Remove `ClusterModeFull` from the valid-modes switch case (L309) and error message (L315).
- Remove `BackfillCooldown` validation (L323–324).
- Remove `ChurnThreshold` validation (L326–327).

### 1.3 `internal/config/loader_test.go`
- Remove `ClusterModeFull` from the valid-modes table test (L183).
- Delete `TestCluster_BackfillCooldown_ParsesFromYAML` (L550–565).
- Delete `TestCluster_BackfillCooldown_DefaultsToZero` (L569–576).
- Delete `TestCluster_BackfillCooldown_NegativeRejected` (L580–591).
- Delete `TestCluster_ChurnThreshold_ParsesFromYAML` (L595–610).
- Delete `TestCluster_ChurnThreshold_DefaultsToZero` (L614–621).
- Delete `TestCluster_ChurnThreshold_OutOfRangeRejected` (L625–637).
- Delete `TestCluster_ChurnThreshold_BoundariesAccepted` (L642–651).
- Add a test: `cluster.mode: full` returns a validation error mentioning the migration path.

---

## Phase 2 — Cluster layer (core removal)

### 2.1 Delete `internal/cluster/antientropy.go`
- Delete the entire file (472 lines): `AntiEntropyConfig`, `KeySource`, `Backfiller`, `Storer`, `AntiEntropy`, `NewAntiEntropy`, `Start`, `loop`, `reconcile`, `reconcileWithPeer`, `detectChurn`, `pruneCooldown`, `inCooldown`, `recordBackfill`, `missingKeys`, `fetchPeerKeys`, `backfillKey`, `PeerKeysHandler`, `NewPeerKeysHandler`, `PeerKeysPath`, `keysToUint64`.

### 2.2 Delete `internal/cluster/antientropy_test.go`
- Delete the entire file (~1063 lines).

### 2.3 `internal/cluster/broadcast.go`
- Remove `replSem` field from `Broadcaster` struct (L44–45).
- Remove `replClient` field (L46).
- Remove their initialization in `NewBroadcaster` (L65–66).
- Remove `config.ClusterModeFull` from `BroadcastPurge` condition (L85) — keep only `config.ClusterModeStrong`.
- Remove `config.ClusterModeFull` from `BroadcastBan` condition (L143) — keep only `config.ClusterModeStrong`.
- Delete `BroadcastReplicate` method (L185–286).
- Delete `sendReplicate` method (L289–320).
- Remove the `storage` import if no longer used after deleting `BroadcastReplicate`.

### 2.4 `internal/cluster/broadcast_test.go`
- Delete `TestBroadcastReplicate_Full_SendsHTTP` (L165–190).
- Delete `TestBroadcastReplicate_Eventual_Noop` (L226–247).
- Delete `TestBroadcastReplicate_Strong_Noop` (L251–272).
- Delete `TestBroadcastReplicate_Full_BodyCopiedNotAliased` (L276–330).
- Update any purge/ban tests that set `mode: "full"` to use `"strong"` or `"eventual"` instead.

### 2.5 `internal/cluster/handlers.go`
- Delete `NewPeerReplicateHandler` function (L64–101).

### 2.6 `internal/cluster/handlers_test.go`
- Delete `TestPeerReplicateHandler_DecodesAndStores` (L16–58).
- Delete `TestPeerReplicateHandler_BadBody` (L61–80).
- Delete `TestPeerReplicateHandler_StoreError` (L82–101).

### 2.7 `internal/cluster/cluster.go`
- Delete `Replicator` struct (L69–73).
- Delete `rep` field on `Cluster` struct (L98).
- Delete `SetReplicator` method (L578–580).
- Update `Mode()` doc comment (L567) to remove `"full"` from the list.
- Update package doc comment (L47) to remove the `full` mode description.
- Remove `ClusterModeFull` from any import of `config` if it was referenced.

### 2.8 `internal/cluster/cluster_test.go`
- Remove all `SetReplicator` calls (L213, L247, L369).
- Remove or update tests that rely on `Replicator` / `SetReplicator`.

### 2.9 `internal/cluster/codec.go`
- Delete `EncodeKeySet` function (L68–90).
- Delete `DecodeKeySet` function (L92–118).
- Remove the `binaryMagic` / `keySetMagic` constants if only used by KeySet codec.

### 2.10 `internal/cluster/codec_test.go`
- Delete `TestEncodeDecodeKeySet_RoundTrip` (L10–30).
- Delete `TestEncodeKeySet_EmptyKeys` (L34–48).
- Delete `TestEncodeKeySet_EmptyNodeName` (L52–66).
- Delete `TestDecodeKeySet_BadMagic` (L70–76).
- Delete `TestDecodeKeySet_ShortFrame` (L80–84).

### 2.11 `internal/cluster/metrics.go`
- Delete `ReplicationsSent` field + comment (L25–27).
- Delete `ReplicationsReceived` field + comment (L28–30).
- Delete `ReplicationsDropped` field + comment (L31–35).
- Delete `ReplicationBytes` field + comment (L36–38).
- Delete `OnReplicationBytes` callback field (L62–66).
- Delete their initialization in `NewMetrics` (L96–113, L127–130).
- Delete `initAntiEntropyMetrics` method (L142–174).
- Delete anti-entropy metric fields: `AntiEntropyReconcile`, `AntiEntropyRepaired`, `AntiEntropyKeysRepaired`, `AntiEntropyFetchFailures`, `AntiEntropyCooldownSkips`, `AntiEntropyChurnSkips` (L43–61).
- Delete their registration in `NewMetrics` (L132–137).
- Delete incrementer methods: `IncReplicationSent`, `IncReplicationReceived`, `IncReplicationDropped`, `AddReplicationBytes`, `IncAntiEntropyReconcile`, `IncAntiEntropyRepaired`, `SetAntiEntropyKeysRepaired`, `IncAntiEntropyFetchFailure`, `AddAntiEntropyCooldownSkips`, `IncAntiEntropyChurnSkip` (L212–320).
- Update mode gauge: remove `"full"` from the label list (L183) — keep `["strong", "eventual"]`.
- Update help text for `cluster_mode_info` (L84) to remove "full".

### 2.12 `internal/cluster/peerfetch.go`
- Remove the anti-entropy comment at L42 (replace with a simpler comment if the context is still relevant for miss fan-out).

### 2.13 `pkg/api/cluster.go`
- Delete `KeySet` struct (L109–114) and its doc comment.

### 2.14 `pkg/header/header.go`
- Delete `BouineIssuer` constant + doc (L209–210).
- Delete `BouineSeq` constant + doc (L211–214).
- Delete `BouineIssuedAt` constant + doc (L215–218).
- Delete `BouineMethod` constant + doc (L219–222).
- (These are only used by `sendReplicate`. Verify no other consumers first.)

---

## Phase 3 — Admin server

### 3.1 `internal/admin/server.go`
- Delete `PeerReplicateHandler` field from `AdminServerConfig` (L84–87).
- Delete `PeerKeysHandler` field (L88–91).
- Delete the `POST /v1/peer/replicate` mux registration (L222–223).
- Delete the `GET /v1/peer/keys` mux registration (L225–226).
- Remove both paths from the `noAuthPaths` set (L509, L514).

---

## Phase 4 — Cache handler

### 4.1 `internal/cache/handler.go`
- Delete `replicateFn` field (L246–249).
- Delete `ReplicateFn` field from `HandlerConfig` (L331–334).
- Remove `replicateFn: cfg.ReplicateFn` assignment in constructor (L383).
- Simplify `storeAndReplicate` to just `storeAndStore` (or inline): remove the `replicateFn` call (L1341–1358). Rename method to `storeCached` or similar since it no longer replicates. Or simply remove the replicate branch and keep the rest.
- Update all call sites of `storeAndReplicate` — they pass `isRefresh` and `r` which were only used for replication. Simplify the signature if possible.

### 4.2 `internal/cache/handler_test.go`
- Remove `ReplicateFn` from all test handler configs (L792–841, L965–985, L1015–1035, L1069–1108, L1105–1146).
- Remove "full mode" references in test comments.
- Remove the `"full-mode-body"` string literal (L805) — use a generic name.

### 4.3 `internal/cache/refresh_registry.go`
- Remove the "storeAndReplicate" reference in the doc comment (L27) — update to reflect the new method name.

---

## Phase 5 — Engine & builder wiring

### 5.1 `cmd/bouine/cmd/engine.go`
- Delete `antiEntropy` field on `engine` struct (L69).
- Delete the `if e.cfg.Cluster.Mode == config.ClusterModeFull` block that sets up Replicator and AntiEntropy (L265–280).
- Delete `initAntiEntropy` method (L286–313).
- Delete `buildPeerReplicateHandler` method (L524–538).
- Delete `buildPeerKeysHandler` method (L515–522).
- Remove `PeerReplicateHandler` from admin config construction (L505).
- Remove `PeerKeysHandler` from admin config construction (L507).
- Remove `clusterMetrics.OnReplicationBytes` callback (L246) and the dashboard handler it feeds.
- Remove the anti-entropy mention in the background tasks comment (L98).

### 5.2 `cmd/bouine/cmd/builder.go`
- Delete the `ClusterModeFull` guard + `ReplicateFn` assignment for proxy routes (L242–243).
- Delete the same for static routes (L314–315).
- Remove the full-mode mention in the builder doc comment (L178).

---

## Phase 6 — Dashboard

### 6.1 `internal/dashboard/handler.go`
- Delete `replicationStats` method (L585–end).
- Remove `Replication: h.replicationStats(...)` from the cluster view model (L574).
- Remove `OnReplicationBytes` callback usage if still referenced.

### 6.2 `internal/dashboard/templates/models.go`
- Delete `ReplicationStats` struct (L261–272).
- Delete `Replication` field from `ClusterData` (L286).
- Delete `ReplicationLastRecv` and `ReplicationBytes` fields from `InsightData` (L70–71 in engine.go, not models.go — check actual location).
- Remove `"full"` from `modeHint` string (L434) and the `case "full"` in the mode-label switch (L846–847).
- Update doc comments that list "full" as a valid mode (L89, L249).

### 6.3 `internal/dashboard/insights/engine.go`
- Delete `ReplicationLastRecv` field (L70).
- Delete `ReplicationBytes` field (L71).

### 6.4 `internal/dashboard/insights/rules.go`
- Delete `ruleClusterFullModeMemory` function (L616–628).
- Delete `ruleConfigAntiEntropyDisabled` function (L781–796).
- Delete `ruleClusterReplicationStalled` function (L1123–1141).
- Delete `ruleClusterNoReplicationTraffic` function (L1146–1159).
- Remove their registrations from the rule list (L35, L44, L59, L60).

### 6.5 `internal/dashboard/insights/engine_test.go`
- Delete `TestRuleConfigAntiEntropyDisabled` (L502–526).
- Delete `TestRuleClusterReplicationStalled` (L814–844).
- Delete `TestRuleClusterNoReplicationTraffic` (L848–868).

### 6.6 `internal/dashboard/templates/overview.templ`
- Remove the `full` mode branch (L200–201).

### 6.7 `internal/dashboard/templates/cluster.templ`
- Remove all `full` mode branches: L21–22, L72, L80–81, L135–140, L159, L284.
- Remove the replication throughput chart section entirely.
- Remove `replicationChartScript` function.

### 6.8 Regenerate templ output
- Run `make templ` (or `go generate ./internal/dashboard/templates/`) to regenerate `overview_templ.go` and `cluster_templ.go`.
- Commit both `.templ` and `_templ.go` files.

---

## Phase 7 — Storage layer (comment cleanup only)

### 7.1 `internal/storage/store.go`
- Update `KeyLister` interface doc (L36–37) — remove "consumed by the anti-entropy reconciler in full cluster mode."
- **Keep `KeyLister` interface itself** — it may be useful for future features or debugging. Removing it is optional; keeping it is zero-cost.

### 7.2 `internal/storage/hot.go`
- Update comment at L617 — remove "Used by anti-entropy" reference.
- Update comment at L679 — remove "anti-entropy reconciler in full cluster mode" reference.

### 7.3 `internal/storage/tiered.go`
- Update comment at L417 — remove "anti-entropy reconciler in full cluster mode" reference.

### 7.4 `internal/storage/warm/warm.go`
- Update comment at L640 — remove "anti-entropy" reference.

### 7.5 `internal/storage/codec_test.go`
- Update comment at L97 — remove "anti-entropy checksums" reference.

### 7.6 `internal/storage/tiered_test.go`
- Update comment at L410 — remove anti-entropy reference.
- `TestTieredStore_ImplementsKeyLister` (L502–520) — keep or remove. Keeping is fine (it's a valid interface assertion test).

---

## Phase 8 — Integration tests

### 8.1 Delete `test/integration/cluster_full_test.go`
- Delete the entire file (215 lines, 8 test functions).

### 8.2 `test/integration/cluster_common_test.go`
- Remove `"full"` from `clusterModes` slice (L12) — keep `["strong", "eventual"]`.
- Remove the `if mode == "full"` special-casing (L32, L39).

### 8.3 `Makefile`
- Remove `test-integration-cluster-full` target (L86–87).
- Remove `test-integration-cluster-full` from the `test-integration-cluster` dependency list (L72).

---

## Phase 9 — Helm

### 9.1 `deploy/helm/bouine/values.yaml`
- Update the HPA comment (L154–156) — remove "full cluster mode" reference. Replace with a generic comment about CPU-based HPA.

---

## Phase 10 — Internal docs (bouine repo)

### 10.1 `docs/architecture.md`
- Remove the `full` mode paragraph (L269–274) — the anti-entropy description.
- Update the failure-mode table (L528) — remove "anti-entropy reconciler" from the purge split-brain mitigation (keep "monotonic purge tokens").
- Update §16.4 or any section that lists three modes — reduce to two.

### 10.2 `docs/runbook/10-cluster-modes.md`
- Delete the entire `### full` section (L77–109).
- Delete `full` column from the per-mode comparison tables.
- Delete `full` mode row from the memory/bandwidth budget section (L130–143).
- Delete `full`-related rows from the switching modes section (L149–172).
- Delete `full`-related alerts: `FullReplicationStalled`, `FullModeMemoryPressure` (L186–205).
- Delete `full`-related rows from the troubleshooting quick reference table (L209–218).

### 10.3 `docs/runbook/20-purge-ban.md`
- Remove `full` from any purge/ban propagation tables (L244–246).

### 10.4 `docs/runbook/static-files.md`
- Remove the "full mode" mention at L48.

### 10.5 ADRs

#### 10.5.1 `docs/decisions/0008-cluster-mode-local-cache-gossip-invalidation.md`
- Add a superseded note at the top: full mode removed in [date]. Point to ADR-0025 (new ADR for this removal, see Phase 11).
- Strike through or annotate full-mode sections. Do not delete the historical content — ADRs are immutable records.

#### 10.5.2 `docs/decisions/0014-anti-entropy-full-mode.md`
- Mark as **Superseded** — add status change at the top. Link to ADR-0025.

#### 10.5.3 `docs/decisions/0015-binary-cluster-wire-format.md`
- Remove `full` mode references from the replication codec description. Update to note that replication was removed.
- If the codec is still used by peer-fetch, keep the peer-fetch parts and remove only the replication-specific paragraphs.

#### 10.5.4 `docs/decisions/0018-backfill-cooldown.md`
- Mark as **Superseded** — the feature it described no longer exists.

#### 10.5.5 `docs/decisions/0019-antientropy-churn-detection.md`
- Mark as **Superseded** — the feature it described no longer exists.

#### 10.5.6 `docs/decisions/README.md`
- Update the ADR index: mark 0014, 0018, 0019 as superseded. Add entry for the new ADR-0025.

#### 10.5.7 `docs/decisions/0020-rps-based-hpa-autoscaling.md`
- Remove `full` mode mention (L19, L24).

#### 10.5.8 `docs/decisions/0020-hot-to-warm-sync.md`
- Remove `full` mode mention (L120).

#### 10.5.9 `docs/decisions/0024-async-wal-fsync.md`
- Remove `full` mode mention (L111).

### 10.6 Plans

#### 10.6.1 `docs/plans/fix-full-mode-gossip-overflow.md`
- Delete the file — it describes fixing a feature that no longer exists.

#### 10.6.2 `docs/plans/cluster-local-cache-mode.md`
- If this plan is fully implemented, delete it. If it still has open items for `eventual` mode, update it to remove all `full` references.

### 10.7 Create new ADR: `docs/decisions/0025-remove-full-cluster-mode.md`

```markdown
# ADR-0025: Remove full cluster mode

- **Status**: Accepted
- **Date**: [date]
- **Deciders**: @thylong

## Context

Full cluster mode replicated every cached object to all peers, giving each
node a complete copy of the working set. This did not scale: memory grew
linearly with cluster size (N× working set), bandwidth was
`fills/s × avg_size × (N-1)` per node, and the anti-entropy reconciler
(472 lines) existed only to repair dropped replications. The mode caused
production pod restarts at 150 RPS due to gossip queue overflow, requiring
a migration from gossip to HTTP POSTs for replication transport.

## Decision

Remove `full` as a valid `cluster.mode` value. Users who need redundancy
should use `strong` with `replicas >= 2` (same HA guarantee, 1/N memory,
near-zero bandwidth). Users who need independent caching should use
`eventual`.

## Consequences

- `cluster.mode: full` is rejected at config validation time with a
  migration message.
- 4 config fields removed: `anti_entropy_interval`, `backfill_limit`,
  `backfill_cooldown`, `churn_threshold`.
- ~2000 lines of code removed (antientropy.go, replication transport,
  handlers, tests, dashboard UI, insights rules).
- ADRs 0014, 0018, 0019 superseded.

## Migration

- `full` → `strong` + `replicas: 2`: same redundancy, fraction of memory.
- `full` → `eventual`: zero cross-node bandwidth, independent caching.
```

---

## Phase 11 — bouine-documentation repo (`../bouine-documentation`)

### 11.1 `content/docs/configuration/cluster-modes.md`
- Remove `full` from the mode list at the top (L14).
- Remove the "Small cluster (2–5 nodes)" bullet that recommends `full` (L21).
- Remove the `full` column from the comparison table (L23–31).
- Delete the entire `## Full mode` section (L122–147).
- Remove `full` column from the invalidation propagation table (L153–157).
- Update the anti-entropy mention at L97 — it described ring digest exchange in strong mode, which stays. Remove any `full` mode anti-entropy references.

### 11.2 `content/docs/operations/cluster-modes.md`
- Delete the `### full` section (L74–106).
- Delete `full` mode from the memory/bandwidth budget section (L122–143).
- Remove `full` from switching modes section (L149–172).
- Delete `FullReplicationStalled` and `FullModeMemoryPressure` alerts (L186–205).
- Remove `full` rows from the troubleshooting table (L209–218).

### 11.3 `content/docs/operations/troubleshooting.md`
- Remove "Memory pressure in full mode" from the quick reference table (L17).
- Remove `full` from the "strong or full mode" purge propagation section (L171) — change to "strong mode".
- Delete the "In `full` mode" stale reads bullet (L187).
- Delete the "Memory pressure in `full` mode" section (L191–204).
- Update "Consider `strong` or `full` mode" (L210) to "Consider `strong` mode".

### 11.4 `content/docs/architecture/_index.md`
- Delete the "Full mode" subsection (L78–80).
- Remove the `full` column from the invalidation propagation table (L104–110).

### 11.5 `content/docs/guides/capacity-planning.md`
- Remove the `full` row from the cluster mode table (L30).
- Update the hot tier sizing formula (L44) — remove "or full mode" from the eventual/full comment.
- Delete the "Full mode bandwidth budget" section (L113–127).

### 11.6 `content/docs/operations/monitoring.md`
- Remove `full` from the mode column of the cluster metrics table (L62–67) — keep only `strong` / `eventual` rows.
- Delete `bouine_cluster_replications_sent_total`, `bouine_cluster_replications_received_total`, `bouine_cluster_replication_bytes_total` rows (these metrics no longer exist).
- Delete the `BouineFullReplicationStalled` alert (L255–262).

### 11.7 `content/docs/configuration/static-files.md`
- Remove "full mode" reference at L67 — reword to "cluster peers".
- Remove "cluster replication" mention at L87 — reword to "caching features" without replication.

### 11.8 `content/docs/getting-started/first-cache.md`
- Remove "cluster replication" mention at L107–108 — reword to "caching or TTL-based eviction".

### 11.9 `content/docs/getting-started/examples.md`
- Remove "cluster replication" mention at L75–76 — reword to "TTL-based eviction".

### 11.10 `content/docs/configuration/helm.md`
- No changes needed (L60 already only mentions strong mode).

### 11.11 Delete `layouts/shortcodes/full-mode-diagram.html`
- Delete the entire file — the shortcode is only used by the full mode section which is being removed.

### 11.12 Remove shortcode reference
- In `content/docs/configuration/cluster-modes.md`, remove the `{{< full-mode-diagram >}}` shortcode usage (within the deleted Full mode section — already handled in 11.1).

---

## Phase 12 — Verification

### 12.1 Build
```bash
make build
```

### 12.2 Lint
```bash
make lint
```

### 12.3 Unit tests
```bash
make test
```

### 12.4 Integration tests
```bash
make integration
```
Verify: only `strong` and `eventual` modes are tested. No `full` mode tests run.

### 12.5 Conformance
```bash
make conformance
```

### 12.6 Check for stale references
```bash
rg -i "full.*mode|ClusterModeFull|anti.entropy|backfill|churn_threshold|BroadcastReplicate|PeerReplicateHandler|PeerKeysHandler|ReplicateFn|replicateFn|ReplicationsSent|ReplicationsReceived|ReplicationsDropped|ReplicationBytes|SetReplicator|KeySet|EncodeKeySet|DecodeKeySet|replSem|replClient" \
  --type go --type yaml --type md \
  -g '!docs/decisions/0008*' -g '!docs/decisions/0014*' -g '!docs/decisions/0018*' -g '!docs/decisions/0019*' \
  internal/ cmd/ pkg/ test/ deploy/ docs/ Makefile
```
Expected: zero matches outside the superseded ADRs.

### 12.7 Templ generation
```bash
make templ
git diff --stat  # verify _templ.go files changed
```

### 12.8 prek hooks
```bash
prek run --all-files
```

---

## Execution order

The phases are ordered to keep the build green at every step:

1. **Phase 1** (config) — makes `full` invalid. Build still compiles (no one references the removed constant yet because phases 2–5 still have the code). Actually, the removed constant IS referenced — so phases 1–5 must be done together before compiling. Do phases 1–5 as one atomic change.
2. **Phases 2–5** (cluster, admin, cache, engine) — remove all runtime code.
3. **Phase 6** (dashboard) — remove UI.
4. **Phase 7** (storage comments) — cosmetic.
5. **Phase 8** (integration tests) — remove tests.
6. **Phase 9** (Helm) — comment fix.
7. **Phase 10** (internal docs) — ADRs, runbooks, architecture.
8. **Phase 11** (bouine-documentation) — external docs.
9. **Phase 12** (verification) — full test suite.

**Commit strategy**: One PR with logical commits:
1. `refactor: remove full cluster mode and all related code`
2. `test: remove full-mode integration and unit tests`
3. `docs: remove full mode from internal docs and ADRs`
4. `docs: remove full mode from bouine-documentation`

Or a single commit if the diff is manageable (≤ 400 lines of net change is unlikely given the scope — this is a deletion-heavy change, so net lines should be strongly negative).
