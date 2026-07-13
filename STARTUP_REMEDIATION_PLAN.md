# Remediation Plan: Startup Path Under Load with Full Warm Tier

Based on `STARTUP_INVESTIGATION.md` and `STARTUP_LINUS_REVIEW.md`.

Cross-referenced every review finding against the plan. Gaps found and
addressed below are marked **[review-gap]**.

## Problem

StatefulSet rollout in strong mode under heavy load with a warm tier filled
with millions of keys causes containers to restart repeatedly
(CrashLoopBackOff) and take a very long time to answer probes.

## Root Causes (cross-referenced with review findings)

| # | Root Cause | Review Finding | Severity | Effort |
|---|-----------|----------------|----------|--------|
| RC-1 | Synchronous warm-tier loading blocks all probe endpoints | BLOCKER-1 | BLOCKER | M |
| RC-2 | RecomputeStats mandatory double-scan (WAL lacks record size) | BLOCKER-2 | BLOCKER | M |
| RC-3 | No `podManagementPolicy: Parallel` — serialized rollout | BLOCKER-4 | BLOCKER | S |
| RC-4 | `readyz` is a sham — `IsReady()` starts true, checks nothing | BLOCKER-3 | BLOCKER | M |
| RC-5 | RecomputeStats doubles index memory under RLock | bug-5 | bug | S |
| RC-6 | No persistent on-disk index — full rebuild every restart | — | taste | L |
| RC-7 | Cluster join is fire-and-forget, returns nil on give-up | bug-6 | bug | M |
| RC-8 | `compactLoop` starts immediately after loading | taste-7 | taste | S |
| RC-9 | `startupProbe` comment names the wrong bottleneck | bullshit-8 | bullshit | S |
| RC-10 | `preStop` hook duration hardcoded from readiness probe config | nit-9 | nit | S |
| RC-11 | Doc comments lie about what the code does | BLOCKER-2, BLOCKER-3 | bullshit | S |

---

## Phase 1: Stop the Bleeding (chart + config changes, no code)

**Goal:** Prevent CrashLoopBackOff and reduce rollout time. Deployable in
minutes via Helm values override.

### 1.1 Add `podManagementPolicy: Parallel` to the StatefulSet

**File:** `deploy/helm/bouine/templates/statefulset.yaml`

```yaml
spec:
  podManagementPolicy: Parallel
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
  ...
```

All pods start simultaneously. In strong mode, they join the cluster
concurrently via gossip. The ring converges within seconds, and traffic is
distributed from the start. No more 3× serialization.

**Why `maxUnavailable: 1` is required:** `podManagementPolicy: Parallel`
affects both initial creation and rolling updates. Without
`maxUnavailable: 1`, a rolling update would restart all pods at once,
causing a full outage. With it, K8s still rolls one pod at a time during
updates — `Parallel` only removes the "wait for pod-N to be Ready before
starting pod-N+1" ordering constraint. This is the correct tradeoff: parallel
initial boot, sequential rolling updates.

### 1.2 Fix `startupProbe` budget and comment

**File:** `deploy/helm/bouine/values.yaml`

**[review-gap]** The review (bullshit-8) calls out that the comment `# 5
minutes max warmup for anti-entropy` names the wrong bottleneck. The actual
bottleneck is WAL replay + RecomputeStats segment scan, not anti-entropy
(which is async and non-blocking). Fix the comment and increase the budget:

```yaml
startupProbe:
  httpGet:
    path: /readyz
    port: admin
  periodSeconds: 10      # was 5 — less aggressive polling
  failureThreshold: 180  # was 60 — 30 minutes max warmup
  # Budget for warm-tier WAL replay + RecomputeStats segment scan.
  # Scale with warm-tier size: ~1 min per 10 GB of segment data at
  # 10 GiB/s mmap throughput under typical container CPU limits.
  # Anti-entropy (ring digest reconciliation) is async and not on
  # the critical path — do not confuse the two.
```

This gives 30 minutes for warm-tier loading. The `periodSeconds: 10`
reduces probe frequency (no point polling every 5s when loading takes
minutes).

**Note:** This is a band-aid. The real fix is Phase 2 item 2.1 (admin
server starts before loading, readyz gates on loading completion).

### 1.3 Increase default resources and auto-compute GOMEMLIMIT

**File:** `deploy/helm/bouine/values.yaml`, `deploy/helm/bouine/templates/_helpers.tpl`, `deploy/helm/bouine/templates/statefulset.yaml`

Loading millions of keys is CPU-intensive (CRC32C, mmap, map operations) and
memory-intensive (index map, idxSnap copy). The defaults are too low for a
warm tier with millions of keys.

**Resource increase:**

```yaml
resources:
  requests:
    cpu: 1000m    # was 500m
    memory: 2Gi   # was 512Mi
  limits:
    cpu: 4        # was 2
    memory: 8Gi   # was 4Gi
```

**GOMEMLIMIT auto-compute:**

**[review-gap]** The review (bug-5) notes that `RecomputeStats` copies the
entire index under RLock, which can trigger aggressive GC or OOMKill under
the container memory limit. The old chart hardcoded `GOMEMLIMIT=3GiB`
regardless of the actual memory limit — if an operator increased the memory
limit without also updating `goMemLimit`, the Go runtime would GC too
aggressively (wasting CPU) or not aggressively enough (risking OOMKill).

Instead of requiring operators to manually keep `GOMEMLIMIT` in sync with
the memory limit, the chart now auto-computes it as 75% of
`resources.limits.memory` via a Helm helper:

**`_helpers.tpl`** — new `bouine.goMemLimit` helper:
- If `.Values.goMemLimit` is set → use it (manual override).
- Otherwise → parse `resources.limits.memory`, multiply by 0.75, output
  with the matching Go-runtime suffix (`Gi`→`GiB`, `Mi`→`MiB`, etc.).

**`statefulset.yaml`** — uses the helper:
```yaml
- name: GOMEMLIMIT
  value: "{{ include "bouine.goMemLimit" . }}"
```

**`values.yaml`** — defaults to empty (auto-compute):
```yaml
# GOMEMLIMIT: leave empty (default) to auto-compute as 75% of
# resources.limits.memory via the bouine.goMemLimit helper.
# Set explicitly to override, e.g. goMemLimit: "6GiB".
goMemLimit: ""
```

Verified with `helm template`:
- `memory: 8Gi` → `GOMEMLIMIT: "6GiB"`
- `memory: 4Gi` → `GOMEMLIMIT: "3GiB"`
- `memory: 512Mi` → `GOMEMLIMIT: "384MiB"`
- `goMemLimit: "5GiB"` → `GOMEMLIMIT: "5GiB"` (override)

This means operators only need to set `resources.limits.memory` and the
chart handles GOMEMLIMIT correctly. The manual override is there for
advanced tuning (e.g., setting a lower limit to trigger GC earlier under
known memory pressure patterns).

### 1.4 Add `minReadySeconds` to the StatefulSet

**File:** `deploy/helm/bouine/templates/statefulset.yaml`

**[review-gap]** The investigation lists "No `minReadySeconds`" as a
contributing factor but the original plan didn't address it. Even with the
readiness gate fix (2.2), a small `minReadySeconds` gives the cluster time
to converge after a pod becomes ready before it enters endpoints:

```yaml
spec:
  minReadySeconds: 30
```

This delays the pod entering the Service endpoints by 30 seconds after
probes pass. In strong mode, this gives the ring time to stabilize after
the pod joins, reducing the window where peer-fetch routes to a node with
an incomplete ring view.

### 1.5 Make `preStop` hook duration independently configurable

**File:** `deploy/helm/bouine/templates/statefulset.yaml`, `deploy/helm/bouine/values.yaml`

**[review-gap]** The review (nit-9) calls out that the `preStop` hook
duration is hardcoded from `readinessProbe.periodSeconds` (6s default). In
strong mode, in-flight peer-fetch requests have a 500ms timeout, but a
queue of them under heavy load could take longer to drain. 6s is the
minimum; it should be independently configurable:

```yaml
# values.yaml
preStop:
  enabled: true
  sleepSeconds: 10  # was hardcoded 6s
```

```yaml
# statefulset.yaml
lifecycle:
  preStop:
    exec:
      command: ["sh", "-c", "sleep {{ .Values.preStop.sleepSeconds | default 10 }}"]
```

---

## Phase 2: Fix the Startup Architecture (code changes)

**Goal:** Admin server starts before warm-tier loading. `readyz` gates on
actual readiness. Eliminate the double-scan. Fix the cluster join semantics.

### 2.1 Start admin server (healthz/readyz only) before `initSubsystems()`

**File:** `cmd/bouine/cmd/engine.go`

**[review-gap]** The review (BLOCKER-1) makes a specific observation the
original plan didn't capture: the current `startupProbe` is structurally
useless, not just misconfigured. Because the admin server starts *after*
loading completes, and `IsReady()` starts `true`, the startup probe passes
on the first TCP connection — it never measures loading time. It's a "dead
man's switch for a process that's already past the dangerous phase." The
fix is not just "increase the threshold" (Phase 1 band-aid) but
fundamentally restructure so the probe can actually observe loading
progress.

Restructure `run()`:

```
1. Start a minimal admin HTTP server with /healthz (always 200) and
   /readyz (returns 503 until ready). No other admin routes yet.
2. Run initSubsystems() (warm-tier loading, cluster init, etc.)
3. Once initSubsystems() completes, swap the admin mux to the full
   route set (dashboard, metrics, peer-fetch, etc.) using
   http.Handler atomic swap, or shut down the minimal server and
   start the full one on the same port.
```

This ensures the kubelet gets HTTP responses from the moment the process
starts. The `startupProbe` measures actual loading time, not "is the
process alive." `livenessProbe` (`/healthz`) passes immediately, preventing
premature kills during slow loading.

**Implementation approach:** The cleanest path is to start the full admin
server early but with a `ReadyFn` that returns false until all subsystems
are initialized (see 2.2). The admin server already has all routes
registered at construction time (`admin.New`), so we just need to construct
it before `initSubsystems()` and wire the `ReadyFn` to the readiness gate.
The dashboard, metrics, and peer-fetch handlers will return 503 or 404
until their dependencies are initialized — this is acceptable during
startup.

Alternatively, use an `atomic.Value[http.Handler]` in the admin server
that starts with a minimal mux (healthz + readyz only) and is swapped to
the full mux after `initSubsystems()`. This avoids serving half-initialized
admin routes.

### 2.2 Implement real readiness gating

**File:** `internal/runtime/shutdown/shutdown.go`, `cmd/bouine/cmd/engine.go`

**[review-gap]** The review (BLOCKER-3) calls `IsReady` a "naming lie" —
it's a shutdown signal, not a readiness signal. The doc comment says
"Reports whether the server is still accepting traffic" but it reports
"accepting traffic" regardless of whether the store is loaded, listeners
are bound, or the cluster is joined. The fix must address both the
behavior and the naming.

Replace the single `atomic.Bool` in `Sequencer` with a `ReadinessGate`:

```go
type ReadinessGate struct {
    ready      atomic.Bool
    conditions map[string]atomic.Bool
    mu         sync.RWMutex
}

func (g *ReadinessGate) IsReady() bool {
    if !g.ready.Load() {
        return false
    }
    g.mu.RLock()
    defer g.mu.RUnlock()
    for _, c := range g.conditions {
        if !c.Load() {
            return false
        }
    }
    return true
}

func (g *ReadinessGate) MarkReady(name string) {
    g.mu.RLock()
    c, ok := g.conditions[name]
    g.mu.RUnlock()
    if ok {
        c.Store(true)
    }
}

func (g *ReadinessGate) Register(name string) {
    g.mu.Lock()
    defer g.mu.Unlock()
    if g.conditions == nil {
        g.conditions = make(map[string]atomic.Bool)
    }
    g.conditions[name] = atomic.Bool{}
}
```

Register conditions during startup:

1. `store-loaded` — registered at startup, marked true after
   `initSubsystems()` completes (warm tier + WAL replay done).
2. `listeners-bound` — registered at startup, marked true after
   `startListeners()` completes.
3. `cluster-joined` (strong mode only) — registered at startup, marked
   true after `joinWithRetry()` succeeds (see 2.4 for timeout semantics).

During shutdown, `Execute()` still flips `ready` to false immediately (for
the `mark-not-ready` step). This preserves the existing shutdown behavior
where `readyz` goes 503 before listeners are shut down.

**Naming fix:** Keep `IsReady()` as the public API name (it's wired into
`ReadyFn`), but update its doc comment to accurately describe what it
checks: "Reports whether all startup conditions are met and the server is
not shutting down." The old comment "Reports whether the server is still
accepting traffic" is removed — it was the naming lie the review called
out.

### 2.3 Store record size in WAL — eliminate mandatory RecomputeStats

**Files:** `internal/storage/wal/wal.go`, `internal/storage/tiered.go`, `internal/storage/warm/warm.go`

**[review-gap]** The review (BLOCKER-2) calls out that the `initWAL` doc
comment (tiered.go:216-218) says "replays it to rebuild the warm-tier
index" but the code does two things: replay + mandatory RecomputeStats.
The doc comment claims one thing, the code does two. Fix the comment as
part of this change.

Change the WAL entry format from 25 bytes to 33 bytes:
- Current: `op(1) + key(8) + segID(4) + offset(8) + crc(4) = 25`
- New: `op(1) + key(8) + segID(4) + offset(8) + size(8) + crc(4) = 33`

Add a WAL version byte in the header (or use a new magic number) to
distinguish v1 (25-byte records) from v2 (33-byte records).

In `initWAL()` (tiered.go:229-237):
```go
rErr := wal.Replay(walDir, func(e wal.Entry) error {
    switch {
    case e.IsPut():
        if e.HasSize() {
            // v2 WAL: size is known, no scan needed.
            t.warm.SetIndexWithSize(e.Key, int(e.SegID), e.Offset, e.Size)
        } else {
            // v1 WAL: size unknown, will need RecomputeStats.
            t.warm.SetIndex(e.Key, int(e.SegID), e.Offset)
        }
    case e.IsDelete():
        t.warm.DelIndex(e.Key)
    }
    return nil
})
```

Then `RecomputeStats()` only runs when:
- WAL replay failed (`rErr != nil`)
- WAL is v1 format (no size field)
- Index is empty after replay (cold start or WAL loss)

For the common case (v2 WAL replay succeeds with size), RecomputeStats is
skipped entirely. This cuts startup time roughly in half for large warm
tiers.

**Backward compatibility:** Check WAL magic/version on open. Old WALs
(25-byte records) trigger the fallback path (replay + RecomputeStats). New
WALs (33-byte records) skip RecomputeStats. After the first successful
startup with the new format, a WAL rewrite (already exists via
`rewriteWAL()`) converts to the new format. The rewrite happens during the
next compaction cycle (30 min) or can be triggered immediately on first
v2 startup.

**Doc comment fix:** Update `initWAL` comment to accurately describe the
two paths:
```go
// initWAL opens the WAL and replays it to rebuild the warm-tier index.
// If the WAL is v2 format (includes record size), the index is fully
// populated with size information and no segment scan is needed.
// If the WAL is v1 format or replay fails, RecomputeStats runs a full
// segment scan to backfill record sizes.
```

### 2.4 Fix `joinWithRetry` semantics and gate cluster readiness in strong mode

**File:** `cmd/bouine/cmd/engine.go`

**[review-gap]** The review (bug-6) raises two specific issues the original
plan didn't fully address:

1. **`joinWithRetry` returns `nil` whether it joined successfully or gave
   up.** Callers can't distinguish success from failure. This is a soft
   failure masquerading as success.

2. **The join should continue retrying in the background even after the
   initial deadline.** The review says: "The node can be 'ready' for
   self-owned keys while still trying to discover peers." The original
   plan's 2.4 said "if the cluster doesn't join within N seconds, the pod
   stays not-ready (and gets restarted)." That's too rigid — it doesn't
   account for eventual-mode nodes or scenarios where the node can serve
   self-owned keys while peers are still converging.

Fix both:

```go
// joinWithRetry attempts to join the cluster, retrying every 2 seconds
// for up to joinTimeout. Returns nil if join succeeded (Members() > 1),
// or an error identifying the failure if the deadline was reached.
// The background retry goroutine continues attempting to join after the
// deadline, so the node can still discover peers that come up later.
func (e *engine) joinWithRetry(ctx context.Context, c *cluster.Cluster, joinTimeout time.Duration) error {
    seeds := e.cfg.Cluster.Join
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()

    deadline := time.After(joinTimeout)
    for {
        _, err := c.Join(seeds)
        if err == nil && len(c.Members()) > 1 {
            e.logger.Info("cluster join succeeded", "members", len(c.Members()))
            return nil
        }
        select {
        case <-ctx.Done():
            return nil
        case <-deadline:
            return fmt.Errorf("cluster join: gave up after %s, running with local member only", joinTimeout)
        case <-ticker.C:
        }
    }
}
```

In `startClusterJoin()`, wire the readiness gate and continue background
retry:

```go
func (e *engine) startClusterJoin(g *supervised.Group, rs *runState) {
    if rs.clusterNode == nil || len(e.cfg.Cluster.Join) == 0 {
        // No cluster configured — mark condition as ready immediately.
        if rs.readinessGate != nil {
            rs.readinessGate.MarkReady("cluster-joined")
        }
        return
    }

    joinTimeout := e.cfg.Cluster.JoinTimeout
    if joinTimeout == 0 {
        joinTimeout = 120 * time.Second // was hardcoded 60s
    }

    g.Go("cluster-join", func(joinCtx context.Context) error {
        err := e.joinWithRetry(joinCtx, rs.clusterNode, joinTimeout)

        if rs.readinessGate != nil {
            if err == nil {
                rs.readinessGate.MarkReady("cluster-joined")
            } else if e.cfg.Cluster.Mode == config.ClusterModeStrong {
                // In strong mode, join failure means the node can't
                // route keys to peers. Keep the condition false so the
                // pod stays not-ready. The startupProbe will eventually
                // restart the pod, which gives it another chance.
                e.logger.Error("cluster join failed in strong mode; pod will stay not-ready",
                    "error", err)
            } else {
                // In eventual mode, the node can cache independently.
                // Mark ready and keep retrying in the background.
                rs.readinessGate.MarkReady("cluster-joined")
            }
        }

        // Continue retrying in the background regardless of the initial
        // result. Peers may come up later (e.g., during a rolling update
        // with sequential pod starts).
        if err != nil {
            e.logger.Info("cluster join: continuing background retry")
            ticker := time.NewTicker(5 * time.Second)
            defer ticker.Stop()
            for {
                select {
                case <-joinCtx.Done():
                    return nil
                case <-ticker.C:
                    if _, e := rs.clusterNode.Join(e.cfg.Cluster.Join); e == nil {
                        if len(rs.clusterNode.Members()) > 1 {
                            e.logger.Info("cluster join succeeded (background retry)",
                                "members", len(rs.clusterNode.Members()))
                            return nil
                        }
                    }
                }
            }
        }
        return nil
    })
}
```

**Config:**
```yaml
cluster:
  join_timeout: 120s  # default; was hardcoded 60s in joinWithRetry
```

**Design decision — strong vs eventual mode readiness:**
- **Strong mode:** Join failure → pod stays not-ready → startupProbe kills
  it → restart → another chance to join. This is correct: a strong-mode
  node that can't reach peers is useless and will cause origin thundering
  herd. The `join_timeout` must be long enough for peers to start (with
  `podManagementPolicy: Parallel`, peers start concurrently, so 120s is
  generous).
- **Eventual mode:** Join failure → pod becomes ready (caches independently)
  → background retry continues. This is correct: an eventual-mode node can
  serve cache hits and fetch from origin for misses, with or without peers.

### 2.5 Optimize RecomputeStats fallback path — eliminate index copy

**File:** `internal/storage/warm/warm.go:521-566`

**[review-gap]** The original plan said this was "eliminated by the BLOCKER-2
fix." The review (bug-5) says: "If RecomputeStats must stay for backward
compatibility, use a concurrent map read pattern that doesn't require a full
copy." The v1 WAL fallback path still runs RecomputeStats, so the index copy
problem persists for old WALs. Fix it.

Replace the full-index copy with a scan-then-update pattern that doesn't
pre-copy:

```go
func (s *Store) RecomputeStats() error {
    // First pass: scan segments to collect (key, recSize) pairs for
    // entries that need size backfill. No index lock needed — we're
    // reading from segment files, not the index.
    var entries, bytes int64
    sizeUpdates := make(map[uint64]int64)
    if err := s.Scan(func(r Record) error {
        if r.IsTomb {
            return nil
        }
        // Check if this record matches the current index entry.
        // We take a brief read lock per-key instead of copying the
        // entire index. This is O(N) brief locks, not O(N) copy.
        s.idxMu.RLock()
        loc, ok := s.index[r.Key]
        s.idxMu.RUnlock()
        if !ok || loc.segID != r.SegID || loc.offset != r.Offset {
            return nil
        }
        recSize := int64(HeaderLen + len(r.Body) + FooterLen)
        atomic.AddInt64(&entries, 1)
        atomic.AddInt64(&bytes, recSize)
        if loc.size == 0 {
            sizeUpdates[r.Key] = recSize
        }
        return nil
    }); err != nil {
        return fmt.Errorf("warm: recompute stats: %w", err)
    }
    s.stats.entries.Store(entries)
    s.stats.bytes.Store(bytes)

    // Backfill size under a single write lock batch.
    if len(sizeUpdates) > 0 {
        s.idxMu.Lock()
        for key, sz := range sizeUpdates {
            if loc, ok := s.index[key]; ok && loc.size == 0 {
                loc.size = sz
                s.index[key] = loc
            }
        }
        s.idxMu.Unlock()
    }
    return nil
}
```

This does a brief RLock per-key during the scan instead of copying the
entire index upfront. The RLock is held only for the map lookup (nanoseconds),
not for the segment read. The scan itself (mmap + CRC) runs without any
index lock. This eliminates the multi-hundred-MB `idxSnap` allocation.

**Trade-off:** Per-key RLock during scan adds lock contention if concurrent
writes are happening. But during startup, no writes are happening (the store
is not yet accepting traffic), so contention is zero. This is strictly
better than the copy approach.

### 2.6 Delay first compaction check

**File:** `internal/storage/tiered.go:167-170`

**[review-gap]** The review (taste-7) notes that `compactLoop`'s ticker
fires immediately on the first iteration, causing compaction to compete
with startup traffic if the warm tier has fragmentation from a previous
run. The original plan's 2.5 had this, but the implementation was naive
— it used a fixed 5-minute delay. Make it configurable and gate it on
the readinessGate so compaction doesn't start until the pod is actually
serving traffic:

```go
func (t *TieredStore) compactLoop() {
    defer t.compactWg.Done()
    // Delay first compaction to avoid competing with startup I/O.
    // The delay gives the pod time to finish loading, join the cluster,
    // and stabilize under traffic before compaction kicks in.
    timer := time.NewTimer(t.compactStartupDelay)
    defer timer.Stop()
    select {
    case <-t.done:
        return
    case <-timer.C:
    }
    ticker := time.NewTicker(t.compactInterval)
    defer ticker.Stop()
    for {
        select {
        case <-t.done:
            return
        case <-ticker.C:
            t.maybeCompact()
        }
    }
}
```

**Config:**
```yaml
storage:
  compact_startup_delay: 5m  # default; was 0 (immediate)
  compact_interval: 30m       # default; unchanged
```

### 2.7 Fix misleading doc comments

**Files:** `internal/storage/tiered.go:216-218`, `internal/runtime/shutdown/shutdown.go:42-43`

**[review-gap]** The review (BLOCKER-2, BLOCKER-3) calls out two doc
comments that lie about what the code does:

1. `initWAL` (tiered.go:216-218): says "replays it to rebuild the
   warm-tier index" but also runs RecomputeStats. Fixed in 2.3.

2. `IsReady` (shutdown.go:42-43): says "Reports whether the server is
   still accepting traffic" but reports true regardless of store/listener/
   cluster state. Fixed in 2.2.

Both are addressed by the code changes above. Verify the new comments
accurately describe the new behavior.

---

## Phase 3: Eliminate the Index Rebuild (larger effort, future phase)

**Goal:** Near-instant startup by persisting the index on disk. Target:
sub-second startup for 10M keys, <5s for 100M keys.

**Full design:** `STARTUP_PHASE3_DESIGN.md` (reviewed and revised based on
`STARTUP_PHASE3_LINUS_REVIEW.md`).

The detailed design covers:

- **3.1 On-disk index snapshot** — file format (32-byte header, segment
  table, 28-byte sorted entries, 8-byte footer), startup load path
  (mmap → validate segment table → build map → WAL delta replay),
  snapshot write path (index copy under RLock ~50ms, lock-free sort +
  file write), SIEVE rebuild strategy (key-order for snapshot entries,
  WAL-replay-order for delta entries), interaction with compaction and
  clean shutdown.
- **3.2 Lazy segment loading** — `Segment.ensureOpen()` with
  `atomic.Bool` fast path, `Segment.Close()` nil-safe, LRU FD cache
  with per-segment `readers atomic.Int32` to prevent eviction mid-read,
  auto-scaling cache size (`min(segCount, 256)`), integration with
  `Get`/`Put`/`Scan`/`Compact`.
- **3.3 Crash-safe WAL checkpointing** — checkpoint sequence (flush →
  copy index → block WAL writes → second flush → truncate → unblock →
  write snapshot from copy), full crash-safety table for every crash
  point, `checkpointing atomic.Bool` spin gate, `checkpointLoop`
  goroutine, config for interval and WAL entry threshold.

Key review findings addressed in the design:
- **BLOCKER-1:** Snapshot write holds idxMu.RLock for ~50ms (not 400ms)
  — copies index like existing `Compact()`, iterates lock-free.
- **BLOCKER-2:** WAL truncate is crash-safe — blocks WAL writes during
  the truncate window, uses index copy as snapshot source (Put updates
  index before WAL enqueue, so copy is a superset).
- **BLOCKER-3:** Segment table in snapshot header for O(S)
  missing-segment validation, not O(N) per-entry.
- **bug-4/5/6/7:** Lazy loading integrates with `Scan` (`ensureOpen`),
  `Compact` (nil-safe `Close`), `Get` (`ensureOpen` before
  `readRecordAt`), LRU eviction (reader count prevents mid-read close).

**Config additions:**
```yaml
storage:
  snapshot_enabled: true
  checkpoint_interval: 5m
  checkpoint_wal_threshold: 100000
  segment_cache_size: 0  # 0 = auto (min(segCount, 256))
```

---

## Phase 4: Observability for Startup

**Goal:** Operators can see what's happening during slow startup.

### 4.1 Startup progress metrics

Add Prometheus gauges:
- `bouine_startup_phase` — current phase (0=init, 1=loading_warm,
  2=loading_wal, 3=recompute_stats, 4=cluster_join, 5=ready)
- `bouine_warm_loading_keys_total` — keys loaded so far during init
- `bouine_warm_loading_bytes_total` — bytes scanned so far during init
- `bouine_startup_duration_seconds` — histogram of total startup time

**Cardinality note:** These are gauges with no labels (or a single `phase`
label with ≤6 values), well within the cardinality budget (§9).

### 4.2 Structured startup logs

Log progress milestones with key counts and durations:
```
INFO  warm-tier loading started segments=42
INFO  wal replay complete entries=1048576 duration=2.3s wal_version=v2
INFO  recompute stats skipped (wal v2 has size) entries=1048576
INFO  startup complete duration=4.8s
```

For the v1 fallback path:
```
INFO  warm-tier loading started segments=42
INFO  wal replay complete entries=1048576 duration=2.3s wal_version=v1
WARN  recompute stats running (wal v1 lacks size field) segments=42
INFO  recompute stats complete entries=1048576 duration=45.2s
INFO  startup complete duration=48.1s
```

### 4.3 `/readyz` detail endpoint

Add `/readyz?detail=1` that returns JSON with each readiness condition:
```json
{
  "status": "not-ready",
  "conditions": [
    {"name": "store-loaded", "status": "true"},
    {"name": "listeners-bound", "status": "true"},
    {"name": "cluster-joined", "status": "false", "detail": "retrying, 45s elapsed"}
  ]
}
```

---

## Implementation Priority

| Priority | Item | Effort | Impact | Review Finding |
|----------|------|--------|--------|----------------|
| P0 | 1.1 `podManagementPolicy: Parallel` + `maxUnavailable: 1` | S | Eliminates serialized rollout | BLOCKER-4 |
| P0 | 1.2 Fix `startupProbe` budget + comment | S | Prevents CrashLoopBackOff, stops misleading operators | BLOCKER-1, bullshit-8 |
| P0 | 2.1 Admin server before loading | M | Probes get HTTP responses; startupProbe measures loading, not liveness | BLOCKER-1 |
| P0 | 2.2 Real readiness gate | M | `readyz` reflects actual state; fixes naming lie | BLOCKER-3 |
| P1 | 2.3 WAL v2 with record size | M | Eliminates double-scan; cuts loading time ~50% | BLOCKER-2 |
| P1 | 2.4 Fix `joinWithRetry` semantics + cluster readiness gate | M | Distinguishes success from give-up; background retry; strong-mode gate | bug-6 |
| P1 | 1.3 Increase resources + GOMEMLIMIT | S | Prevents OOM during loading | bug-5 |
| P1 | 1.4 `minReadySeconds: 30` | S | Gives ring time to converge before endpoints | — |
| P2 | 2.5 Optimize RecomputeStats fallback (no index copy) | S | Eliminates memory spike for v1 WAL path | bug-5 |
| P2 | 2.6 Delay first compaction | S | Reduces startup CPU/I/O contention | taste-7 |
| P2 | 2.7 Fix doc comments | S | Stops lying to readers | BLOCKER-2, BLOCKER-3 |
| P2 | 1.5 Configurable `preStop` hook | S | Allows longer endpoint deregistration in strong mode | nit-9 |
| P2 | 4.1-4.3 Startup observability | M | Operators can diagnose slow startup | — |
| P3 | 3.1 On-disk index snapshot | L | Near-instant startup | RC-6 |
| P3 | 3.2 Lazy segment loading | M | Reduces startup FD/mmap overhead | — |
| P3 | 3.3 Incremental WAL checkpointing | M | Bounds WAL replay time | — |

---

## Testing Plan

### Unit tests
- `ReadinessGate`: all conditions true → ready; one false → not-ready;
  shutdown → not-ready; `MarkReady` on unregistered name is a no-op.
- WAL v2 format: write + replay with size field; verify `SetIndexWithSize`
  populates `size` correctly.
- WAL v1 backward compat: write a v1 WAL (25-byte records), replay with
  v2 code, verify it falls back to RecomputeStats. Verify no data loss
  (all keys present in index after replay + scan).
- `initWAL`: skip RecomputeStats when WAL v2 has size; run it when v1
  or replay fails or index is empty.
- `joinWithRetry`: returns nil on success, error on deadline; background
  retry continues after deadline; context cancellation stops both.
- `RecomputeStats` (fallback path): no index copy; per-key RLock;
  correct size backfill; correct stats counters.

### Integration tests
- 3-node StatefulSet with `podManagementPolicy: Parallel`, warm tier with
  100K keys, strong mode. All pods become Ready within 2 minutes.
- Rolling update with `maxUnavailable: 1`: no 5xx during pod restart.
- Pod restart with 1M keys: startup completes within startupProbe budget.
- v1 WAL → v2 migration: start with a v1 WAL, verify the pod starts
  (via fallback), writes a v2 WAL on next checkpoint, and the next
  restart skips RecomputeStats.
- Strong-mode cluster join failure: seed peers unreachable → pod stays
  not-ready → startupProbe kills it → restart → same behavior. Verify
  no traffic is accepted during the not-ready window.
- Eventual-mode cluster join failure: seed peers unreachable → pod
  becomes ready → serves cache hits → background retry succeeds when
  peers come up → ring converges.

### Benchmark
- `BenchmarkWarmTierInit`: measure `NewTieredStore` with N keys, v1 WAL
  vs v2 WAL (with size). Assert v2 WAL path is ≥ 2× faster.
- `BenchmarkRecomputeStats`: measure scan time with N keys. Assert
  no allocation for index copy in the optimized fallback path
  (`allocs/op` for the index-related portion = 0).
- `BenchmarkStartupProbe`: measure time from process start to first
  `readyz` 200 response, with and without the admin-server-before-loading
  change. Assert the early-admin path answers probes within 1s of
  process start.

---

## Risks and Mitigations

- **`podManagementPolicy: Parallel` + rolling update:** All pods could
  restart simultaneously. Mitigated by `maxUnavailable: 1` in
  `updateStrategy.rollingUpdate` — K8s rolls one pod at a time during
  updates while allowing parallel initial boot. Test this explicitly in
  the integration suite.

- **WAL format change:** Old v1 WALs must still replay correctly. The
  fallback path (RecomputeStats) must remain functional. Test with WALs
  generated by the current code (commit a v1 WAL as testdata). After the
  first v2 startup, `rewriteWAL()` converts to v2 format during the next
  compaction. Verify no data loss across the migration.

- **Readiness gate in strong mode:** If cluster join fails (network
  partition, wrong seeds), the pod stays not-ready and eventually gets
  killed by the startupProbe. This is correct but operators must
  understand it. Document in `docs/runbook/` — add a "Pod stuck in
  not-ready" troubleshooting section. Provide `cluster.join_timeout`
  config to tune (default 120s, was hardcoded 60s).

- **Admin server early start:** Starting the admin server before
  `initSubsystems()` means dashboard, metrics, and peer-fetch routes
  aren't fully functional during loading. Two options: (a) serve them
  with stub responses (503 for peer-fetch, empty for metrics), or (b)
  use `atomic.Value[http.Handler]` swap to replace the minimal mux with
  the full mux after init. Option (b) is cleaner — no half-initialized
  routes. Test that the minimal mux only exposes `/healthz` and
  `/readyz`.

- **`joinWithRetry` background retry goroutine:** The background retry
  runs indefinitely until the context is cancelled or join succeeds.
  This goroutine is owned by the supervised group and joins on shutdown.
  Verify it doesn't leak (the `g.Go("cluster-join", ...)` wrapper
  handles this).

- **`RecomputeStats` per-key RLock under concurrent writes:** During
  startup there are no concurrent writes (store not yet accepting
  traffic), so contention is zero. But if RecomputeStats is ever called
  at runtime (e.g., after a compaction), the per-key RLock could contend
  with writes. Mitigated by: (a) compaction holds a write lock on
  segments, preventing concurrent RecomputeStats, and (b) the per-key
  RLock is held for nanoseconds (map lookup only), not for the segment
  read. Benchmark to verify.

- **Ring rebalancing mid-traffic:** Even with the readiness gate, when a
  new peer joins after the pod is already ready, the ring rebalances and
  some keys start routing to the new peer. If the new peer isn't ready
  yet, peer-fetch fails → 502. This is a remaining issue not fully
  solved by this plan. Mitigated by: (a) `minReadySeconds: 30` gives
  convergence time, (b) `podManagementPolicy: Parallel` means peers
  start concurrently and join at roughly the same time, (c) the
  peer-fetch 500ms timeout falls through to origin (degraded but not
  broken). A future fix could add a "ring warming" period where new
  peers are added to the ring but marked as not-ready, so peer-fetch
  skips them. This is out of scope for this plan.
