# Linus Review: Startup Path Under Load with Full Warm Tier

**Verdict: Fix-before-merge — this startup path will CrashLoopBackOff any
StatefulSet with a multi-million-key warm tier, and the readiness gate is
security theater.**

Reviewed files:
- `cmd/bouine/cmd/engine.go` (startup orchestration)
- `internal/storage/tiered.go` (tiered store init)
- `internal/storage/warm/warm.go` (warm tier loading, scan, stats)
- `internal/runtime/shutdown/shutdown.go` (readiness sequencer)
- `internal/admin/server.go` (probe handlers)
- `deploy/helm/bouine/templates/statefulset.yaml` (StatefulSet)
- `deploy/helm/bouine/values.yaml` (probe/resource config)

---

## BLOCKER-1: Synchronous warm-tier loading makes the process unresponsive for minutes, with a 5-minute kill timer

**Location:** `cmd/bouine/cmd/engine.go:101`, `internal/storage/tiered.go:112-185`

`engine.run()` calls `initSubsystems()` synchronously (line 101). Inside that,
`buildStore()` → `NewTieredStore()` calls `initWarm()` and `initWAL()` — both
synchronous, both blocking. The admin HTTP server starts at line 111, in the
supervised group — **after** `initSubsystems()` returns.

So during the entire warm-tier loading phase, no HTTP server is bound to the
admin port. The `startupProbe` (`/readyz` on admin port, 5s period, 60
failures = 300s) gets connection-refused on every attempt. If loading takes
> 5 minutes, the kubelet kills the pod. Pod restarts. Loading starts from
scratch. Same wall. CrashLoopBackOff.

The comment on `startupProbe` in `values.yaml:138` says `# 5 minutes max
warmup for anti-entropy`. That's not even the right justification — the
bottleneck is warm-tier WAL replay + RecomputeStats, not anti-entropy. The
comment is theater: it names a budget for the wrong problem, and the budget
isn't even sufficient for the real problem.

**Fix:** The admin server (at minimum `/healthz` and `/readyz`) must start
**before** warm-tier loading, and `/readyz` must return 503 until loading
completes. This way the `startupProbe` gets HTTP responses instead of
connection-refused, and the 5-minute budget actually measures "loading is
taking too long" rather than "the process is dead."

---

## BLOCKER-2: RecomputeStats always does a full segment scan, even after a successful WAL replay — double-scan design

**Location:** `internal/storage/tiered.go:258`, `internal/storage/warm/warm.go:521-566`

`initWAL()` (tiered.go:219-263):
1. `wal.Replay()` — reads 25-byte records, calls `SetIndex(key, segID,
   offset)` with `size=0`. O(N), compact, fast.
2. Line 258: `t.warm.RecomputeStats()` — **always runs**, even when WAL
   replay succeeded and the index is populated.

`RecomputeStats()` (warm.go:521-566):
- Line 523: copies the **entire index** into `idxSnap` under RLock. With
  millions of keys, this is a multi-hundred-MB allocation that doubles the
  index memory temporarily.
- Line 531: calls `s.Scan()` which mmaps every segment file and walks every
  record (live + tombstone), computing CRC32C for each.
- For each live record, checks if `(segID, offset)` matches the index entry,
  counts it, and backfills `size` if it was 0.

This is a **double-scan**: WAL replay builds the index, RecomputeStats scans
all segments again. The root cause is that the WAL record format (25 bytes:
op + key + segID + offset) **does not store the record size**. So the only
way to recover `size` for accurate byte accounting on Delete is to scan every
segment.

The comment on `initWAL` (tiered.go:216-218) says "replays it to rebuild the
warm-tier index." That's half-true. It rebuilds the index, then
RecomputeStats scans everything again. The doc comment claims one thing, the
code does two.

With 10M keys at ~4 KiB average body size, that's ~40 GB of segment data to
mmap-scan. At 10-20 GiB/s mmap throughput, that's 2-4 seconds per scan. But
under container CPU limits (2 cores), memory pressure from the index copy,
and concurrent page faults, real-world time will be much higher. And if the
WAL was empty or corrupt, `rebuildIndexFromScan()` (tiered.go:714) scans
first, then RecomputeStats scans **again** — two full scans.

**Fix:** Store the record size in the WAL entry. Change the WAL format from
25 bytes to 33 bytes (add an 8-byte `size` field). Then `SetIndex` can set
`size` during replay, and RecomputeStats becomes unnecessary when WAL replay
succeeds. For backward compatibility, check the WAL magic/version and fall
back to RecomputeStats for old WALs.

---

## BLOCKER-3: `readyz` is a sham — `IsReady()` starts `true` and never checks anything

**Location:** `internal/runtime/shutdown/shutdown.go:32,44`, `cmd/bouine/cmd/engine.go:429`

`NewSequencer()` sets `ready.Store(true)` (line 32). `IsReady()` returns that
value (line 44). It only goes `false` when `Execute()` is called during
shutdown (line 53).

The `readyz` handler (admin/server.go:240-246) calls `ReadyFn()` which is
`rs.seq.IsReady` (engine.go:429). So `/readyz` returns 200 from the moment
the admin server starts, and only returns 503 during shutdown.

The doc comment on `IsReady()` (shutdown.go:42-43) says "Reports whether the
server is still accepting traffic." That's technically true — but it's also
useless. It reports "accepting traffic" regardless of whether the store is
loaded, the cluster is joined, or the data-plane listeners are bound. The
function is a shutdown signal, not a readiness signal. Calling it `IsReady`
is a naming lie.

In strong mode, this is actively harmful. The pod enters Service endpoints
before `startClusterJoin()` completes. The ring contains only self. All keys
route to self → all misses go to origin → origin thundering herd under heavy
load. When peers eventually join, the ring rebalances mid-traffic, causing
peer-fetch failures and 502s.

The `startupProbe` also hits `/readyz`. Since `IsReady()` is always true
during startup, the startup probe passes on the first successful TCP
connection to the admin port. The 5-minute budget is irrelevant — the probe
passes as soon as the admin server binds, which is after loading completes.
So the startup probe doesn't protect against slow loading at all — it's a
dead man's switch for a process that's already past the dangerous phase.

**Fix:** `IsReady()` should return `false` until:
1. The store is loaded (warm tier init complete).
2. Data-plane listeners are bound.
3. In strong mode: cluster join has completed (or at least quorum is
   reached).

The `Sequencer` should have a `MarkReady()` method that flips the flag after
all startup conditions are met.

---

## BLOCKER-4: No `podManagementPolicy: Parallel` — serialized rollout compounds loading time

**Location:** `deploy/helm/bouine/templates/statefulset.yaml`

The StatefulSet template does not set `podManagementPolicy`. Default is
`OrderedReady`: pod-N must be Ready before pod-N+1 starts.

With a 3-replica cluster and 3-minute loading per pod, total rollout is 9
minutes. If pod-0 takes 6 minutes (exceeds startupProbe), it restarts, and
pods 1 and 2 don't even start. The entire rollout is blocked behind one
slow pod.

In strong mode, `OrderedReady` is especially bad: pod-0 starts alone with no
peers. It owns the entire ring. Under heavy load, it takes all traffic and
fetches everything from origin. When it finally becomes Ready and pod-1
starts, pod-0 must rebalance while still under load.

**Fix:** Set `podManagementPolicy: Parallel` so all pods start simultaneously.
In strong mode, this is correct — all pods join the cluster concurrently via
gossip, the ring converges, and traffic is distributed from the start.

---

## bug-5: RecomputeStats copies the entire index under RLock — memory spike and GC pressure

**Location:** `internal/storage/warm/warm.go:522-527`

```go
s.idxMu.RLock()
idxSnap := make(map[uint64]warmLoc, len(s.index))
for k, v := range s.index {
    idxSnap[k] = v
}
s.idxMu.RUnlock()
```

With millions of keys, `idxSnap` is a multi-hundred-MB allocation. It's used
to avoid holding the lock during the scan, which is correct for lock
duration, but the copy itself is O(N) in both time and memory. Under
container memory limits (4Gi default, GOMEMLIMIT at 3GiB), this allocation
can trigger aggressive GC or OOMKill.

The copy is only needed because the scan backfills `size` into index entries
(line 556-563 takes a write lock). If the WAL stored `size` (see BLOCKER-2
fix), the scan wouldn't be needed at all, and neither would the copy.

**Fix:** Eliminated by the BLOCKER-2 fix (store size in WAL). If
RecomputeStats must stay for backward compatibility, use a concurrent map
read pattern that doesn't require a full copy — e.g., iterate the index
without a snapshot and skip entries that were modified during iteration.

---

## bug-6: `joinWithRetry` gives up silently after 60s and runs single-node in strong mode

**Location:** `cmd/bouine/cmd/engine.go:710-735`

```go
case <-deadline:
    e.logger.Warn("cluster join: gave up after 60s, running with local member only", ...)
    return nil
```

In strong mode, running single-node means the node owns all keys. If peers
are just slow to start (e.g., during a rolling update where pods start
sequentially), the node gives up before peers are ready, routes everything
to itself, and never re-checks. The ring only converges when peers' gossip
push/pull reaches this node (every 5s), but the node itself has stopped
trying to join.

This is a soft failure masquerading as success. The function returns `nil`
(no error) whether it joined successfully or gave up. Callers can't
distinguish.

**Fix:** In strong mode, cluster join failure should affect readiness (see
BLOCKER-3). The 60s timeout should be configurable. The join should
continue retrying in the background even after the initial deadline — the
node can be "ready" for self-owned keys while still trying to discover peers.

---

## taste-7: `compactLoop` starts immediately after loading — competes with traffic

**Location:** `internal/storage/tiered.go:167-170`

```go
if ts.warm != nil {
    ts.compactWg.Add(1)
    go ts.compactLoop()
}
```

`compactLoop` checks every 30 minutes. But the first check happens
immediately (ticker fires right away). If the warm tier has fragmentation
from a previous run, compaction starts right after loading, while the pod is
already serving traffic under heavy load. Compaction mmaps segments, copies
live records, and rewrites segment files — significant I/O and CPU.

**Fix:** Add a startup delay before the first compaction check (e.g., 5
minutes), or gate compaction on a "warmup complete" signal.

---

## bullshit-8: startupProbe comment names the wrong bottleneck

**Location:** `deploy/helm/bouine/values.yaml:138`

```yaml
failureThreshold: 60  # 5 minutes max warmup for anti-entropy
```

The comment says "anti-entropy" but the actual bottleneck is WAL replay +
RecomputeStats segment scan. Anti-entropy (ring digest reconciliation) is
async and non-blocking (runs on push/pull interval). The comment misleads
operators into thinking the budget is for cluster sync, when it's actually
for disk I/O.

**Fix:** Update the comment to name the real bottleneck, and make the budget
configurable based on warm-tier size.

---

## nit-9: `preStop` hook duration is hardcoded from `readinessProbe.periodSeconds`

**Location:** `deploy/helm/bouine/templates/statefulset.yaml:98`

```yaml
command: ["sh", "-c", "sleep {{ add1 (.Values.readinessProbe.periodSeconds | default 5) }}"]
```

This sleeps 6s (default). In strong mode, in-flight peer-fetch requests have
a 500ms timeout, but a queue of them could take longer. The `preStop` hook
should allow enough time for the endpoints controller to deregister the pod
before the shutdown sequencer stops listeners. 6s is the minimum; consider
making it independently configurable.

---

## What's done well

- The `GOMAXPROCS` auto-detection from cgroup quota (engine.go:93-98) is
  correct and necessary in K8s.
- The `preStop` → `mark-not-ready` → `drain-refresh-handlers` → `flush-store`
  → `cluster-leave` shutdown order is sound.
- The WAL + segment-scan fallback design is the right idea, just poorly
  executed (missing size field → mandatory double-scan).
- The mmap-based scan with `MADV_SEQUENTIAL` is the right approach for
  throughput.
- The `joinWithRetry` retry loop is correctly non-blocking (runs in a
  supervised goroutine).
