# Startup Investigation: StatefulSet Strong-Mode Rollout Under Heavy Load with Full Warm Tier

## Scenario

Rolling out a bouine cluster as a StatefulSet (official Helm chart), strong
consistency mode, under heavy load, with a warm tier filled with millions of
keys. Containers restart repeatedly and take a very long time to answer
probes.

---

## Root Causes (ordered by severity)

### RC-1: Synchronous warm-tier loading blocks all probe endpoints

**Files:** `cmd/bouine/cmd/engine.go:89-118`, `internal/storage/tiered.go:112-185`

`engine.run()` calls `initSubsystems()` synchronously (line 101), which calls
`buildStore()` → `NewTieredStore()`. The admin HTTP server (`/healthz`,
`/readyz`) only starts in the supervised group (line 111) — **after**
`initSubsystems()` returns. During the entire warm-tier loading phase, no HTTP
server is listening on the admin port.

The `startupProbe` has a 5-minute budget (60 failures × 5s = 300s). If
warm-tier loading exceeds 5 minutes, the kubelet kills the pod. The pod
restarts, loading starts from scratch, hits the same wall, gets killed again
— classic CrashLoopBackOff.

Even if loading finishes in 3-4 minutes, the pod appears unresponsive for
that entire duration because no probe can be answered.

### RC-2: RecomputeStats does a mandatory full segment scan even after successful WAL replay

**Files:** `internal/storage/tiered.go:258`, `internal/storage/warm/warm.go:521-566`

`initWAL()` does:
1. WAL replay — reads 25-byte records, calls `SetIndex(key, segID, offset)`
   with `size=0`. Fast and compact.
2. **Always** calls `RecomputeStats()` — even when WAL replay succeeded.

`RecomputeStats()` (warm.go:521):
- Takes a read lock and **copies the entire index** into `idxSnap` (line 523)
  — doubling memory for the index map.
- Calls `s.Scan()` which mmaps **every segment file** and walks **every
  record** (live + tombstone), computing CRC32C for each.
- For each live record, checks if `(segID, offset)` matches the index entry,
  and if so, counts it and backfills the `size` field.

This is a **double-scan design**: WAL replay builds the index, then
RecomputeStats scans all segments again. With millions of keys and non-trivial
body sizes, the RecomputeStats scan dominates startup time.

The WAL stores `(op, key, segID, offset)` — 25 bytes — but **not** the record
size. That's why RecomputeStats is mandatory: it's the only way to recover
`size` for accurate byte accounting on Delete.

### RC-3: No `podManagementPolicy: Parallel` — StatefulSet serializes pod startup

**File:** `deploy/helm/bouine/templates/statefulset.yaml`

The StatefulSet template does not set `podManagementPolicy`. The default is
`OrderedReady`, meaning:
- Pod-0 must be Running + Ready before pod-1 starts.
- Pod-1 must be Running + Ready before pod-2 starts.

With a 3-replica StatefulSet and 3-minute loading time per pod, total rollout
= 9 minutes. If any pod exceeds the 5-minute startupProbe budget, it
restarts, further delaying downstream pods.

In strong mode this is especially harmful: pod-0 starts alone, with no peers
in the ring. It owns all keys. Under heavy load, it gets hammered with
origin fetches for everything. When pod-1 finally starts, pod-0 must drain
and rebalance.

### RC-4: Readiness gate is non-functional — `/readyz` returns 200 from the moment the admin server starts

**Files:** `internal/runtime/shutdown/shutdown.go:32,44`, `cmd/bouine/cmd/engine.go:429`

`Sequencer.IsReady()` starts `true` (line 32) and only goes `false` during
shutdown (line 53). The `ReadyFn` is wired to `IsReady()` (engine.go:429).

There is **no readiness check** that gates on:
- Warm-tier loading being complete (moot because admin starts after loading,
  but the intent is wrong)
- Cluster join being complete
- Data-plane listeners being bound
- Store being usable

Once the admin server starts, `/readyz` returns 200 immediately. In strong
mode, the pod enters the Service endpoints **before** `startClusterJoin()`
completes (it runs concurrently in the supervised group). Under heavy load,
the pod receives traffic with an incomplete cluster membership:
- Ring contains only self → `OwnerFn` returns self for all keys → no peer
  fetch → all misses go to origin.
- As peers join, the ring rebalances mid-traffic → keys start routing to
  peers that may still be loading → peer-fetch timeouts (500ms) → 502s.

### RC-5: Memory pressure during loading — RecomputeStats doubles the index in memory

**File:** `internal/storage/warm/warm.go:522-527`

`RecomputeStats()` copies the entire `map[uint64]warmLoc` into `idxSnap`
(line 523). With millions of keys (~100-150 bytes per entry), this is a
transient ~100-150 MB allocation per million keys, on top of the live index.

Combined with:
- The live index map (~100-150 MB per million keys)
- mmap'd segment pages during scan (with `MmapPopulate`, forces page-in)
- Go runtime overhead, GOMEMLIMIT at 3GiB

With 10M+ keys, this can trigger GC pressure or OOMKills, especially under
container memory limits (default 4Gi in the chart).

### RC-6: No persistent on-disk index — full rebuild every restart

**Files:** `internal/storage/tiered.go:219-263`, `internal/storage/warm/warm.go`

The warm-tier index (`map[uint64]warmLoc`) is rebuilt from scratch on every
startup. The WAL helps (compact 25-byte records vs full segment scan), but
the mandatory RecomputeStats scan negates most of that benefit.

There is no on-disk index file that could be mmapped for near-instant
startup. Every restart pays the full O(N) scan cost.

### RC-7: Cluster join is fire-and-forget — no readiness gate in strong mode

**Files:** `cmd/bouine/cmd/engine.go:659-735`

`startClusterJoin()` runs as a background goroutine. `joinWithRetry()` tries
every 2s for 60s, then gives up and runs single-node. There is no callback
that flips readiness after a successful join. In strong mode, accepting
traffic before cluster membership is established causes:
- All keys routed to self (no peers in ring)
- Origin thundering herd
- Ring rebalancing mid-traffic when peers eventually join

---

## Contributing Factors

- **No `minReadySeconds`** on the StatefulSet — pods become Ready
  instantly once probes pass, no grace period for cluster convergence.
- **`preStop` hook is only 6s** — may be too short for in-flight peer-fetch
  requests in strong mode.
- **Default resources (2 CPU, 4Gi memory)** may be insufficient for loading
  millions of keys under concurrent traffic.
- **`warm_sync_interval: 60s`** means the WAL may have 60s of uncheckpointed
  entries, adding to replay time.
- **`compactLoop` starts immediately after loading** (tiered.go:169) — if
  the warm tier has many fragmented segments from a previous run,
  compaction kicks in right after startup, adding I/O and CPU pressure
  while the pod is already serving traffic.

---

## Startup Timeline (3-replica StatefulSet, 3-minute load time, strong mode)

```
t=0     Pod-0 starts. initSubsystems() begins. No admin server.
        startupProbe: connection refused (no failure counted yet, first check).
t=5s    startupProbe: connection refused (failure 1).
t=10s   startupProbe: connection refused (failure 2).
...
t=180s  initSubsystems() completes. Admin server starts.
        startupProbe: /readyz → 200 (pass). Pod-0 becomes Ready.
        Data-plane listeners start. Cluster join starts (async).
        Pod-0 enters Service endpoints. Heavy traffic arrives.
        Ring has only pod-0 → all keys → origin thundering herd.
t=185s  Pod-1 starts (OrderedReady). initSubsystems() begins.
t=240s  Pod-0 cluster join succeeds (or gives up at t=240s).
        Ring rebalances. Some keys now route to pod-1 (not yet ready).
        Peer-fetch to pod-1 fails → 502s for those keys.
t=365s  Pod-1 initSubsystems() completes. Admin server starts.
        Pod-1 becomes Ready. Enters endpoints.
t=370s  Pod-2 starts. ...
t=545s  Pod-2 becomes Ready. Full cluster operational.
```

Total rollout: ~9 minutes for 3 pods. If any pod exceeds 5 minutes → restart
→ adds another 5+ minutes.

With 10M keys and larger bodies, loading could take 5-10 minutes per pod,
guaranteeing CrashLoopBackOff.

---

## Summary

The fundamental problem is a **synchronous, eager, double-scan loading
design** with **no real readiness gate** and **serialized pod startup**. The
warm tier loads everything into memory before the process can answer any
probe, and the mandatory RecomputeStats scan doubles the I/O cost. The 5-minute
startupProbe budget is a hard cliff with no graceful degradation.
