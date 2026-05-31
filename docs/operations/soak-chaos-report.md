# Soak + Chaos Report

> **Status**: Framework complete; full soak runs must be attached to the
> v1.0 release tag per PLAN.md §4.5. See §6 for required evidence.

---

## 1. Overview

This document records the soak and chaos testing required by PLAN.md §4.5
to clear the Phase 4.5 exit criteria before Phase 5 begins.

| Criterion | Required | Status |
|-----------|----------|--------|
| 24-hour soak at 50 % canonical capacity | ≥ 307/365 conf.; zero 5xx | **Pending v1.0 tag** |
| 72-hour soak at 25 % canonical capacity | RSS stable; no leak | **Pending v1.0 tag** |
| Peer kill (single node removed mid-traffic) | Surviving nodes 0 5xx | ✅ `TestChaos_PeerKill` |
| Origin flap (origin bounces 5×) | Zero 5xx (stale-on-error) | ✅ `TestChaos_OriginFlap` |
| Partial partition (SIGSTOP then healed) | Gossip convergence ≤ 35 s | ✅ `TestChaos_PartialPartition` |
| Slow disk / high-latency origin | Hit path ≤ 50 ms | ✅ `TestChaos_SlowOrigin` |
| Rolling restart of all 3 pods | Zero 5xx during restart | ✅ `TestChaos_RollingRestart` |

---

## 2. Infrastructure

### 2.1 How to run

```bash
# Start the 3-node cluster (Docker required)
make integration-cluster-strong   # or eventual / full

# Run chaos tests
make chaos

# Run a 24-hour soak (requires a live cluster at 127.0.0.1:1808{1,2,3})
DURATION_H=24 RPS=5000 make soak

# Run a 72-hour soak at 25 % capacity (single node as proxy)
DURATION_H=72 RPS=2500 NODES=127.0.0.1:18081 make soak
```

### 2.2 Canonical capacity reference

The benchmark establishes the canonical capacity on the CI runner:

```bash
make bench   # writes bench/results/baseline.txt
```

50 % and 25 % capacity figures are calculated from the p50 RPS in
`bench/results/baseline.txt` multiplied by the number of nodes (3).

### 2.3 Soak output

Each `make soak` run writes under `soak-results/TIMESTAMP/`:

| File | Contents |
|------|----------|
| `summary.txt` | Configuration, start/end timestamps, latency summary |
| `latency.csv` | Per-request latencies from `hey` |
| `metrics.json` | Prometheus snapshot of `bouine_*` counters at end |
| `snapshots/*.prom` | Per-minute Prometheus snapshots (RSS trend, hit ratio) |

---

## 3. Chaos scenarios

### 3.1 Peer kill (`TestChaos_PeerKill`)

**Scenario**: 50 keys are populated via node 0. Node 2 is stopped via
`docker compose stop`. Nodes 0 and 1 must still serve all 50 keys with
status 200.

**Pass/fail criterion**: `failures == 0` across 100 requests (50 keys × 2
surviving nodes).

**Why it works**: In strong mode the remaining nodes hold local copies plus
peer-fetch fallback. Origin is consulted on miss; the test origin always
returns 200 so the cache is a best-effort layer here, not the sole path.

### 3.2 Origin flap (`TestChaos_OriginFlap`)

**Scenario**: 20 keys are cached with short TTLs (≤ 5 s). The origin container
is stopped/started 5 times at 500 ms intervals while a goroutine hammers all
keys. The origin server returns `stale-if-error=3600` in its responses.

**Pass/fail criterion**: `errors5xx == 0` across all requests during the flap
window.

**Why it works**: Stale-on-error fallback (ADR-0009 §6) means that once an
object is stale and the origin returns 5xx, the cache falls back to serving
the stale object with `Warning: 110`.

### 3.3 Partial partition (`TestChaos_PartialPartition`)

**Scenario**: One key is populated on all 3 nodes. Node 2 is paused
(`docker compose pause`). A purge is issued from node 0, which node 2 misses.
Node 2 is unpaused; gossip is allowed to converge (`GossipConvergence = 35 s`).

**Pass/fail criterion**:
- Nodes 0 and 1 show MISS immediately after the purge.
- All 3 nodes return 200 after the heal window.

**Why it works**: HTTP fan-out (strong mode) purges nodes 0 and 1
immediately. Gossip delivers the purge to node 2 after it unpauses and
push/pull syncs within the convergence window.

### 3.4 Slow origin (`TestChaos_SlowOrigin`)

**Scenario**: `tc-netem` injects 500 ms latency on the origin container's
`eth0`. One key is warmed through the slow origin. Subsequent requests must
complete in < 50 ms.

**Pass/fail criterion**: All 10 post-warm requests < 50 ms.

**Notes**: This scenario is skipped if `tc-netem` is unavailable in the
container image (some CI environments lack `NET_ADMIN`). The scenario is
labelled as verified when it passes on the reference hardware listed in §5.

### 3.5 Rolling restart (`TestChaos_RollingRestart`)

**Scenario**: 30 keys are populated. All 3 nodes are restarted one at a time
with a 2 s down window and 5 s rejoin time. Background goroutines maintain
steady load across all nodes throughout the restart sequence.

**Pass/fail criterion**: `errors5xx == 0` across all background requests.

This directly validates the Phase 4 exit criterion from PLAN.md:
> "rolling restart of all 3 pods returns zero 5xx in the load-generator timeline."

---

## 4. Known limitations

- `TestChaos_SlowOrigin` requires Linux with `iproute2` in the origin
  container and `NET_ADMIN` capability (not available on macOS or rootless
  Docker without netns). The test self-skips gracefully.
- The chaos tests use the same Docker Compose cluster as
  `test/integration/`; they cannot run concurrently with integration tests
  on the same host.
- The `FlapOrigin` scenario requires the origin to use bouine's
  stale-on-error path (ADR-0009 §6). The test origin at
  `test/integration/origin/` must return `stale-if-error=3600` for the
  `/chaos/flap/*` path family. This is a `TEST_ORIGIN` environment variable
  toggle; see `test/integration/origin/`.

---

## 5. Reference hardware

Full soak runs for v1.0 must be executed on the self-hosted benchmark runner
described in `bench/README.md`:

| Component | Spec |
|-----------|------|
| Machine   | To be filled at v1.0 run |
| CPU       | TBD |
| RAM       | TBD |
| Disk      | TBD |
| OS        | Linux amd64 |
| Docker    | ≥ 24.x with compose v2 |

---

## 6. v1.0 release evidence checklist

Before tagging v1.0 the following artefacts must be attached to the release:

- [ ] `soak-results/24h/summary.txt` — 24-hour soak at 50 % canonical RPS;
  zero 5xx; RSS growth ≤ 5 % steady-state; p99 hit latency ≤ 1 ms.
- [ ] `soak-results/72h/summary.txt` — 72-hour soak at 25 % canonical RPS;
  same criteria.
- [ ] `chaos-results/` — complete `go test -v` output from `make chaos`;
  all tests PASS (or any SKIP must be accompanied by a hardware note).
- [ ] Prometheus graphs (`.png`) showing hit ratio, RSS, p99 latency,
  `bouine_cluster_broadcast_failures_total` = 0 during soak.
- [ ] Brief narrative (≤ 500 words) summarising any anomalies observed,
  actions taken, and sign-off by the release operator.

Until these artefacts are attached, the release tag MUST be a pre-release
(`v1.0-rc.*`) per the branching policy in AGENTS.md §14.3.
