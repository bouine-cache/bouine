# PLAN_LOADTEST.md — Production Load Testing & Competitive Benchmarking

## Goal

Prove bouine is production-ready by measuring its behaviour under
realistic and extreme load, characterising degradation curves, and
comparing head-to-head against NGINX, Varnish, and Envoy on the same
hardware with the same workloads.

The output is a set of **reproducible, publishable benchmark reports**
with charts, tables, and percentile distributions that operators can
use to make deployment decisions.

---

## 1. Tooling

| Tool | Purpose |
|---|---|
| **[vegeta](https://github.com/tsenart/vegeta)** | Constant-rate HTTP load generator (Go); outputs HDR histograms and CSV |
| **[k6](https://k6.io/)** | Scenario-based load testing with ramp-up, stages, thresholds |
| **[wrk2](https://github.com/giltene/wrk2)** | Coordinated-omission-aware latency measurement |
| **[gnuplot](http://www.gnuplot.info/) / [plotly](https://plotly.com/python/)** | Chart generation (latency heatmaps, throughput curves, comparison bars) |
| **[pprof](https://pkg.go.dev/net/http/pprof)** | CPU/memory/goroutine profiling during load |
| **[prometheus](https://prometheus.io/) + grafana** | Live metrics dashboard during test runs |
| **Docker Compose** | Single-node and multi-node topology orchestration |
| **Kubernetes (k3s / kind)** | Cluster topology tests (gossip, fan-out, rolling updates under load) |

All test scripts, configs, and analysis notebooks live under
`bench/loadtest/` in the bouine repo.

---

## 2. Targets Under Test (TUTs)

Each TUT is configured identically (same origin, same cache size, same
number of workers/threads) to ensure apples-to-apples comparison.

| TUT | Version | Config |
|---|---|---|
| **bouine** | `HEAD` (current binary) | YAML config, SIEVE eviction, 1 GiB hot tier |
| **NGINX** | `1.27.x` (latest stable) | `proxy_cache_path`, `proxy_cache_valid`, same pool |
| **Varnish** | `7.6.x` | VCL with same TTL/SWR/SIE policy, malloc 1G |
| **Envoy** | `1.32.x` | HTTP cache filter, same upstream cluster |

### Origin server

The test origin is `test/integration/origin/` (already exists):
configurable latency (`/slow?ms=N`), cache-control headers
(`/hit`, `/miss`, `/bypass`, `/stale`, `/revalidate`, `/vary`),
error responses (`/error`). Extended with:

- `/large?kb=N` — response body of N KiB (memory pressure tests)
- `/unique?n=N` — N unique URLs per prefix (working-set tests)
- `/ttl?s=N` — explicit `max-age=N` (TTL distribution tests)

---

## 3. Test Scenarios

### 3.1 Single-node throughput ramp

**Goal**: Find the RPS ceiling and characterise the latency curve as
load increases.

| Parameter | Value |
|---|---|
| Topology | 1 proxy, 1 origin |
| Workload | 90% `/hit` (cache-hot), 10% `/miss` |
| Ramp | 1k → 5k → 10k → 25k → 50k → 100k RPS (60s per step) |
| Measurements | p50/p95/p99/p999 latency, throughput, error rate, CPU%, RSS |

**Output**: Latency-vs-RPS curve for each TUT on the same chart.

### 3.2 Cache-hit-only baseline

**Goal**: Measure the absolute floor — pure hit-path performance with
zero origin calls.

| Parameter | Value |
|---|---|
| Warmup | Pre-populate 10k keys |
| Workload | 100% hits to pre-populated keys |
| Rate | Fixed 50k RPS, 120s |
| Measurements | p50/p99 latency, allocs/op (pprof), GC pause histogram |

### 3.3 Cache-miss storm

**Goal**: Measure behaviour when the entire working set is uncacheable.

| Parameter | Value |
|---|---|
| Workload | 100% `/miss` (no-store) or 100% unique URLs |
| Rate | 10k RPS, 120s |
| Measurements | Origin connection count, proxy RSS growth, error rate |

### 3.4 Working-set overflow

**Goal**: Measure eviction pressure when the cache is smaller than the
working set.

| Parameter | Value |
|---|---|
| Cache size | 64 MiB hot tier |
| Working set | 50k unique `/large?kb=64` URLs = ~3.2 GiB (50x cache) |
| Rate | 5k RPS, 300s |
| Measurements | Hit ratio over time, eviction rate, p99 latency, RSS |

### 3.5 Vary blow-up

**Goal**: Measure the MaxVariants cap under load.

| Parameter | Value |
|---|---|
| Workload | 1 URL with `Vary: X-Test`, 1000 distinct header values |
| Rate | 1k RPS, 60s |
| Measurements | `bouine_vary_cap_hits_total`, hit ratio, latency |

### 3.6 Mixed realistic workload

**Goal**: Simulate a production traffic mix.

| Parameter | Value |
|---|---|
| Mix | 60% `/hit`, 15% `/miss`, 10% `/stale`, 5% `/revalidate`, 5% `/bypass`, 3% `/vary`, 2% `/error` |
| Rate | 10k RPS sustained, 300s |
| Measurements | Per-category hit ratio, p99 latency, throughput |

---

## 4. Multi-Node Scenarios

### 4.1 Cluster throughput scaling

**Goal**: Measure horizontal scalability.

| Parameter | Value |
|---|---|
| Topology | 1, 2, 3, 5, 10 bouine nodes behind a round-robin LB |
| Workload | Mixed (§3.6), 10k RPS per node |
| Measurements | Aggregate throughput, per-node CPU, peer-fetch hit rate |

**Output**: Throughput scaling curve (ideal linear vs actual).

### 4.2 Gossip convergence under load

**Goal**: Measure how fast membership changes propagate during traffic.

| Parameter | Value |
|---|---|
| Topology | 5-node cluster under 50k total RPS |
| Event | Kill node 3, wait, restart node 3 |
| Measurements | Time to detect leave, time to rejoin, peer-fetch errors during transition, ring rebalance duration |

### 4.3 Peer-fetch pressure

**Goal**: Characterise fan-out overhead.

| Parameter | Value |
|---|---|
| Topology | 3-node cluster, each node owns ~33% of keys |
| Workload | 100% requests to keys NOT owned by the receiving node (force peer-fetch) |
| Rate | 5k → 10k → 25k RPS |
| Measurements | Peer-fetch latency, hop-limit hits, inter-node bandwidth |

### 4.4 Hedged request pressure

**Goal**: Measure hedged fetch overhead and tail-latency improvement.

| Parameter | Value |
|---|---|
| Origin | `/slow?ms=200` with 5% chance of `/slow?ms=2000` (outlier) |
| Config | `hedge_timeout: 250ms` |
| Rate | 5k RPS, 120s |
| Measurements | p99 with/without hedging, origin load amplification factor |

### 4.5 Rolling update under load

**Goal**: Measure zero-downtime during StatefulSet rollout.

| Parameter | Value |
|---|---|
| Topology | 3-node K8s StatefulSet under 10k RPS |
| Event | `kubectl rollout restart` |
| Measurements | Error rate during rollout, latency spike duration, client-visible 5xx count |

---

## 5. Stress & Limit Tests

### 5.1 Connection exhaustion

| Parameter | Value |
|---|---|
| Concurrent connections | 1k → 5k → 10k → 50k |
| Rate | 1 req/conn (no keep-alive) |
| Measurements | Accept latency, FD count, error rate, OOM risk |

### 5.2 Large body streaming

| Parameter | Value |
|---|---|
| Workload | `/large?kb=10240` (10 MiB responses) |
| Rate | 100 RPS (= 1 GiB/s throughput) |
| Measurements | RSS, GC pauses, client timeout rate, origin connection hold time |

### 5.3 Slow origin (backpressure)

| Parameter | Value |
|---|---|
| Origin | `/slow?ms=5000` (5s latency) |
| Rate | 1k RPS |
| Measurements | In-flight request count, goroutine count, RSS, client timeout rate |

### 5.4 Request collapsing under contention

| Parameter | Value |
|---|---|
| Workload | 10k concurrent requests to 1 uncached URL |
| Measurements | Origin request count (should be 1), client p99, collapse queue depth |

### 5.5 Purge/ban broadcast under load

| Parameter | Value |
|---|---|
| Topology | 5-node cluster under 10k RPS |
| Event | Burst of 100 purges/sec for 10s |
| Measurements | Broadcast latency, peer-side purge processing time, cache-hit ratio drop and recovery |

---

## 6. Output & Visualisation

### Per-scenario output files

```
bench/loadtest/results/<scenario>/<tut>/
  raw.csv          # vegeta/k6 raw results
  latency.hdr      # HDR histogram (wrk2 format)
  summary.json     # p50/p95/p99/p999, rps, error_rate, duration
  pprof-cpu.pb.gz  # CPU profile during test
  pprof-heap.pb.gz # Heap profile at end of test
  metrics.prom     # Prometheus scrape snapshot
```

### Comparison charts (generated by `bench/loadtest/plot.py`)

| Chart | Description |
|---|---|
| `latency_vs_rps.svg` | p99 latency (y) vs RPS (x) for all 4 TUTs on one chart |
| `throughput_scaling.svg` | Aggregate RPS vs node count (1–10) |
| `hit_ratio_over_time.svg` | Hit ratio during working-set overflow |
| `latency_heatmap.svg` | Time (x) vs latency bucket (y) colour-coded by density |
| `comparison_bars.svg` | Side-by-side bars: p50/p99/max for each TUT at 10k/50k/100k RPS |
| `gossip_convergence.svg` | Timeline: node leave, detect, rejoin, ring stable |
| `hedging_tail.svg` | CDF of latency with/without hedging |
| `resource_usage.svg` | CPU% and RSS over time during sustained load |
| `collapse_efficiency.svg` | Origin requests vs client requests for collapsible workload |

### Summary report

`bench/loadtest/REPORT.md` — auto-generated Markdown report with
embedded SVG charts, one section per scenario, comparison tables, and
a "verdict" paragraph per scenario stating whether bouine meets the
performance target.

---

## 7. Execution Infrastructure

### Local (single-node scenarios)

```bash
make loadtest-setup     # builds bouine + NGINX + Varnish + Envoy images
make loadtest-single    # runs §3.1–§3.6 on Docker Compose, all 4 TUTs
make loadtest-report    # generates charts + REPORT.md
```

### Kubernetes (multi-node scenarios)

```bash
make loadtest-cluster-setup   # deploys 5-node bouine cluster + LB + origin
make loadtest-cluster         # runs §4.1–§4.5 and §5.1–§5.5
make loadtest-cluster-report  # generates cluster-specific charts
```

### CI integration

A nightly GitHub Actions workflow runs the single-node suite on a
self-hosted runner with CPU affinity, writes results to
`bench/loadtest/results/nightly/`, and opens an issue if any scenario
regresses beyond the defined thresholds.

---

## 8. Success Criteria

| Metric | Target |
|---|---|
| Single-node p99 at 10k RPS (hit-only) | ≤ 1ms |
| Single-node p99 at 50k RPS (mixed) | ≤ 5ms |
| Single-node ceiling RPS (hit-only) | ≥ 80% of NGINX, ≥ 100% of Varnish |
| Throughput scaling (3 nodes) | ≥ 2.7x single-node (90% linear) |
| Gossip convergence (5 nodes) | ≤ 5s detect + rejoin |
| Rolling update error rate | 0 client-visible 5xx |
| Request collapsing efficiency | 1 origin request for N concurrent clients |
| Hedged fetch p99 improvement | ≥ 3x reduction vs non-hedged on outlier workload |
| RSS at steady state (10k RPS, 1 GiB cache) | ≤ 2 GiB |
| Zero allocs/op on hit path | confirmed by pprof during load |

---

## 9. Implementation Order

| Step | Scope | Effort |
|---|---|---|
| 1 | Install vegeta + k6 + wrk2; scaffold `bench/loadtest/` dir | 1h |
| 2 | Docker Compose with bouine + NGINX + Varnish + Envoy + origin | 4h |
| 3 | Implement §3.1–§3.2 (single-node throughput + hit-only baseline) | 3h |
| 4 | Implement §3.3–§3.6 (miss storm, overflow, vary, mixed) | 4h |
| 5 | Build `plot.py` for comparison charts | 3h |
| 6 | K8s multi-node setup for §4.x scenarios | 4h |
| 7 | Implement §4.1–§4.5 (cluster scaling, gossip, rolling update) | 6h |
| 8 | Implement §5.1–§5.5 (stress/limit tests) | 4h |
| 9 | Auto-generate REPORT.md | 2h |
| 10 | CI nightly workflow | 2h |

**Total: ~33h** — split across 3–4 focused sessions.
