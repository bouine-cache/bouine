// Command topology drives an in-process 3-node strong-cluster measurement
// harness for bench-level load experiments that the single-node bench/load
// setups cannot express: peer-hit latency and allocation cost under steady
// load (F1), tracing overhead on the slow path at several sampling rates
// (F4), and the wasted peer RPC on remote-owned unstorable keys (F12).
//
// All components (nodes, origins, load generator, fake OTLP collector) are
// co-located in one process, so absolute RPS is not comparable to the pinned
// Linux bench runner; per-outcome latency deltas, outcome shares, origin
// counts and allocation deltas are robust to co-location. The boot technique
// mirrors test/integration/driver (cobra entrypoint, real YAML configs, real
// listeners) but carries no testing.T dependency.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

const harnessToken = "topology-bench-token" //nolint:gosec // G101: synthetic bench credential for a throwaway in-process cluster, never a real secret

func main() {
	var (
		profile   = flag.String("profile", "hit-heavy", "load profile: hit-heavy | herd-unstorable")
		rate      = flag.Float64("rate", 1000, "target arrival rate (req/s)")
		duration  = flag.Duration("duration", 15*time.Second, "measurement window (after warmup)")
		warmup    = flag.Duration("warmup", 10*time.Second, "warmup window, excluded from results")
		tracing   = flag.Float64("tracing", -1, "tracing sampling_rate; negative disables tracing")
		collector = flag.String("collector", "ok", "fake OTLP collector mode: ok | slow | down")
		keys      = flag.Int("keys", 2000, "keyspace size (hit-heavy)")
		zipfS     = flag.Float64("zipf", 1.1, "Zipf s parameter")
		seed      = flag.Uint64("seed", 42, "PRNG seed (per-worker streams derive from it)")
		workers   = flag.Int("workers", 192, "load generator worker goroutines")
		list      = flag.Bool("list", false, "list profiles and exit")
		memprof   = flag.String("memprofile", "", "write an allocation profile to this path after the run")
		near      = flag.Bool("near", false, "enable experimental.near_cache on every node (64MiB / 30s) — ADR-0071")
	)
	flag.Parse()

	if *list {
		_, _ = fmt.Fprintln(os.Stdout, "profiles: hit-heavy, herd-unstorable")
		return
	}

	// GOMAXPROCS=10 pins the same CPU budget the audit baselines used, so
	// co-location contention is comparable across runs; the engine also
	// reads the env explicitly (cgroup-aware when unset).
	_ = os.Setenv("GOMAXPROCS", "10") // best-effort hint for the engine's env read
	runtime.GOMAXPROCS(10)

	prof, err := newProfile(*profile, *keys, *zipfS, *seed)
	if err != nil {
		fatal("profile: %v", err)
	}

	origin, err := startOrigin()
	if err != nil {
		fatal("origin: %v", err)
	}
	defer origin.stop()
	_, _ = fmt.Fprintln(os.Stdout, "[harness] origin up:", origin.addr)

	var coll *fakeCollector
	if *tracing >= 0 {
		coll, err = startCollector(*collector)
		if err != nil {
			fatal("collector: %v", err)
		}
		defer coll.stop()
		_, _ = fmt.Fprintf(os.Stdout, "[harness] collector %s up: %s\n", *collector, collAddr(coll))
	}

	t, err := bootTier(origin.addr, tracingSpec{rate: *tracing, endpoint: collAddr(coll)}, *near)
	if err != nil {
		fatal("tier: %v", err)
	}
	defer t.down()
	_, _ = fmt.Fprintln(os.Stdout, "[harness] 3-node tier up")

	lg := newLoadGen(loadGenConfig{
		nodes:       t.dataAddrs(),
		profile:     prof,
		rate:        *rate,
		warmup:      *warmup,
		measure:     *duration,
		workers:     *workers,
		originStats: origin.stats,
		peerStats:   t.peerFetchCounters,
	})
	result := lg.run()

	if *memprof != "" {
		f, ferr := os.Create(*memprof)
		if ferr != nil {
			fatal("memprofile: %v", ferr)
		}
		if ferr := pprof.Lookup("allocs").WriteTo(f, 0); ferr != nil {
			fatal("memprofile: %v", ferr)
		}
		_ = f.Close()
	}

	printReport(prof, *rate, *duration, *tracing, *collector, result)
	if *near {
		_, _ = fmt.Fprintln(os.Stdout, "near-cache: enabled (experimental.near_cache: 64MiB / 30s)")
	}

	if coll != nil && coll.srv != nil {
		_, _ = fmt.Fprintf(os.Stdout, "[collector] exports=%d bytes=%d\n", coll.exports.Load(), coll.bytes.Load())
	}
}
