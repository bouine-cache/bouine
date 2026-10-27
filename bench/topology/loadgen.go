package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"
)

// profile selects the request path shape and key distribution.
type profile struct {
	name      string
	singleKey string
	paths     []string
	zipfS     float64
	seed      uint64
}

func newProfile(name string, keys int, zipfS float64, seed uint64) (profile, error) {
	switch name {
	case "hit-heavy":
		p := profile{name: name, zipfS: zipfS, seed: seed}
		p.paths = make([]string, keys)
		for i := range p.paths {
			p.paths[i] = fmt.Sprintf("/ssr/key-%06d", i)
		}
		return p, nil
	case "herd-unstorable":
		return profile{name: name, singleKey: "/nostore/herd-1", zipfS: zipfS, seed: seed}, nil
	default:
		return profile{}, fmt.Errorf("unknown profile %q (see -list)", name)
	}
}

// sampler returns worker w's request path generator. Per-worker Zipf
// streams: same keyspace and parameters, independent samplers
// (math/rand Zipf is not concurrency-safe, and a shared sampler would
// serialize the workers).
func (p profile) sampler(w int) func() string {
	if p.singleKey != "" {
		return func() string { return p.singleKey }
	}
	z := rand.NewZipf(rand.New(rand.NewPCG(p.seed, p.seed+uint64(w)+1)), p.zipfS, 1, uint64(len(p.paths)-1)) //nolint:gosec // G404: reproducible synthetic key popularity — determinism is the point
	return func() string { return p.paths[z.Uint64()] }
}

type outcome uint8

const (
	outHot outcome = iota
	outPeer
	outMiss
	outBypass
	outOther
	outCount
)

var outcomeNames = [outCount]string{"HIT/hot", "HIT/peer", "MISS", "BYPASS", "OTHER"}

const nodesPerTier = 3

// harnessHost is the constant Host header the load generator sends, so
// the three co-located nodes share one cache-key space (one VIP) instead
// of three port-specific ones. The dial target stays pinned to each
// node's loopback address via per-node clients with a custom Dial.
const harnessHost = "www.bench.local"

type recorder struct {
	latBuf [outCount][nodesPerTier][]time.Duration
	counts [outCount][nodesPerTier]atomic.Int64
	latIdx [outCount][nodesPerTier]atomic.Int64
}

func newRecorder(capacity int) *recorder {
	r := &recorder{}
	for o := range outCount {
		for n := range nodesPerTier {
			r.latBuf[o][n] = make([]time.Duration, capacity)
		}
	}
	return r
}

func (r *recorder) reset() {
	for o := range outCount {
		for n := range nodesPerTier {
			r.counts[o][n].Store(0)
			r.latIdx[o][n].Store(0)
		}
	}
}

func (r *recorder) record(node int, o outcome, lat time.Duration) {
	r.counts[o][node].Add(1)
	i := r.latIdx[o][node].Add(1) - 1
	buf := r.latBuf[o][node]
	if int(i) < len(buf) {
		buf[i] = lat
	}
}

// bucket returns sorted latencies for outcome o across all nodes, and the
// per-node count for the per-node readout.
func (r *recorder) bucket(o outcome) (all []time.Duration, perNode [nodesPerTier]int) {
	for n := range nodesPerTier {
		perNode[n] = int(r.counts[o][n].Load())
		all = append(all, r.latBuf[o][n][:min(perNode[n], len(r.latBuf[o][n]))]...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return all, perNode
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p / 100)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

type loadGenConfig struct {
	originStats func() originStats
	peerStats   func() [nodesPerTier]peerFetchCounters
	nodes       []string
	profile     profile
	warmup      time.Duration
	measure     time.Duration
	rate        float64
	workers     int
}

type loadGen struct {
	rec     *recorder
	work    chan int
	done    chan struct{}
	clients []*fasthttp.Client
	cfg     loadGenConfig
	wg      sync.WaitGroup
	dropped atomic.Int64
	errors  atomic.Int64
	non200  atomic.Int64
	sent    atomic.Int64
}

func newLoadGen(cfg loadGenConfig) *loadGen {
	l := &loadGen{
		cfg: cfg,
		rec: newRecorder(int(cfg.rate * cfg.measure.Seconds() * 2 / nodesPerTier)),
	}
	// One client per node with the dial pinned to the node's loopback
	// address: the request Host is the constant harnessHost (so all nodes
	// see one key space), while the TCP dial stays on 127.0.0.1.
	l.clients = make([]*fasthttp.Client, len(cfg.nodes))
	for i, addr := range cfg.nodes {
		u, err := url.Parse(addr)
		if err != nil || u.Host == "" {
			fatal("loadgen: bad node addr %q", addr)
		}
		dialAddr := u.Host
		l.clients[i] = &fasthttp.Client{
			MaxConnsPerHost:          2048 / len(cfg.nodes),
			NoDefaultUserAgentHeader: true,
			ReadBufferSize:           64 * 1024,
			Dial: func(addr string) (net.Conn, error) { //nolint:noctx // ctx-less fasthttp hook; the dial below is context-bearing
				_ = addr
				var d net.Dialer
				return d.DialContext(context.Background(), "tcp", dialAddr)
			},
		}
	}
	return l
}

func (l *loadGen) start() {
	l.done = make(chan struct{})
	l.work = make(chan int, 8192)
	for w := range l.cfg.workers {
		paths := l.cfg.profile.sampler(w)
		l.wg.Add(1)
		go l.worker(paths)
	}
	l.wg.Add(1)
	go l.dispatch()
}

func (l *loadGen) dispatch() {
	defer l.wg.Done()
	interval := time.Duration(float64(time.Second) / l.cfg.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	i := 0
	for {
		select {
		case <-l.done:
			return
		case <-ticker.C:
			select {
			case l.work <- i % nodesPerTier:
			default:
				l.dropped.Add(1)
			}
			i++
		}
	}
}

func (l *loadGen) worker(paths func() string) {
	defer l.wg.Done()
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	for {
		select {
		case <-l.done:
			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
			return
		case node := <-l.work:
			req.Reset()
			resp.Reset()
			req.SetRequestURI("http://" + harnessHost + paths())
			start := time.Now()
			err := l.clients[node].Do(req, resp)
			lat := time.Since(start)
			if err != nil {
				l.errors.Add(1)
				l.rec.record(node, outOther, lat)
				continue
			}
			if resp.StatusCode() != fasthttp.StatusOK {
				l.non200.Add(1)
				l.rec.record(node, outOther, lat)
				continue
			}
			l.sent.Add(1)
			l.rec.record(node, classify(resp.Header.Peek("X-Cache"), resp.Header.Peek("X-Cache-Source")), lat)
		}
	}
}

var (
	bHIT    = []byte("HIT")
	bMISS   = []byte("MISS")
	bBYPASS = []byte("BYPASS")
	bHot    = []byte("hot")
	bPeer   = []byte("peer")
)

func classify(xc, xcs []byte) outcome {
	switch {
	case bytes.Equal(xc, bHIT) && bytes.Equal(xcs, bHot):
		return outHot
	case bytes.Equal(xc, bHIT) && bytes.Equal(xcs, bPeer):
		return outPeer
	case bytes.Equal(xc, bMISS):
		return outMiss
	case bytes.Equal(xc, bBYPASS):
		return outBypass
	default:
		return outOther
	}
}

func (l *loadGen) stop() {
	close(l.done)
	l.wg.Wait()
}

type measureSnapshot struct {
	wall    time.Time
	ru      syscall.Rusage
	ms      runtime.MemStats
	sent    int64
	dropped int64
	errors  int64
	non200  int64
}

func (l *loadGen) takeSnapshot() measureSnapshot {
	var s measureSnapshot
	runtime.ReadMemStats(&s.ms)
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &s.ru)
	s.wall = time.Now()
	// Outcome and error counters are snapshotted at the same instant as
	// the runtime stats so completed/dropped/errors/non200 are bracketed
	// to the measurement window exactly like Mallocs and CPU: requests
	// completing during collect() must not widen the denominator of
	// share, mallocs/req or cpu_us/req.
	s.sent = l.sent.Load()
	s.dropped = l.dropped.Load()
	s.errors = l.errors.Load()
	s.non200 = l.non200.Load()
	return s
}

// run drives warmup then the measurement window, and returns the window's
// aggregated result. The loadgen runs continuously across both phases; the
// recorder, the outcome/error counters, the origin and peer counters and
// the runtime snapshots bracket only the measurement window — warmup
// requests never dilute the reported shares or per-request derivatives.
func (l *loadGen) run() runResult {
	l.start()
	time.Sleep(l.cfg.warmup)

	l.rec.reset()
	l.sent.Store(0)
	l.dropped.Store(0)
	l.errors.Store(0)
	l.non200.Store(0)
	originBefore := l.cfg.originStats()
	peerBefore := l.cfg.peerStats()
	before := l.takeSnapshot()
	time.Sleep(l.cfg.measure)
	after := l.takeSnapshot()
	peerAfter := l.cfg.peerStats()
	originAfter := l.cfg.originStats()
	result := l.collect(before, after, originBefore, originAfter, peerBefore, peerAfter)

	l.stop()
	return result
}

type runResult struct {
	completed   int64
	dropped     int64
	errors      int64
	non200      int64
	mallocsPer  float64
	allocKBPer  float64
	gcPerSec    float64
	cpuPerReqUs float64
	cpuSec      float64
	origin      originStats
	peerFetch   [nodesPerTier]peerFetchCounters
	perOutcome  [outCount]outcomeResult
	missByNode  [nodesPerTier]nodeLatency
}

type outcomeResult struct {
	count int64
	share float64
	p50   time.Duration
	p90   time.Duration
	p99   time.Duration
	p999  time.Duration
}

type nodeLatency struct {
	count int64
	p50   time.Duration
	p99   time.Duration
}

func (l *loadGen) collect(before, after measureSnapshot, originBefore, originAfter originStats, peerBefore, peerAfter [nodesPerTier]peerFetchCounters) runResult {
	var r runResult
	r.completed = after.sent - before.sent
	r.dropped = after.dropped - before.dropped
	r.errors = after.errors - before.errors
	r.non200 = after.non200 - before.non200
	r.origin.ssrRendered = originAfter.ssrRendered - originBefore.ssrRendered
	r.origin.nostoreSeen = originAfter.nostoreSeen - originBefore.nostoreSeen
	for n := range nodesPerTier {
		r.peerFetch[n] = peerFetchCounters{
			hits:   peerAfter[n].hits - peerBefore[n].hits,
			misses: peerAfter[n].misses - peerBefore[n].misses,
		}
	}
	mallocs := after.ms.Mallocs - before.ms.Mallocs
	allocBytes := after.ms.TotalAlloc - before.ms.TotalAlloc
	if r.completed > 0 {
		r.mallocsPer = float64(mallocs) / float64(r.completed)
		r.allocKBPer = float64(allocBytes) / 1024 / float64(r.completed)
	}
	wallSec := after.wall.Sub(before.wall).Seconds()
	if wallSec > 0 {
		r.gcPerSec = float64(after.ms.NumGC-before.ms.NumGC) / wallSec
	}
	r.cpuSec = cpuSeconds(after.ru) - cpuSeconds(before.ru)
	if r.completed > 0 {
		r.cpuPerReqUs = r.cpuSec * 1e6 / float64(r.completed)
	}
	for o := range outCount {
		lats, _ := l.rec.bucket(outcome(o))
		total := int64(len(lats))
		res := outcomeResult{count: total}
		if r.completed > 0 {
			res.share = float64(total) / float64(r.completed) * 100
		}
		res.p50 = pct(lats, 50)
		res.p90 = pct(lats, 90)
		res.p99 = pct(lats, 99)
		res.p999 = pct(lats, 99.9)
		r.perOutcome[o] = res
	}
	missLats, perNode := l.rec.bucket(outMiss)
	_ = missLats
	for n := range nodesPerTier {
		r.missByNode[n] = nodeLatency{count: int64(perNode[n])}
	}
	// Per-node MISS percentiles need per-node slices, re-derived here.
	for n := range nodesPerTier {
		lats := l.rec.latBuf[outMiss][n][:min(int(perNode[n]), len(l.rec.latBuf[outMiss][n]))]
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		r.missByNode[n].p50 = pct(lats, 50)
		r.missByNode[n].p99 = pct(lats, 99)
	}
	return r
}

func cpuSeconds(ru syscall.Rusage) float64 {
	return float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6 +
		float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
}

// getMaxRSSKB reports peak RSS in KiB. darwin's ru_maxrss is bytes;
// linux's is KiB already.
func getMaxRSSKB() int64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	if runtime.GOOS == "darwin" {
		return ru.Maxrss / 1024
	}
	return ru.Maxrss
}

func printReport(prof profile, rate float64, duration time.Duration, tracing float64, collector string, r runResult) {
	_, _ = fmt.Fprintf(os.Stdout, "=== profile=%s rate=%.0f/s window=%s nodes=%d\n", prof.name, rate, duration, nodesPerTier)
	_, _ = fmt.Fprintf(os.Stdout, "completed=%d dropped=%d errors=%d non200=%d origin_renders(ssr)=%d origin(nostore)=%d\n",
		r.completed, r.dropped, r.errors, r.non200, r.origin.ssrRendered, r.origin.nostoreSeen)
	if tracing >= 0 {
		_, _ = fmt.Fprintf(os.Stdout, "tracing sampling_rate=%.4f collector=%s\n", tracing, collector)
	} else {
		_, _ = fmt.Fprintln(os.Stdout, "tracing off")
	}
	_, _ = fmt.Fprintf(os.Stdout, "mallocs/req=%.1f alloc_kB/req=%.1f gc/s=%.2f cpu_s=%.2f cpu_us/req=%.2f rss_mb=%.1f\n",
		r.mallocsPer, r.allocKBPer, r.gcPerSec, r.cpuSec, r.cpuPerReqUs, float64(getMaxRSSKB())/1024)
	for o := range outCount {
		res := r.perOutcome[o]
		_, _ = fmt.Fprintf(os.Stdout, "%-8s share=%5.1f%% n=%-7d p50=%-8s p90=%-8s p99=%-8s p99.9=%-8s\n",
			outcomeNames[o], res.share, res.count,
			res.p50.Round(time.Microsecond), res.p90.Round(time.Microsecond),
			res.p99.Round(time.Microsecond), res.p999.Round(time.Microsecond))
	}
	for n := range nodesPerTier {
		m := r.missByNode[n]
		_, _ = fmt.Fprintf(os.Stdout, "  MISS node%d n=%-7d p50=%-8s p99=%-8s\n",
			n+1, m.count, m.p50.Round(time.Microsecond), m.p99.Round(time.Microsecond))
	}
	for n := range nodesPerTier {
		pf := r.peerFetch[n]
		_, _ = fmt.Fprintf(os.Stdout, "  peerfetch node%d hits=%-7d misses=%d\n", n+1, pf.hits, pf.misses)
	}
}
