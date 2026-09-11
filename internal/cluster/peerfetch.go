package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/transport"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// peerFetchEncodePool reuses []byte buffers for encoding cached objects
// in peer-fetch responses. Buffers larger than 64 KiB are discarded to
// prevent the pool from pinning oversized buffers.
var peerFetchEncodePool = sync.Pool{
	New: func() any { b := make([]byte, 0, 4096); return &b },
}

// peerFetchBinaryVersion is the version byte for the binary peer-fetch
// request format. v2 uses 16-byte (128-bit) keys. Must not collide with
// JSON's '{' (0x7B).
const peerFetchBinaryVersion = 2

const (
	// PeerFetchPath is the HTTP path for peer cache lookups.
	PeerFetchPath = "/v1/peer/fetch"
	// PeerPutPath is the HTTP path for write-to-owner RPCs: a non-owner
	// that fetched from origin forwards the object to the owner so
	// subsequent peer-fetches hit (issue #509).
	PeerPutPath = "/v1/peer/put"
	// MaxHops is the default maximum number of peers a single request
	// may traverse before going to origin (threat T36).
	MaxHops = 2
	// BouineHopHeader carries the current hop count for loop detection.
	BouineHopHeader = header.BouineHop
	// ClusterVersionHeader carries the cluster protocol version for
	// negotiation during rolling upgrades.
	ClusterVersionHeader = header.XBouineClusterVersion
	// ClusterProtocolVersion is the current protocol version.
	// Bumped to "3" in issue #187: the peer-fetch response format
	// changed from JSON to the binary storage codec. A mixed v2/v3
	// cluster fails detectably on version-mismatch instead of
	// silently producing codec decode errors.
	ClusterProtocolVersion = "3"
	// PeerFetchTimeout is the maximum time for a peer-fetch or peer-put RPC.
	// Exported so the engine wiring can reuse the same budget for the
	// write-to-owner goroutine (issue #509).
	PeerFetchTimeout = 500 * time.Millisecond
	// defaultPeerFetchConcurrency bounds concurrent peer-fetch RPCs to
	// prevent memory blow-up during miss fan-out (issue #133).
	defaultPeerFetchConcurrency = 4
	// MaxPeerFetchConcurrency is the exported upper bound for the
	// configurable fetch/put concurrency (cluster.peer_fetch_concurrency).
	// The config loader uses it for validation; each in-flight peer fetch
	// holds a goroutine and buffers up to maxPeerFetchBytes (64 MiB) while
	// decoding, so the cap bounds worst-case decode memory.
	MaxPeerFetchConcurrency = 128
)

// maxPeerFetchBytes caps the response body read from a peer during
// binary decode. A compromised peer could send an arbitrarily large
// payload; without a limit io.ReadAll buffers unbounded data on
// the heap. This is the total encoded payload limit (metadata + body).
const maxPeerFetchBytes int64 = 64 << 20

// Default pipelining configuration (issue #521, Phase 6.4).
const (
	defaultPeerMaxConnsPerHost = 8
	// defaultPeerMaxIdleConnDur must stay below the peer's admin-server
	// idle timeout (default 300s, internal/admin/server.go). The client
	// must close idle connections before the server reaps them,
	// otherwise the first request on a reaped connection fails with EOF
	// or broken pipe and the fetch falls back to origin
	// (internal/config/config.go documents the same invariant for
	// listen.idle_timeout).
	defaultPeerMaxIdleConnDur = 120 * time.Second
	// peerMaxPendingRequests is the maximum number of pending pipelined
	// requests per connection. With 8 connections × 16 pending = 128
	// concurrent peer fetches per peer, matching the old HTTP/2 capacity.
	peerMaxPendingRequests = 16
)

// Peer address breaker defaults. A peer address that fails
// consecutive RPCs (dial timeouts against a peer that died without a
// ring update — e.g. a pod restart whose NotifyUpdate was never
// delivered) is blacklisted for a cooldown so fetches fail fast and
// fall back to origin instead of paying a full dial timeout per
// request.
const (
	defaultPeerFailureThreshold = 3
	defaultPeerFailureCooldown  = 30 * time.Second
)

// ErrPeerBlacklisted is returned by Fetch and Put when the target
// address is in cooldown after consecutive failures. Callers treat it
// like any other peer-fetch error: fall back to origin.
var ErrPeerBlacklisted = errors.New("peer address blacklisted after consecutive failures")

// errPeerAddrRetired is returned by a parked dial once the fetcher
// closes. It reports itself as a timeout so fasthttp's pipeline worker
// throttles its restart loop (1s sleep) instead of spinning while the
// process drains.
var errPeerAddrRetired = &timeoutError{errors.New("peer address retired")}

type timeoutError struct{ error }

func (e *timeoutError) Error() string   { return e.error.Error() }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

// addrFailureState tracks consecutive transport failures for one peer
// address and the cooldown deadline once the threshold trips.
type addrFailureState struct {
	trippedUntil time.Time
	consecutive  int
}

// PeerFetcherConfig configures a PeerFetcher.
type PeerFetcherConfig struct {
	TLSConfig           *tls.Config
	HopLimit            int
	MaxConnsPerHost     int
	MaxIdleConnDuration time.Duration
	// FetchConcurrency bounds concurrent peer-fetch and peer-put RPCs.
	// Zero applies defaultPeerFetchConcurrency. Negative values are
	// rejected by the config loader (cluster.peer_fetch_concurrency).
	FetchConcurrency int
	// FailureThreshold is the number of consecutive transport failures
	// before an address is blacklisted. Zero applies
	// defaultPeerFailureThreshold.
	FailureThreshold int
	// FailureCooldown is how long a blacklisted address stays tripped
	// before the fetcher probes it again. Zero applies
	// defaultPeerFailureCooldown.
	FailureCooldown time.Duration
}

// PeerFetcher issues cache-lookup RPCs to peer nodes using HTTP/1.1
// pipelining (fasthttp.PipelineClient) for efficient connection reuse.
// Each peer address gets its own PipelineClient with a small number
// of connections (default 8) that pipeline up to 16 concurrent requests
// per connection, reducing connection pool memory by ~85% compared to
// the non-pipelined approach.
//
// Stable.
type PeerFetcher struct {
	pDuration prometheus.Observer
	pHopLimit prometheus.Counter
	pMisses   prometheus.Counter
	pHits     prometheus.Counter
	// pActive is the current number of in-flight peer-fetch RPCs.
	// Detects queue buildup before timeouts appear.
	pActive prometheus.Gauge
	// pBlacklisted reports the number of peer addresses currently in
	// breaker cooldown, when a registry was passed.
	pBlacklisted prometheus.Gauge
	// breaker is the per-address failure breaker: consecutive transport
	// failures trip a cooldown so dead peer addresses fail fast
	// instead of paying a dial timeout per RPC.
	breaker map[string]*addrFailureState
	// Prometheus counters — registered if a non-nil registry is passed.
	logger observability.Logger
	// putSem bounds concurrent write-to-owner RPCs to prevent memory
	// blow-up during miss fan-out (same rationale as fetchSem, issue #509).
	putSem    chan struct{}
	tlsConfig *tls.Config
	fetchSem  chan struct{}
	// pipelineClients caches one PipelineClient per peer address. Held
	// behind an atomic.Pointer so Close can drop the whole map without
	// racing concurrent Fetch/Put lookups: swapping a bare sync.Map field
	// is a non-atomic struct write against readers (data race caught by
	// the nightly -race integration run, TestTLS_CertRotation). nil means
	// the fetcher is closed — callers fail fast and fall back to origin.
	pipelineClients atomic.Pointer[sync.Map] // map[string]*fasthttp.PipelineClient
	// retiredAddrs holds addresses whose cached PipelineClient was
	// evicted because the address is no longer a live peer's current
	// address (ring prune or peer restart). Dials for these addresses
	// park instead of dialing: fasthttp's pipeline worker cannot be
	// stopped once its dial fails (no Close API; it exits only after a
	// successful dial plus idle retire), so without the guard a worker
	// whose peer died would re-dial the dead address every ~3s forever,
	// logging "error in PipelineClient" for the life of the process.
	//
	// done is closed by Close (guarded by doneClosed) to release dials
	// parked on retired addresses. Both sit with the pointer-carrying
	// fields so the GC-scan prefix stays compact.
	done         chan struct{}
	retiredAddrs sync.Map // set[string]
	latSumMs     atomic.Int64
	maxBodyBytes int64
	// pipelining configuration (Phase 6.4).
	maxConnsPerHost     int
	maxIdleConnDuration time.Duration
	// breaker tuning: consecutive-failure threshold and cooldown.
	breakerMu        sync.Mutex
	failureThreshold int
	failureCooldown  time.Duration
	hopLimitHits     atomic.Int64
	latN             atomic.Int64
	misses           atomic.Int64
	hits             atomic.Int64
	hopLimit         int
	useTLS           bool
	doneClosed       atomic.Bool
}

// PeerFetchStats returns a snapshot of peer fetch telemetry.
func (f *PeerFetcher) PeerFetchStats() (hits, misses, hopLimitHits, latN, latSumMs int64) {
	return f.hits.Load(), f.misses.Load(), f.hopLimitHits.Load(), f.latN.Load(), f.latSumMs.Load()
}

// Close drops the pipeline client map so idle cluster connections are
// collected. Should be called during shutdown so that rolling restarts
// don't leave TIME_WAIT sockets on peers. Race-free against concurrent
// Fetch/Put: the map pointer is swapped atomically and later lookups see
// nil (closed) instead of torn sync.Map state. In-flight RPCs that
// already loaded the old map complete on their own goroutines; the
// PipelineClients' own idle timeouts reclaim their sockets.
func (f *PeerFetcher) Close(_ context.Context) error {
	if f.doneClosed.CompareAndSwap(false, true) {
		close(f.done)
	}
	f.pipelineClients.Store(nil)
	return nil
}

// RetireAddress evicts the PipelineClient cached for addr and parks
// its dialing worker. Call when addr stops being a live peer's current
// address: a peer that left the ring (prune, NotifyLeave) or a peer
// that restarted at a new address (ring refresh). The eviction means
// later traffic for a returned address transparently gets a fresh
// client; the parked worker — which fasthttp offers no way to stop —
// costs one parked goroutine until process exit instead of a dial
// attempt and a log line every ~3s.
func (f *PeerFetcher) RetireAddress(addr string) {
	if addr == "" {
		return
	}
	f.retiredAddrs.Store(addr, struct{}{})
	if clients := f.pipelineClients.Load(); clients != nil {
		clients.Delete(addr)
	}
}

// NewPeerFetcher creates a PeerFetcher. tlsCfg must have the cluster
// mTLS credentials. If nil a plain HTTP client is used (test-only).
// reg, if non-nil, receives Prometheus metric registration.
// hopLimit caps the number of peers a request may traverse; 0 uses MaxHops.
func NewPeerFetcher(tlsCfg *tls.Config, reg prometheus.Registerer, hopLimit int) *PeerFetcher {
	return NewPeerFetcherWithConfig(PeerFetcherConfig{
		TLSConfig: tlsCfg,
		HopLimit:  hopLimit,
	}, reg, nil)
}

// NewPeerFetcherWithLogger creates a PeerFetcher with a structured logger.
func NewPeerFetcherWithLogger(tlsCfg *tls.Config, reg prometheus.Registerer, logger observability.Logger, hopLimit int) *PeerFetcher {
	return NewPeerFetcherWithConfig(PeerFetcherConfig{
		TLSConfig: tlsCfg,
		HopLimit:  hopLimit,
	}, reg, logger)
}

// NewPeerFetcherWithConfig creates a PeerFetcher with full pipelining
// configuration. MaxConnsPerHost and MaxIdleConnDuration default to 8
// and 120s respectively when zero.
func NewPeerFetcherWithConfig(cfg PeerFetcherConfig, reg prometheus.Registerer, logger observability.Logger) *PeerFetcher {
	hopLimit := cfg.HopLimit
	if hopLimit <= 0 {
		hopLimit = MaxHops
	}
	maxConns := cfg.MaxConnsPerHost
	if maxConns <= 0 {
		maxConns = defaultPeerMaxConnsPerHost
	}
	maxIdle := cfg.MaxIdleConnDuration
	if maxIdle <= 0 {
		maxIdle = defaultPeerMaxIdleConnDur
	}
	fetchConcurrency := cfg.FetchConcurrency
	if fetchConcurrency <= 0 {
		fetchConcurrency = defaultPeerFetchConcurrency
	}
	if fetchConcurrency > MaxPeerFetchConcurrency {
		fetchConcurrency = MaxPeerFetchConcurrency
	}
	failureThreshold := cfg.FailureThreshold
	if failureThreshold <= 0 {
		failureThreshold = defaultPeerFailureThreshold
	}
	failureCooldown := cfg.FailureCooldown
	if failureCooldown <= 0 {
		failureCooldown = defaultPeerFailureCooldown
	}
	f := &PeerFetcher{
		useTLS:              cfg.TLSConfig != nil,
		hopLimit:            hopLimit,
		maxBodyBytes:        maxPeerFetchBytes,
		fetchSem:            make(chan struct{}, fetchConcurrency),
		putSem:              make(chan struct{}, fetchConcurrency),
		logger:              observability.ResolveLogger(logger),
		maxConnsPerHost:     maxConns,
		maxIdleConnDuration: maxIdle,
		tlsConfig:           cfg.TLSConfig,
		breaker:             make(map[string]*addrFailureState),
		failureThreshold:    failureThreshold,
		failureCooldown:     failureCooldown,
		done:                make(chan struct{}),
	}
	f.pipelineClients.Store(&sync.Map{})
	if reg != nil {
		f.pHits = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine", Name: "peer_fetch_hits_total",
			Help: "Cache objects served from a cluster peer (L0 promotion).",
		})
		f.pMisses = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine", Name: "peer_fetch_misses_total",
			Help: "Peer-fetch RPCs that returned a miss; fell through to origin.",
		})
		f.pHopLimit = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine", Name: "peer_fetch_hop_limit_hits_total",
			Help: "Peer-fetch attempts aborted because MaxHops was reached.",
		})
		dur := prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:                       "bouine",
			Name:                            "peer_fetch_duration_seconds",
			Help:                            "Round-trip time for successful peer-fetch RPCs. Also exposed as a native (sparse-bucket) histogram; the classic _bucket series stay on the wire until a metric_relabel_configs rule drops them (see docs/runbook/native-histogram.md).",
			Buckets:                         []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  80,
			NativeHistogramMinResetDuration: time.Hour,
		})
		f.pDuration = dur
		f.pActive = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bouine", Name: "peer_fetch_active",
			Help: "Current number of in-flight peer-fetch RPCs. A rising value indicates queue buildup before timeouts appear.",
		})
		f.pBlacklisted = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bouine", Name: "peer_addr_blacklisted",
			Help: "Peer addresses currently in cooldown after consecutive transport failures.",
		})
		reg.MustRegister(f.pHits, f.pMisses, f.pHopLimit, dur, f.pActive, f.pBlacklisted)
	}
	return f
}

// getPipelineClient returns the PipelineClient for the given peer
// address, creating one on first use, or nil once the fetcher is closed
// (Close dropped the map). Each PipelineClient maintains a small pool
// of pipelined connections (default 8) that can handle up to 16
// concurrent in-flight requests per connection, matching the old HTTP/2
// capacity with ~85% less connection pool memory.
func (f *PeerFetcher) getPipelineClient(addr string) *fasthttp.PipelineClient {
	clients := f.pipelineClients.Load()
	if clients == nil {
		return nil // closed during shutdown
	}
	// A retired address reaching here means it is a routing target
	// again: drop the retired mark so the fresh client below dials
	// normally, and never hand back the evicted client whose worker
	// may already be parked on the retired mark.
	if _, retired := f.retiredAddrs.Load(addr); retired {
		clients.Delete(addr)
		f.retiredAddrs.Delete(addr)
	}
	if v, ok := clients.Load(addr); ok {
		return v.(*fasthttp.PipelineClient)
	}
	pc := &fasthttp.PipelineClient{
		Addr:                          addr,
		MaxConns:                      f.maxConnsPerHost,
		MaxPendingRequests:            peerMaxPendingRequests,
		MaxIdleConnDuration:           f.maxIdleConnDuration,
		ReadTimeout:                   PeerFetchTimeout,
		WriteTimeout:                  5 * time.Minute,
		IsTLS:                         f.useTLS,
		TLSConfig:                     f.tlsConfig,
		DisableHeaderNamesNormalizing: true,
		Dial: func(addr string) (net.Conn, error) {
			if _, retired := f.retiredAddrs.Load(addr); retired {
				// Park instead of dialing: this client was evicted by
				// RetireAddress, and returning a dial error would send
				// fasthttp's worker into its restart loop, re-dialing
				// the dead address forever. Block until the fetcher
				// closes; the goroutine dies with the process, bounded
				// by one per retired address.
				<-f.done
				return nil, errPeerAddrRetired
			}
			return (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial("tcp", addr)
		},
	}
	actual, _ := clients.LoadOrStore(addr, pc)
	return actual.(*fasthttp.PipelineClient)
}

// peerAddr returns the address (with scheme context) for a peer.
func peerAddr(peer api.PeerInfo) string {
	addr := peer.AdminAddr
	if addr == "" {
		addr = peer.Addr
	}
	return addr
}

// breakerAllowed reports whether addr may be dialed now. A blacklisted
// address becomes allowed again once its cooldown lapses — the next
// request then probes the address (the peer may have come back); if it
// fails again the retained consecutive count re-trips the breaker
// immediately, so a still-dead address costs at most one dial per
// cooldown.
func (f *PeerFetcher) breakerAllowed(addr string) bool {
	f.breakerMu.Lock()
	defer f.breakerMu.Unlock()
	st, ok := f.breaker[addr]
	if !ok || st.trippedUntil.IsZero() {
		return true
	}
	if time.Now().Before(st.trippedUntil) {
		return false
	}
	st.trippedUntil = time.Time{}
	f.updateBlacklistedLocked()
	return true
}

// recordPeerSuccess clears the failure state for addr: any successful
// round trip (including a 404 miss — the peer is reachable) proves the
// address healthy.
func (f *PeerFetcher) recordPeerSuccess(addr string) {
	f.breakerMu.Lock()
	defer f.breakerMu.Unlock()
	if _, ok := f.breaker[addr]; !ok {
		return
	}
	delete(f.breaker, addr)
	f.updateBlacklistedLocked()
}

// recordPeerFailure counts one transport failure for addr and trips the
// cooldown once the configured threshold of consecutive failures is
// reached.
func (f *PeerFetcher) recordPeerFailure(addr string) {
	f.breakerMu.Lock()
	defer f.breakerMu.Unlock()
	st := f.breaker[addr]
	if st == nil {
		st = &addrFailureState{}
		f.breaker[addr] = st
	}
	st.consecutive++
	if st.consecutive >= f.failureThreshold && st.trippedUntil.IsZero() {
		st.trippedUntil = time.Now().Add(f.failureCooldown)
		f.logger.Warn("peer address blacklisted after consecutive failures",
			"addr", addr, "consecutive", st.consecutive, "cooldown", f.failureCooldown.String())
	}
	f.updateBlacklistedLocked()
}

// updateBlacklistedLocked refreshes the blacklisted-address gauge. The
// breaker mutex must be held.
func (f *PeerFetcher) updateBlacklistedLocked() {
	if f.pBlacklisted == nil {
		return
	}
	now := time.Now()
	n := 0
	for _, st := range f.breaker {
		if !st.trippedUntil.IsZero() && now.Before(st.trippedUntil) {
			n++
		}
	}
	f.pBlacklisted.Set(float64(n))
}

// buildPeerRequest constructs a fasthttp.Request for a peer-fetch RPC.
func buildPeerRequest(peer api.PeerInfo, req api.PeerFetchRequest, useTLS bool) *fasthttp.Request {
	fetchAddr := peer.AdminAddr
	if fetchAddr == "" {
		fetchAddr = peer.Addr
	}
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	uri := scheme + "://" + fetchAddr + PeerFetchPath

	body := make([]byte, 0, 18+len(req.VaryKey))
	body = append(body, peerFetchBinaryVersion)
	body = append(body, req.Key[:]...)
	body = append(body, byte(len(req.VaryKey))) //nolint:gosec // VaryKey is a short variant key, always < 256 bytes
	body = append(body, req.VaryKey...)

	httpReq := fasthttp.AcquireRequest()
	httpReq.Header.SetMethod(fasthttp.MethodPost)
	httpReq.SetRequestURI(uri)
	httpReq.SetBodyRaw(body)
	httpReq.Header.Set(header.ContentType, "application/octet-stream")
	httpReq.Header.Set(BouineHopHeader, fmt.Sprintf("%d", req.Hops))
	httpReq.Header.Set(ClusterVersionHeader, ClusterProtocolVersion)
	return httpReq
}

// Fetch asks a peer for a cached object. varyKey, when non-empty, is the
// requesting node's Vary assertion for key (RFC 9111 §4.1): the peer must
// only return an object stored under that same variant dimension — a peer
// whose only entry for key is the primary-key Vary resolver answers with a
// miss instead of another variant's body. Returns nil, nil on a cache
// miss at the peer; returns an error only on network/protocol failure.
//
//nolint:gocyclo // 16: hop/error/decode branches are inherently branchy
func (f *PeerFetcher) Fetch(ctx context.Context, peer api.PeerInfo, req api.PeerFetchRequest) (*api.Object, error) {
	if req.Hops >= f.hopLimit {
		f.hopLimitHits.Add(1)
		if f.pHopLimit != nil {
			f.pHopLimit.Inc()
		}
		return nil, nil // hop limit reached — go to origin
	}
	req.Hops++

	httpReq := buildPeerRequest(peer, req, f.useTLS)
	defer fasthttp.ReleaseRequest(httpReq)

	addr := peerAddr(peer)
	if !f.breakerAllowed(addr) {
		return nil, fmt.Errorf("peer fetch %s: %w", peer.Addr, ErrPeerBlacklisted)
	}

	select {
	case f.fetchSem <- struct{}{}:
		defer func() { <-f.fetchSem }()
		if f.pActive != nil {
			f.pActive.Inc()
			defer f.pActive.Dec()
		}
	case <-ctx.Done():
		return nil, fmt.Errorf("peer fetch %s: %w", peer.Addr, ctx.Err())
	}

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	start := time.Now()
	pc := f.getPipelineClient(addr)
	if pc == nil {
		return nil, fmt.Errorf("peer fetch %s: fetcher closed during shutdown", peer.Addr)
	}
	if err := transport.PipelineDo(ctx, pc, httpReq, resp); err != nil {
		// A canceled caller context is not a peer failure — do not
		// count it toward the breaker.
		if ctx.Err() == nil {
			f.recordPeerFailure(addr)
		}
		return nil, fmt.Errorf("peer fetch %s: %w", peer.Addr, err)
	}
	f.recordPeerSuccess(addr)

	if resp.StatusCode() == fasthttp.StatusNotFound {
		f.misses.Add(1)
		if f.pMisses != nil {
			f.pMisses.Inc()
		}
		f.logger.Info("peer fetch miss",
			"key", req.Key, "peer", peer.Addr, "hops", req.Hops)
		return nil, nil // peer miss
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		return nil, fmt.Errorf("peer fetch %s: status %d", peer.Addr, resp.StatusCode())
	}

	// Binary object codec — no JSON, no base64, no reflection (issue #187).
	respBody := resp.Body()
	if int64(len(respBody)) > f.maxBodyBytes {
		return nil, fmt.Errorf("peer fetch %s: response too large", peer.Addr)
	}
	obj, err := storage.DecodeObject(respBody)
	if err != nil {
		return nil, fmt.Errorf("peer fetch decode: %w", err)
	}
	// DecodeObject returns an Object whose Body aliases respBody.
	// We must copy the body — resp is released after this function
	// returns, which would invalidate obj.Body.
	obj.Body = append([]byte(nil), obj.Body...)

	f.hits.Add(1)
	if f.pHits != nil {
		f.pHits.Inc()
	}
	latMs := time.Since(start).Milliseconds()
	f.latSumMs.Add(latMs)
	f.latN.Add(1)
	if f.pDuration != nil {
		f.pDuration.Observe(float64(latMs) / 1000)
	}
	f.logger.Info("peer fetch hit",
		"key", req.Key, "peer", peer.Addr, "hops", req.Hops,
		"dur_ms", latMs)
	return obj, nil
}

// PeerFetchHandler is a fasthttp.RequestHandler that serves peer-fetch
// requests from the local store. Mount on PeerFetchPath.
type PeerFetchHandler struct {
	store    PeerStore
	logger   observability.Logger
	hopLimit int
}

// PeerStore is the minimal storage interface needed by peer fetch.
// It is satisfied by storage.Store.
type PeerStore interface {
	Get(ctx context.Context, key api.Key) (*api.Object, api.Source, error)
}

// NewPeerFetchHandler creates a peer-fetch handler backed by store.
// hopLimit caps the hop count for incoming requests; 0 uses MaxHops.
func NewPeerFetchHandler(store PeerStore, hopLimit int) *PeerFetchHandler {
	return NewPeerFetchHandlerWithLogger(store, nil, hopLimit)
}

// NewPeerFetchHandlerWithLogger creates a peer-fetch handler with a
// structured logger.
func NewPeerFetchHandlerWithLogger(store PeerStore, logger observability.Logger, hopLimit int) *PeerFetchHandler {
	if hopLimit <= 0 {
		hopLimit = MaxHops
	}
	return &PeerFetchHandler{store: store, hopLimit: hopLimit, logger: observability.ResolveLogger(logger)}
}

// parsePeerFetchBody decodes the peer-fetch request body: binary framing
// (v2) or the legacy JSON fallback. ok=false maps to a 400 response.
func parsePeerFetchBody(body []byte) (api.PeerFetchRequest, bool) {
	var req api.PeerFetchRequest
	switch body[0] {
	case peerFetchBinaryVersion:
		if len(body) < 18 {
			return req, false
		}
		copy(req.Key[:], body[1:17])
		varyLen := int(body[17])
		if len(body) < 18+varyLen {
			return req, false
		}
		req.VaryKey = string(body[18 : 18+varyLen])
		return req, true
	case '{':
		if err := json.Unmarshal(body, &req); err != nil {
			return req, false
		}
		return req, true
	default:
		return req, false
	}
}

// Handle is the fasthttp.RequestHandler for peer fetch requests.
func (h *PeerFetchHandler) Handle(ctx *fasthttp.RequestCtx) {
	if !bytes.Equal(ctx.Method(), []byte(fasthttp.MethodPost)) {
		ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
		return
	}

	// Hop-limit guard (T36).
	hopBytes := ctx.Request.Header.Peek(BouineHopHeader)
	hops := 0
	if len(hopBytes) > 0 {
		if parsed, err := strconv.Atoi(string(hopBytes)); err == nil {
			hops = parsed
			if hops >= h.hopLimit {
				ctx.Error("hop limit", fasthttp.StatusLoopDetected)
				return
			}
		}
	}

	body := ctx.PostBody()
	if len(body) == 0 {
		ctx.Error("read error", fasthttp.StatusBadRequest)
		return
	}

	req, ok := parsePeerFetchBody(body)
	if !ok {
		ctx.Error("bad request", fasthttp.StatusBadRequest)
		return
	}

	obj, _, err := h.store.Get(ctx, req.Key)
	if err != nil || obj == nil {
		h.logger.Info("served peer fetch miss", "key", req.Key, "hops", hops)
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return
	}
	// Vary assertion (RFC 9111 §4.1): when the requester asked for a
	// specific variant and the only stored entry under req.Key is the
	// primary-key Vary resolver (filled by a different selecting-header
	// set), answer with a miss. Returning the resolver's body would serve
	// one variant's content to a request selecting another — the
	// cross-market body served in production before this gate.
	if req.VaryKey != "" && obj.VaryKey != req.VaryKey {
		h.logger.Info("served peer fetch miss: variant mismatch",
			"key", req.Key, "want_vary", req.VaryKey, "have_vary", obj.VaryKey, "hops", hops)
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return
	}
	// Resolver-body leak: the primary-key entry is the Vary resolver —
	// its body belongs to whichever variant filled it first (the handler
	// stores it so lookups can learn the Vary list, not to be served). A
	// requester that misses locally peer-fetches its primary key with a
	// blank assertion (it has no local object to derive the Vary list
	// from), so the previous gate never fired and the owner handed back
	// the resolver body — the first market's content — as a peer HIT.
	// RFC 9111 §4.1: a request that selects a variant must never be
	// satisfied by an entry stored under the bare primary key, so answer
	// with a miss and let the requester fill (and assert) its own
	// variant. A peer fetch for a REAL variant key never lands here:
	// variant entries carry a non-empty VaryKey.
	if obj.VaryKey == "" && obj.VaryValue != "" {
		h.logger.Info("served peer fetch miss: primary entry is a Vary resolver",
			"key", req.Key, "vary", obj.VaryValue, "hops", hops)
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return
	}

	h.logger.Info("served peer fetch hit", "key", req.Key, "hops", hops)
	ctx.Response.Header.Set(header.ContentType, "application/octet-stream")

	bufp := peerFetchEncodePool.Get().(*[]byte)
	encoded := storage.EncodeObjectInto(obj, (*bufp)[:0])
	_, _ = ctx.Write(encoded)
	if cap(encoded) <= 64*1024 {
		*bufp = encoded[:0]
		peerFetchEncodePool.Put(bufp)
	}
}

// Put forwards obj to the owner peer via the write-to-owner RPC. Used in
// strong mode so a non-owner that fetched from origin delivers the object
// to the owner for future peer-fetches (issue #509). Best-effort: errors
// are logged and returned but do not block the caller's response. The
// caller is responsible for running this off the response path.
// Bounded by putSem to prevent unbounded goroutine fan-out during miss
// storms; if the semaphore is full, the RPC is skipped (best-effort).
func (f *PeerFetcher) Put(ctx context.Context, peer api.PeerInfo, obj *api.Object) error {
	if obj == nil {
		return nil
	}

	addr := peerAddr(peer)
	if !f.breakerAllowed(addr) {
		return fmt.Errorf("peer put %s: %w", peer.Addr, ErrPeerBlacklisted)
	}

	select {
	case f.putSem <- struct{}{}:
		defer func() { <-f.putSem }()
	case <-ctx.Done():
		return fmt.Errorf("peer put %s: %w", peer.Addr, ctx.Err())
	}
	fetchAddr := addr
	scheme := "http"
	if f.useTLS {
		scheme = "https"
	}
	uri := scheme + "://" + fetchAddr + PeerPutPath

	body := storage.EncodeObject(obj)
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetRequestURI(uri)
	req.SetBodyRaw(body)
	req.Header.Set(header.ContentType, "application/octet-stream")
	req.Header.Set(ClusterVersionHeader, ClusterProtocolVersion)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	putClient := f.getPipelineClient(addr)
	if putClient == nil {
		return fmt.Errorf("peer put %s: fetcher closed during shutdown", peer.Addr)
	}
	if err := transport.PipelineDo(ctx, putClient, req, resp); err != nil {
		// A canceled caller context is not a peer failure — do not
		// count it toward the breaker.
		if ctx.Err() == nil {
			f.recordPeerFailure(addr)
		}
		return fmt.Errorf("peer put %s: %w", peer.Addr, err)
	}
	f.recordPeerSuccess(addr)
	if resp.StatusCode() != fasthttp.StatusOK {
		return fmt.Errorf("peer put %s: status %d", peer.Addr, resp.StatusCode())
	}
	f.logger.Debug("peer put ok", "key", obj.Key, "peer", peer.Addr)
	return nil
}

// PeerPutHandler receives write-to-owner RPCs and stores the forwarded
// object in the local store. Mounted on PeerPutPath. The owner node is
// the destination for non-owner origin-fetches (issue #509).
type PeerPutHandler struct {
	store   PeerPutStore
	logger  observability.Logger
	onStore func(ctx context.Context, obj *api.Object)
}

// PeerPutStore is the minimal storage interface needed by peer put.
// It is satisfied by storage.Store.
type PeerPutStore interface {
	Put(ctx context.Context, key api.Key, obj *api.Object) error
}

// NewPeerPutHandler creates a peer-put handler backed by store.
func NewPeerPutHandler(store PeerPutStore, logger observability.Logger) *PeerPutHandler {
	return &PeerPutHandler{store: store, logger: observability.ResolveLogger(logger)}
}

// SetOnStore sets a callback invoked after a successful store. Used by the
// engine to wire refresh scheduling (storeObject) for forwarded objects.
func (h *PeerPutHandler) SetOnStore(fn func(ctx context.Context, obj *api.Object)) {
	h.onStore = fn
}

// Handle is the fasthttp.RequestHandler for peer put requests.
func (h *PeerPutHandler) Handle(ctx *fasthttp.RequestCtx) {
	if string(ctx.Method()) != fasthttp.MethodPost {
		ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
		return
	}
	body := ctx.PostBody()
	if len(body) == 0 {
		ctx.Error("read error", fasthttp.StatusBadRequest)
		return
	}
	obj, err := storage.DecodeObject(body)
	if err != nil {
		h.logger.Warn("peer put decode failed", "error", err)
		ctx.Error("decode error", fasthttp.StatusBadRequest)
		return
	}
	if obj == nil || obj.Key == (api.Key{}) {
		ctx.Error("missing key", fasthttp.StatusBadRequest)
		return
	}
	if err := h.store.Put(ctx, obj.Key, obj); err != nil {
		h.logger.Warn("peer put store failed", "key", obj.Key, "error", err)
		ctx.Error("store error", fasthttp.StatusInternalServerError)
		return
	}
	if h.onStore != nil {
		h.onStore(ctx, obj)
	}
	h.logger.Debug("served peer put", "key", obj.Key)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

// Ensure unused imports are referenced for future use.
var _ = json.Marshal
