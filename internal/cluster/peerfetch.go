package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// peerFetchBufPool reuses bytes.Buffer instances across peer-fetch calls
// to avoid allocating a new buffer per fetch. Buffers that grow past
// maxPeerFetchBytes are discarded to prevent the pool from pinning
// oversized buffers.
var peerFetchBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// peerFetchEncodePool reuses []byte buffers for encoding cached objects
// in peer-fetch responses. Buffers larger than 64 KiB are discarded to
// prevent the pool from pinning oversized buffers.
var peerFetchEncodePool = sync.Pool{
	New: func() any { b := make([]byte, 0, 4096); return &b },
}

// peerFetchBinaryVersion is the version byte for the binary peer-fetch
// request format. Must not collide with JSON's '{' (0x7B).
const peerFetchBinaryVersion = 1

// maxPeerFetchBinaryBody is the maximum binary request body size.
// 1 (version) + 8 (key) + 1 (vary-key len) + 255 (vary-key) = 265.
const maxPeerFetchBinaryBody = 512

const (
	// PeerFetchPath is the HTTP path for peer cache lookups.
	PeerFetchPath = "/v1/peer/fetch"
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
	// peerFetchTimeout is the maximum time for a peer-fetch RPC.
	peerFetchTimeout = 500 * time.Millisecond
	// defaultPeerFetchConcurrency bounds concurrent peer-fetch RPCs to
	// prevent memory blow-up during miss fan-out (issue #133).
	defaultPeerFetchConcurrency = 4
)

// maxPeerFetchBytes caps the response body read from a peer during
// binary decode. A compromised peer could send an arbitrarily large
// payload; without a limit io.ReadAll buffers unbounded data on
// the heap. This is the total encoded payload limit (metadata + body).
const maxPeerFetchBytes int64 = 64 << 20

// PeerFetcher issues cache-lookup RPCs to peer nodes.
//
// Stable.
type PeerFetcher struct {
	client       *http.Client
	useTLS       bool
	hopLimit     int
	hits         atomic.Int64
	misses       atomic.Int64
	latSumMs     atomic.Int64
	latN         atomic.Int64
	hopLimitHits atomic.Int64
	maxBodyBytes int64
	fetchSem     chan struct{}
	logger       observability.Logger
	// Prometheus counters — registered if a non-nil registry is passed.
	pHits     prometheus.Counter
	pMisses   prometheus.Counter
	pHopLimit prometheus.Counter
	pDuration prometheus.Observer
}

// PeerFetchStats returns a snapshot of peer fetch telemetry.
func (f *PeerFetcher) PeerFetchStats() (hits, misses, hopLimitHits, latN, latSumMs int64) {
	return f.hits.Load(), f.misses.Load(), f.hopLimitHits.Load(), f.latN.Load(), f.latSumMs.Load()
}

// Close drains idle cluster connections. Should be called during shutdown
// so that rolling restarts don't leave TIME_WAIT sockets on peers.
func (f *PeerFetcher) Close(_ context.Context) error {
	if t, ok := f.client.Transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
}

// NewPeerFetcher creates a PeerFetcher. tlsCfg must have the cluster
// mTLS credentials. If nil a plain HTTP client is used (test-only).
// reg, if non-nil, receives Prometheus metric registration.
// hopLimit caps the number of peers a request may traverse; 0 uses MaxHops.
func NewPeerFetcher(tlsCfg *tls.Config, reg prometheus.Registerer, hopLimit int) *PeerFetcher {
	return NewPeerFetcherWithLogger(tlsCfg, reg, nil, hopLimit)
}

// newClusterTransport builds a tuned *http.Transport for cluster-internal
// RPCs (peer fetch, broadcast). The defaults leave MaxIdleConnsPerHost at
// Go's default of 2, which causes TLS handshake storms under bursty miss
// traffic. Setting it to 64 matches the origin pool and keeps idle
// connections warm for reuse. MaxConnsPerHost caps concurrent connections
// per peer to prevent FD exhaustion during purge storms in strong mode.
func newClusterTransport(tlsCfg *tls.Config) *http.Transport {
	return &http.Transport{
		ForceAttemptHTTP2:   true,
		TLSClientConfig:     tlsCfg,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 64,
		MaxConnsPerHost:     256,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
}

// NewPeerFetcherWithLogger creates a PeerFetcher with a structured logger.
func NewPeerFetcherWithLogger(tlsCfg *tls.Config, reg prometheus.Registerer, logger observability.Logger, hopLimit int) *PeerFetcher {
	transport := newClusterTransport(tlsCfg)
	if hopLimit <= 0 {
		hopLimit = MaxHops
	}
	f := &PeerFetcher{
		client: &http.Client{
			Transport: transport,
			Timeout:   peerFetchTimeout,
		},
		useTLS:       tlsCfg != nil,
		hopLimit:     hopLimit,
		maxBodyBytes: maxPeerFetchBytes,
		fetchSem:     make(chan struct{}, defaultPeerFetchConcurrency),
		logger:       observability.ResolveLogger(logger),
	}
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
			Namespace: "bouine", Name: "peer_fetch_duration_seconds",
			Help:    "Round-trip time for successful peer-fetch RPCs.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		})
		f.pDuration = dur
		reg.MustRegister(f.pHits, f.pMisses, f.pHopLimit, dur)
	}
	return f
}

// buildPeerRequest constructs the HTTP request for a peer-fetch RPC.
// Extracted from Fetch to keep complexity under gocyclo/funlen limits.
// The request body is JSON by design: 3 fields (key, vary_key, hops),
// trivially small. Only the response carries a full Object and needs
// the binary codec.
func buildPeerRequest(ctx context.Context, peer api.PeerInfo, req api.PeerFetchRequest, useTLS bool) (*http.Request, error) {
	fetchAddr := peer.AdminAddr
	if fetchAddr == "" {
		fetchAddr = peer.Addr
	}
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	url := scheme + "://" + fetchAddr + PeerFetchPath

	// Binary request: 1 byte version + 8 bytes key + 1 byte vary-key
	// length + vary-key string. ~10x faster than json.Marshal for a
	// 2-field struct and eliminates the io.ReadAll allocation on the
	// server side.
	body := make([]byte, 0, 10+len(req.VaryKey))
	body = append(body, peerFetchBinaryVersion)
	body = binary.LittleEndian.AppendUint64(body, uint64(req.Key))
	body = append(body, byte(len(req.VaryKey))) //nolint:gosec // VaryKey is a short variant key, always < 256 bytes
	body = append(body, req.VaryKey...)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("peer fetch request: %w", err)
	}
	httpReq.Header.Set(header.ContentType, "application/octet-stream")
	httpReq.Header.Set(BouineHopHeader, fmt.Sprintf("%d", req.Hops))
	httpReq.Header.Set(ClusterVersionHeader, ClusterProtocolVersion)
	return httpReq, nil
}

// Fetch asks a peer for a cached object. Returns nil, nil on a cache
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

	httpReq, err := buildPeerRequest(ctx, peer, req, f.useTLS)
	if err != nil {
		return nil, err
	}

	select {
	case f.fetchSem <- struct{}{}:
		defer func() { <-f.fetchSem }()
	case <-ctx.Done():
		return nil, fmt.Errorf("peer fetch %s: %w", peer.Addr, ctx.Err())
	}

	start := time.Now()
	resp, err := f.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("peer fetch %s: %w", peer.Addr, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		f.misses.Add(1)
		if f.pMisses != nil {
			f.pMisses.Inc()
		}
		f.logger.Info("peer fetch miss",
			"key", req.Key, "peer", peer.Addr, "hops", req.Hops)
		return nil, nil // peer miss
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer fetch %s: status %d", peer.Addr, resp.StatusCode)
	}

	// Binary object codec — no JSON, no base64, no reflection (issue #187).
	// Read into a pooled bytes.Buffer that grows incrementally instead of
	// io.ReadAll which allocates a single contiguous buffer up to
	// maxPeerFetchBytes. With 4 concurrent fetches, the pooled approach
	// reduces peak transient allocation from 256 MiB to ~64 MiB.
	buf := peerFetchBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	putBufBack := true
	defer func() {
		if putBufBack && buf.Cap() <= int(maxPeerFetchBytes) {
			peerFetchBufPool.Put(buf)
		}
	}()
	if _, err := io.Copy(buf, io.LimitReader(resp.Body, f.maxBodyBytes)); err != nil {
		return nil, fmt.Errorf("peer fetch read: %w", err)
	}
	respBody := buf.Bytes()
	obj, err := storage.DecodeObject(respBody)
	if err != nil {
		return nil, fmt.Errorf("peer fetch decode: %w", err)
	}
	// DecodeObject returns an Object whose Body aliases respBody (buf.Bytes()).
	// We must NOT return buf to the pool — the caller retains obj.Body which
	// points into buf's backing array. Returning buf would allow a concurrent
	// Fetch to overwrite obj.Body (use-after-free, race detector finds it).
	putBufBack = false

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

// PeerFetchHandler returns an http.Handler that serves peer-fetch
// requests from the local store. Mount on PeerFetchPath.
type PeerFetchHandler struct {
	store    PeerStore
	hopLimit int
	logger   observability.Logger
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

// ServeHTTP handles peer fetch requests.
func (h *PeerFetchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Hop-limit guard (T36).
	hopStr := r.Header.Get(BouineHopHeader)
	var hops int
	if hopStr != "" {
		if _, err := fmt.Sscanf(hopStr, "%d", &hops); err == nil && hops >= h.hopLimit {
			http.Error(w, "hop limit", http.StatusLoopDetected)
			return
		}
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxPeerFetchBinaryBody))
	if err != nil || len(body) == 0 {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	var req api.PeerFetchRequest
	switch body[0] {
	case peerFetchBinaryVersion:
		// Binary format: 1 byte version + 8 bytes key + 1 byte
		// vary-key length + vary-key string.
		if len(body) < 10 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		req.Key = api.Key(binary.LittleEndian.Uint64(body[1:9]))
		varyLen := int(body[9])
		if len(body) < 10+varyLen {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		req.VaryKey = string(body[10 : 10+varyLen])
	case '{':
		// Legacy JSON format for backward compatibility during
		// rolling upgrades. JSON always starts with '{' (0x7B).
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
	default:
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	obj, _, err := h.store.Get(r.Context(), req.Key)
	if err != nil || obj == nil {
		h.logger.Info("served peer fetch miss", "key", req.Key, "hops", hops)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	h.logger.Info("served peer fetch hit", "key", req.Key, "hops", hops)
	w.Header().Set(header.ContentType, "application/octet-stream")

	// Pool the encode buffer to avoid per-response allocation.
	bufp := peerFetchEncodePool.Get().(*[]byte)
	encoded := storage.EncodeObjectInto(obj, (*bufp)[:0])
	_, _ = w.Write(encoded)
	if cap(encoded) <= 64*1024 {
		*bufp = encoded[:0]
		peerFetchEncodePool.Put(bufp)
	}
}
