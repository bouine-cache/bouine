package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

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

// NewPeerFetcher creates a PeerFetcher. tlsCfg must have the cluster
// mTLS credentials. If nil a plain HTTP client is used (test-only).
// reg, if non-nil, receives Prometheus metric registration.
func NewPeerFetcher(tlsCfg *tls.Config, reg prometheus.Registerer) *PeerFetcher {
	return NewPeerFetcherWithLogger(tlsCfg, reg, nil)
}

// newClusterTransport builds a tuned *http.Transport for cluster-internal
// RPCs (peer fetch, broadcast). The defaults leave MaxIdleConnsPerHost at
// Go's default of 2, which causes TLS handshake storms under bursty miss
// traffic. Setting it to 64 matches the origin pool and keeps idle
// connections warm for reuse.
func newClusterTransport(tlsCfg *tls.Config) *http.Transport {
	return &http.Transport{
		ForceAttemptHTTP2:   true,
		TLSClientConfig:     tlsCfg,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
}

// NewPeerFetcherWithLogger creates a PeerFetcher with a structured logger.
func NewPeerFetcherWithLogger(tlsCfg *tls.Config, reg prometheus.Registerer, logger observability.Logger) *PeerFetcher {
	transport := newClusterTransport(tlsCfg)
	f := &PeerFetcher{
		client: &http.Client{
			Transport: transport,
			Timeout:   peerFetchTimeout,
		},
		useTLS:       tlsCfg != nil,
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

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("peer fetch marshal: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("peer fetch request: %w", err)
	}
	httpReq.Header.Set(header.ContentType, "application/json")
	httpReq.Header.Set(BouineHopHeader, fmt.Sprintf("%d", req.Hops))
	httpReq.Header.Set(ClusterVersionHeader, ClusterProtocolVersion)
	return httpReq, nil
}

// Fetch asks a peer for a cached object. Returns nil, nil on a cache
// miss at the peer; returns an error only on network/protocol failure.
func (f *PeerFetcher) Fetch(ctx context.Context, peer api.PeerInfo, req api.PeerFetchRequest) (*api.Object, error) {
	if req.Hops >= MaxHops {
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
	// The full response is buffered into memory before decode. With
	// defaultPeerFetchConcurrency = 4 and maxPeerFetchBytes = 64 MiB, a
	// worst-case fan-out holds up to 256 MiB of transient allocation. In
	// practice peer-fetch responses are small (cached objects << 64 KiB);
	// the body-last varint framing in the codec would allow a future
	// streaming decode, but that refactor is out of scope for #187.
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("peer fetch read: %w", err)
	}
	obj, err := storage.DecodeObject(respBody)
	if err != nil {
		return nil, fmt.Errorf("peer fetch decode: %w", err)
	}

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
	store  PeerStore
	logger observability.Logger
}

// PeerStore is the minimal storage interface needed by peer fetch.
// It is satisfied by storage.Store.
type PeerStore interface {
	Get(ctx context.Context, key api.Key) (*api.Object, api.Source, error)
}

// NewPeerFetchHandler creates a peer-fetch handler backed by store.
func NewPeerFetchHandler(store PeerStore) *PeerFetchHandler {
	return NewPeerFetchHandlerWithLogger(store, nil)
}

// NewPeerFetchHandlerWithLogger creates a peer-fetch handler with a
// structured logger.
func NewPeerFetchHandlerWithLogger(store PeerStore, logger observability.Logger) *PeerFetchHandler {
	return &PeerFetchHandler{store: store, logger: observability.ResolveLogger(logger)}
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
		if _, err := fmt.Sscanf(hopStr, "%d", &hops); err == nil && hops >= MaxHops {
			http.Error(w, "hop limit", http.StatusLoopDetected)
			return
		}
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	var req api.PeerFetchRequest
	if err := json.Unmarshal(body, &req); err != nil {
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
	_, _ = w.Write(storage.EncodeObject(obj))
}
