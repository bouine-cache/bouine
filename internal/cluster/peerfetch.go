package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thylong/bouine/pkg/api"
)

const (
	// PeerFetchPath is the HTTP path for peer cache lookups.
	PeerFetchPath = "/v1/peer/fetch"
	// MaxHops is the default maximum number of peers a single request
	// may traverse before going to origin (threat T36).
	MaxHops = 2
	// BouineHopHeader carries the current hop count for loop detection.
	BouineHopHeader = "Bouine-Hop"
	// ClusterVersionHeader carries the cluster protocol version for
	// negotiation during rolling upgrades.
	ClusterVersionHeader = "X-Bouine-Cluster-Version"
	// ClusterProtocolVersion is the current protocol version.
	ClusterProtocolVersion = "1"
	// peerFetchTimeout is the maximum time for a peer-fetch RPC.
	peerFetchTimeout = 500 * time.Millisecond
)

// PeerFetcher issues cache-lookup RPCs to peer nodes.
//
// Stable.
type PeerFetcher struct {
	client       *http.Client
	hits         atomic.Int64
	misses       atomic.Int64
	latSumMs     atomic.Int64
	latN         atomic.Int64
	hopLimitHits atomic.Int64
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
	transport := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig:   tlsCfg,
	}
	f := &PeerFetcher{
		client: &http.Client{
			Transport: transport,
			Timeout:   peerFetchTimeout,
		},
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

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("peer fetch marshal: %w", err)
	}

	// Peer-fetch RPCs are served on the peer's admin port (where
	// /v1/peer/fetch is registered). The cluster gossip port (peer.Addr)
	// is used for memberlist only; HTTP peer fetch uses peer.AdminAddr.
	fetchAddr := peer.AdminAddr
	if fetchAddr == "" {
		fetchAddr = peer.Addr // fallback for nodes without AdminAddr
	}
	url := "http://" + fetchAddr + PeerFetchPath
	if f.client.Transport.(*http.Transport).TLSClientConfig != nil {
		url = "https://" + fetchAddr + PeerFetchPath
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("peer fetch request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(BouineHopHeader, fmt.Sprintf("%d", req.Hops))
	httpReq.Header.Set(ClusterVersionHeader, ClusterProtocolVersion)

	resp, err := f.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("peer fetch %s: %w", peer.Addr, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // peer miss
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer fetch %s: status %d", peer.Addr, resp.StatusCode)
	}

	var fetchResp api.PeerFetchResponse
	if err := json.NewDecoder(resp.Body).Decode(&fetchResp); err != nil {
		return nil, fmt.Errorf("peer fetch decode: %w", err)
	}
	start := time.Now()
	_ = start // latency measured from RPC call
	if !fetchResp.Hit {
		f.misses.Add(1)
		if f.pMisses != nil {
			f.pMisses.Inc()
		}
		return nil, nil
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
	return fetchResp.Object, nil
}

// PeerFetchHandler returns an http.Handler that serves peer-fetch
// requests from the local store. Mount on PeerFetchPath.
type PeerFetchHandler struct {
	store PeerStore
}

// PeerStore is the minimal storage interface needed by peer fetch.
// It is satisfied by storage.Store.
type PeerStore interface {
	Get(ctx context.Context, key api.Key) (*api.Object, error)
}

// NewPeerFetchHandler creates a peer-fetch handler backed by store.
func NewPeerFetchHandler(store PeerStore) *PeerFetchHandler {
	return &PeerFetchHandler{store: store}
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

	obj, err := h.store.Get(r.Context(), req.Key)
	if err != nil || obj == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(api.PeerFetchResponse{Hit: true, Object: obj})
}
