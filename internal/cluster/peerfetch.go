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
// request format. v2 uses 16-byte (128-bit) keys. Must not collide with
// JSON's '{' (0x7B).
const peerFetchBinaryVersion = 2

// maxPeerFetchBinaryBody is the maximum binary request body size.
// 1 (version) + 16 (key) + 1 (vary-key len) + 255 (vary-key) = 273.
const maxPeerFetchBinaryBody = 512

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
	client       *transport.Client
	useTLS       bool
	hopLimit     int
	hits         atomic.Int64
	misses       atomic.Int64
	latSumMs     atomic.Int64
	latN         atomic.Int64
	hopLimitHits atomic.Int64
	maxBodyBytes int64
	fetchSem     chan struct{}
	// putSem bounds concurrent write-to-owner RPCs to prevent memory
	// blow-up during miss fan-out (same rationale as fetchSem, issue #509).
	putSem chan struct{}
	logger observability.Logger
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
	f.client.CloseIdleConnections()
	return nil
}

// NewPeerFetcher creates a PeerFetcher. tlsCfg must have the cluster
// mTLS credentials. If nil a plain HTTP client is used (test-only).
// reg, if non-nil, receives Prometheus metric registration.
// hopLimit caps the number of peers a request may traverse; 0 uses MaxHops.
func NewPeerFetcher(tlsCfg *tls.Config, reg prometheus.Registerer, hopLimit int) *PeerFetcher {
	return NewPeerFetcherWithLogger(tlsCfg, reg, nil, hopLimit)
}

// NewPeerFetcherWithLogger creates a PeerFetcher with a structured logger.
func NewPeerFetcherWithLogger(tlsCfg *tls.Config, reg prometheus.Registerer, logger observability.Logger, hopLimit int) *PeerFetcher {
	if hopLimit <= 0 {
		hopLimit = MaxHops
	}
	fc := &fasthttp.Client{
		MaxConnsPerHost:     256,
		MaxIdleConnDuration: 90 * time.Second,
		ReadTimeout:         PeerFetchTimeout,
		WriteTimeout:        5 * time.Minute,
		TLSConfig:           tlsCfg,
		Dial: func(addr string) (net.Conn, error) {
			return (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial("tcp", addr)
		},
	}
	f := &PeerFetcher{
		client:       transport.NewClient(fc),
		useTLS:       tlsCfg != nil,
		hopLimit:     hopLimit,
		maxBodyBytes: maxPeerFetchBytes,
		fetchSem:     make(chan struct{}, defaultPeerFetchConcurrency),
		putSem:       make(chan struct{}, defaultPeerFetchConcurrency),
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

	httpReq := buildPeerRequest(peer, req, f.useTLS)
	defer fasthttp.ReleaseRequest(httpReq)

	select {
	case f.fetchSem <- struct{}{}:
		defer func() { <-f.fetchSem }()
	case <-ctx.Done():
		return nil, fmt.Errorf("peer fetch %s: %w", peer.Addr, ctx.Err())
	}

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	start := time.Now()
	if err := f.client.Do(ctx, httpReq, resp); err != nil {
		return nil, fmt.Errorf("peer fetch %s: %w", peer.Addr, err)
	}

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

// PeerFetchHandler returns an http.Handler that serves peer-fetch
// requests from the local store. Mount on PeerFetchPath.
//
// Server-side handlers remain on http.Handler because the admin server
// (internal/admin) still uses net/http.ServeMux. They will be migrated
// to fasthttp.RequestHandler when the admin server is migrated (Phase 7).
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
		// Binary format: 1 byte version + 16 bytes key + 1 byte
		// vary-key length + vary-key string.
		if len(body) < 18 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		copy(req.Key[:], body[1:17])
		varyLen := int(body[17])
		if len(body) < 18+varyLen {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		req.VaryKey = string(body[18 : 18+varyLen])
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
	select {
	case f.putSem <- struct{}{}:
		defer func() { <-f.putSem }()
	case <-ctx.Done():
		return fmt.Errorf("peer put %s: %w", peer.Addr, ctx.Err())
	}
	fetchAddr := peer.AdminAddr
	if fetchAddr == "" {
		fetchAddr = peer.Addr
	}
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

	if err := f.client.Do(ctx, req, resp); err != nil {
		return fmt.Errorf("peer put %s: %w", peer.Addr, err)
	}
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
	store  PeerPutStore
	logger observability.Logger
	// onStore, if non-nil, is called after the object is stored. The cache
	// handler wires this to its storeObject so refresh-before-expiry is
	// scheduled for forwarded objects (which bypass the normal miss path
	// on the owner). The request is a synthetic GET built from the
	// forwarded object's headers; it carries enough context (Vary,
	// conditional headers) for the refresh registry.
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

// ServeHTTP handles peer put requests: decode the forwarded object and
// store it locally. The owner is the authoritative store for this key.
func (h *PeerPutHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxPeerFetchBytes))
	if err != nil || len(body) == 0 {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	obj, err := storage.DecodeObject(body)
	if err != nil {
		h.logger.Warn("peer put decode failed", "error", err)
		http.Error(w, "decode error", http.StatusBadRequest)
		return
	}
	if obj == nil || obj.Key == (api.Key{}) {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if err := h.store.Put(r.Context(), obj.Key, obj); err != nil {
		h.logger.Warn("peer put store failed", "key", obj.Key, "error", err)
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	// Fire the onStore callback so the cache handler can schedule
	// refresh-before-expiry for forwarded objects (issue #509).
	if h.onStore != nil {
		h.onStore(r.Context(), obj)
	}
	h.logger.Debug("served peer put", "key", obj.Key)
	w.WriteHeader(http.StatusOK)
}
