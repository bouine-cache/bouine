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
	"net/url"
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
	// peerFetchTimeout is the maximum time for a peer-fetch or peer-put RPC.
	// Cluster peers are on the same LAN (sub-ms RTT); 150ms is generous for
	// a full request-response cycle including TLS handshake. When a peer
	// is dead, this bounds the penalty before the caller falls back to
	// origin: 150ms instead of 500ms per request during the memberlist
	// suspicion window (~5s). ECONNREFUSED returns immediately regardless.
	peerFetchTimeout = 150 * time.Millisecond
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
// The dial timeout is 200ms: cluster peers are on the same LAN, so a
// healthy peer connects in <1ms. A dead peer's endpoint is typically
// already removed by the kubelet (ECONNREFUSED, <1ms); the 200ms cap
// covers the rare case where the IP is still routable but the process
// is gone (no RST, SYN dropped).
func newClusterTransport(tlsCfg *tls.Config) *http.Transport {
	return &http.Transport{
		ForceAttemptHTTP2:   true,
		TLSClientConfig:     tlsCfg,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 64,
		MaxConnsPerHost:     256,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   200 * time.Millisecond,
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

	// Binary request: 1 byte version + 16 bytes key + 1 byte vary-key
	// length + vary-key string. ~10x faster than json.Marshal for a
	// 2-field struct and eliminates the io.ReadAll allocation on the
	// server side.
	body := make([]byte, 0, 18+len(req.VaryKey))
	body = append(body, peerFetchBinaryVersion)
	body = append(body, req.Key[:]...)
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

// Put forwards obj to the owner peer via the write-to-owner RPC. Used in
// strong mode so a non-owner that fetched from origin delivers the object
// to the owner for future peer-fetches (issue #509). Best-effort: errors
// are logged and returned but do not block the caller's response.
// Bounded by putSem to prevent unbounded goroutine fan-out during miss
// storms; if the semaphore is full, the RPC is skipped (best-effort).
// The request metadata (URL and Vary-relevant headers) is encoded
// alongside the object so the owner can schedule refresh-before-expiry
// with the correct request context.
func (f *PeerFetcher) Put(ctx context.Context, peer api.PeerInfo, obj *api.Object, req *http.Request) error {
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
	url := scheme + "://" + fetchAddr + PeerPutPath

	body := encodePeerPutPayload(obj, req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("peer put request: %w", err)
	}
	httpReq.Header.Set(header.ContentType, "application/octet-stream")
	httpReq.Header.Set(ClusterVersionHeader, ClusterProtocolVersion)

	resp, err := f.client.Do(httpReq) //nolint:gosec // G704: URL is built from peer.Addr, a trusted cluster peer (same as Fetch)
	if err != nil {
		return fmt.Errorf("peer put %s: %w", peer.Addr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer put %s: status %d", peer.Addr, resp.StatusCode)
	}
	f.logger.Debug("peer put ok", "key", obj.Key, "peer", peer.Addr)
	return nil
}

// PutAsync launches a fire-and-forget write-to-owner RPC in a goroutine
// owned by the PeerFetcher. The goroutine is bounded by putSem (acquired
// before spawning so a miss storm does not pin N goroutines each holding
// a response body in memory). If the semaphore is full the RPC is dropped
// (best-effort, matching the write-to-owner contract). The context is
// detached from the caller so the RPC completes after the response is
// sent. Used by the engine wiring in strong mode (issue #509).
func (f *PeerFetcher) PutAsync(ctx context.Context, peer api.PeerInfo, obj *api.Object, req *http.Request) {
	if obj == nil {
		return
	}
	// Acquire the semaphore before spawning so a miss storm does not
	// create N blocked goroutines each pinning a response body. If the
	// sem is full, drop the RPC — best-effort, same contract as before.
	select {
	case f.putSem <- struct{}{}:
	default:
		return
	}
	go func() {
		defer func() { <-f.putSem }()
		putCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), peerFetchTimeout)
		defer cancel()
		if err := f.Put(putCtx, peer, obj, req); err != nil {
			f.logger.Debug("peer put error (non-fatal)",
				"owner", peer.Name, "key", obj.Key, "error", err)
		}
	}()
}

// peerPutMeta carries the original request metadata needed by the
// owner to schedule refresh-before-expiry for a forwarded object: the
// URL (for the refresh registry) and the Vary-relevant request headers
// (for content negotiation on future revalidations). Encoded alongside
// the object in the peer-put wire payload (issue #509).
type peerPutMeta struct {
	method string
	url    string
	header http.Header // Vary-relevant request headers only
}

// encodePeerPutPayload serialises the request metadata and the object
// into a single byte slice for the write-to-owner RPC. The format is:
//   - method  (uvarint len + bytes)
//   - url     (uvarint len + bytes)
//   - headers (uvarint count; per header: uvarint key len + key + uvarint val len + val)
//   - object  (raw storage.EncodeObject output)
//
// The object is last so storage.DecodeObject can alias the body slice
// directly from the tail of the payload without a copy.
func encodePeerPutPayload(obj *api.Object, req *http.Request) []byte {
	objBytes := storage.EncodeObject(obj)
	// Pre-allocate: method + url + header count + ~4 headers (16 bytes each) + object.
	buf := make([]byte, 0, len(objBytes)+256)

	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	buf = binary.AppendUvarint(buf, uint64(len(method)))
	buf = append(buf, method...)

	urlStr := ""
	if req.URL != nil {
		urlStr = req.URL.String()
	}
	buf = binary.AppendUvarint(buf, uint64(len(urlStr)))
	buf = append(buf, urlStr...)

	if req.Header != nil {
		buf = binary.AppendUvarint(buf, uint64(len(req.Header)))
		for k, vs := range req.Header {
			for _, v := range vs {
				buf = binary.AppendUvarint(buf, uint64(len(k)))
				buf = append(buf, k...)
				buf = binary.AppendUvarint(buf, uint64(len(v)))
				buf = append(buf, v...)
			}
		}
	} else {
		buf = binary.AppendUvarint(buf, 0)
	}

	buf = append(buf, objBytes...)
	return buf
}

// peerPutReader is a cursor over the peer-put wire payload.
type peerPutReader struct {
	data []byte
	pos  int
}

func (r *peerPutReader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.data[r.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("peer put: corrupt uvarint at offset %d", r.pos)
	}
	r.pos += n
	return v, nil
}

// bytes returns the next n bytes as a sub-slice (no copy). n must be
// bounds-checked by the caller; readLen enforces the cap.
func (r *peerPutReader) bytes(n int) ([]byte, error) {
	if n < 0 || n > len(r.data)-r.pos {
		return nil, fmt.Errorf("peer put: truncated at offset %d (need %d, have %d)", r.pos, n, len(r.data)-r.pos)
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

// readLen reads a uvarint length and the following bytes, enforcing a
// maximum to prevent integer overflow on 32-bit platforms and reject
// absurd payloads. The cap must be <= len(remaining data).
func (r *peerPutReader) readLen(max int) ([]byte, error) {
	v, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if v > uint64(max) { //nolint:gosec // G115: max is int (from len), always fits uint64 on real platforms
		return nil, fmt.Errorf("peer put: length %d exceeds cap %d", v, max)
	}
	return r.bytes(int(v)) //nolint:gosec // bounded by max check above
}

// decodePeerPutPayload deserialises a peer-put payload into the object
// and the original request metadata. The returned object's Body aliases
// the input slice (no copy); callers must treat body as immutable.
func decodePeerPutPayload(data []byte) (*api.Object, *peerPutMeta, error) {
	r := &peerPutReader{data: data}
	rem := len(data)

	methodBytes, err := r.readLen(rem)
	if err != nil {
		return nil, nil, err
	}
	method := string(methodBytes)

	urlBytes, err := r.readLen(rem)
	if err != nil {
		return nil, nil, err
	}
	urlStr := string(urlBytes)

	hdr, err := decodePeerPutHeaders(r, rem)
	if err != nil {
		return nil, nil, err
	}

	obj, err := storage.DecodeObject(r.data[r.pos:])
	if err != nil {
		return nil, nil, err
	}
	meta := &peerPutMeta{method: method, url: urlStr, header: hdr}
	return obj, meta, nil
}

// decodePeerPutHeaders reads the header count and per-header key/value
// pairs from the reader. rem is the remaining byte budget (used as the
// per-field length cap).
func decodePeerPutHeaders(r *peerPutReader, rem int) (http.Header, error) {
	hdrCount, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if hdrCount == 0 {
		return nil, nil
	}
	// Each header needs at least 2 uvarint lengths (2 bytes) + key + val.
	remBytes := len(r.data) - r.pos
	if int(hdrCount) > remBytes { //nolint:gosec // G115: hdrCount bounded by uvarint, remBytes is small
		return nil, fmt.Errorf("peer put: header count %d exceeds remaining bytes", hdrCount)
	}
	hdr := make(http.Header, min(int(hdrCount), 16)) //nolint:gosec // bounded by remaining-length check
	for range hdrCount {
		keyBytes, err := r.readLen(rem)
		if err != nil {
			return nil, err
		}
		valBytes, err := r.readLen(rem)
		if err != nil {
			return nil, err
		}
		hdr.Add(string(keyBytes), string(valBytes))
	}
	return hdr, nil
}

// PeerPutHandler receives write-to-owner RPCs and stores the forwarded
// object in the local store. Mounted on PeerPutPath. The owner node is
// the destination for non-owner origin-fetches (issue #509).
type PeerPutHandler struct {
	store  PeerPutStore
	logger observability.Logger
	// onStore, if non-nil, is called after the object is stored. The
	// cache handler wires this to its StoreFromPeer so refresh-before-
	// expiry is scheduled for forwarded objects (which bypass the normal
	// miss path on the owner). The forwarded request metadata (URL and
	// Vary-relevant headers) is passed through so the refresh registry
	// has the correct request context.
	onStore func(ctx context.Context, obj *api.Object, req *http.Request)
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
// engine to wire refresh scheduling (StoreFromPeer) for forwarded objects.
func (h *PeerPutHandler) SetOnStore(fn func(ctx context.Context, obj *api.Object, req *http.Request)) {
	h.onStore = fn
}

// ServeHTTP handles peer put requests: decode the forwarded object and
// request metadata, store the object, and fire the onStore callback so
// the cache handler schedules refresh-before-expiry. The owner is the
// authoritative store for this key.
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
	obj, meta, err := decodePeerPutPayload(body)
	if err != nil {
		h.logger.Warn("peer put decode failed", "error", err)
		http.Error(w, "decode error", http.StatusBadRequest)
		return
	}
	if obj == nil || obj.Key == (api.Key{}) {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	// Build the synthetic request from the forwarded metadata. The
	// onStore callback (StoreFromPeer → storeObject) handles both the
	// store.Put and refresh scheduling, so we do NOT Put here — that
	// would be a redundant write (issue #509 review finding).
	reqURL, err := url.Parse(meta.url)
	if err != nil || reqURL == nil {
		reqURL = &url.URL{}
	}
	method := meta.method
	if method == "" {
		method = http.MethodGet
	}
	synReq := &http.Request{
		Method: method,
		URL:    reqURL,
		Host:   reqURL.Host,
		Header: meta.header,
	}
	if h.onStore != nil {
		h.onStore(r.Context(), obj, synReq)
	} else {
		// No refresh wiring: store directly.
		if err := h.store.Put(r.Context(), obj.Key, obj); err != nil {
			h.logger.Warn("peer put store failed", "key", obj.Key, "error", err)
			http.Error(w, "store error", http.StatusInternalServerError)
			return
		}
	}
	h.logger.Debug("served peer put", "key", obj.Key)
	w.WriteHeader(http.StatusOK)
}
