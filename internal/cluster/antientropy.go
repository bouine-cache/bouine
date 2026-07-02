package cluster

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
)

// AntiEntropyConfig configures the anti-entropy reconciler.
type AntiEntropyConfig struct {
	// Interval is the period between reconciliation rounds. Default 30s.
	Interval time.Duration
	// BackfillLimit caps the number of keys backfilled per peer per round.
	// 0 means no limit.
	BackfillLimit int
	// FetchTimeout bounds each peer-fetch during backfill. Default 2s.
	FetchTimeout time.Duration
	// Logger is the structured logger.
	Logger observability.Logger
}

// KeySource provides the local key set for anti-entropy diffing.
type KeySource interface {
	Keys() []api.Key
}

// Backfiller retrieves missing objects from peers.
type Backfiller interface {
	Fetch(ctx context.Context, peer api.PeerInfo, req api.PeerFetchRequest) (*api.Object, error)
}

// Storer writes objects to the local store. Implemented by storage.Store.
type Storer interface {
	Put(ctx context.Context, key api.Key, obj *api.Object) error
}

// AntiEntropy runs periodic object-set reconciliation between peers in full
// cluster mode. It computes the diff between the local key set and each
// peer's key set, then backfills missing objects via the existing peer-fetch
// HTTP path and stores them locally. Convergence is the goal, not instant
// sync: backfill is bounded per round and rate-limited.
type AntiEntropy struct {
	cfg     AntiEntropyConfig
	node    string
	keys    KeySource
	fetch   Backfiller
	store   Storer
	peers   func() []api.PeerInfo
	metrics *Metrics
	logger  observability.Logger
}

// NewAntiEntropy creates a reconciler. peers returns the current peer list
// (excluding self). fetch is the PeerFetcher used for backfill. store is
// the local store where backfilled objects are written. keys provides
// the local key set.
func NewAntiEntropy(cfg AntiEntropyConfig, node string, keys KeySource, fetch Backfiller, store Storer, peers func() []api.PeerInfo, metrics *Metrics) *AntiEntropy {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.FetchTimeout <= 0 {
		cfg.FetchTimeout = 2 * time.Second
	}
	logger := cfg.Logger
	return &AntiEntropy{
		cfg:     cfg,
		node:    node,
		keys:    keys,
		fetch:   fetch,
		store:   store,
		peers:   peers,
		metrics: metrics,
		logger:  logger,
	}
}

// Start launches the reconciler goroutine. It exits when ctx is cancelled.
func (ae *AntiEntropy) Start(ctx context.Context) {
	go ae.loop(ctx)
}

func (ae *AntiEntropy) loop(ctx context.Context) {
	ticker := time.NewTicker(ae.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ae.reconcile(ctx)
		}
	}
}

func (ae *AntiEntropy) reconcile(ctx context.Context) {
	localKeys := ae.keys.Keys()
	localSet := make(map[api.Key]struct{}, len(localKeys))
	for _, k := range localKeys {
		localSet[k] = struct{}{}
	}

	for _, peer := range ae.peers() {
		if peer.Name == ae.node {
			continue
		}
		ae.reconcileWithPeer(ctx, peer, localSet)
	}
}

func (ae *AntiEntropy) reconcileWithPeer(ctx context.Context, peer api.PeerInfo, localSet map[api.Key]struct{}) {
	peerKeys, ok := ae.fetchPeerKeys(ctx, peer)
	if !ok {
		ae.metrics.IncAntiEntropyFetchFailure()
		return
	}

	ae.metrics.IncAntiEntropyReconcile()

	var missing []api.Key
	for _, key := range peerKeys {
		if _, ok := localSet[key]; !ok {
			missing = append(missing, key)
		}
	}

	if len(missing) == 0 {
		ae.metrics.SetAntiEntropyKeysRepaired(0)
		return
	}

	if ae.cfg.BackfillLimit > 0 && len(missing) > ae.cfg.BackfillLimit {
		missing = missing[:ae.cfg.BackfillLimit]
	}

	ae.logger.Debug("anti-entropy: backfilling", "peer", peer.Name, "missing", len(missing))

	start := time.Now()
	repaired := 0
	for _, key := range missing {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if ae.backfillKey(ctx, peer, key) {
			repaired++
		}
	}

	ae.metrics.SetAntiEntropyKeysRepaired(float64(repaired))
	ae.logger.Debug("reconciled with peer",
		"peer", peer.Name,
		"peer_address", peer.Addr,
		"missing_count", len(missing),
		"backfilled_count", repaired,
		"dur_ms", time.Since(start).Milliseconds(),
	)
}

func (ae *AntiEntropy) fetchPeerKeys(ctx context.Context, peer api.PeerInfo) ([]api.Key, bool) {
	fetchAddr := peer.AdminAddr
	if fetchAddr == "" {
		fetchAddr = peer.Addr
	}
	url := "http://" + fetchAddr + PeerKeysPath

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		ae.logger.Debug("anti-entropy: build request", "peer", peer.Name, "error", err)
		return nil, false
	}
	req.Header.Set(ClusterVersionHeader, ClusterProtocolVersion)

	client := &http.Client{Timeout: ae.cfg.FetchTimeout}
	resp, err := client.Do(req)
	if err != nil {
		ae.logger.Debug("anti-entropy: fetch key set", "peer", peer.Name, "error", err)
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		ae.logger.Debug("anti-entropy: peer key set status", "peer", peer.Name, "status", resp.StatusCode)
		return nil, false
	}

	buf, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024*1024))
	if err != nil {
		ae.logger.Debug("anti-entropy: read key set", "peer", peer.Name, "error", err)
		return nil, false
	}
	_, ksKeys, err := DecodeKeySet(buf)
	if err != nil {
		ae.logger.Debug("anti-entropy: decode key set", "peer", peer.Name, "error", err)
		return nil, false
	}
	return ksKeys, true
}

func (ae *AntiEntropy) backfillKey(ctx context.Context, peer api.PeerInfo, key api.Key) bool {
	bfCtx, cancel := context.WithTimeout(ctx, ae.cfg.FetchTimeout)
	defer cancel()
	obj, err := ae.fetch.Fetch(bfCtx, peer, api.PeerFetchRequest{Key: key})
	if err != nil || obj == nil {
		return false
	}
	if err := ae.store.Put(ctx, key, obj); err != nil {
		ae.logger.Debug("anti-entropy: store backfilled", "peer", peer.Name, "key", key, "error", err)
		return false
	}
	ae.metrics.IncAntiEntropyRepaired()
	ae.logger.Info("backfilled key from peer",
		"key", key,
		"peer", peer.Name,
		"peer_address", peer.Addr,
	)
	return true
}

// PeerKeysHandler serves the local key set for anti-entropy reconciliation.
// Mounted at GET /v1/peer/keys. Returns a binary KeySet.
type PeerKeysHandler struct {
	keys KeySource
	node string
}

// NewPeerKeysHandler creates a handler for the anti-entropy key-set endpoint.
func NewPeerKeysHandler(keys KeySource, node string) *PeerKeysHandler {
	return &PeerKeysHandler{keys: keys, node: node}
}

// PeerKeysPath is the HTTP path for the anti-entropy key-set endpoint.
const PeerKeysPath = "/v1/peer/keys"

func (h *PeerKeysHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	buf, err := EncodeKeySet(h.node, h.keys.Keys())
	if err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
		return
	}
	w.Header().Set(header.ContentType, "application/octet-stream")
	_, _ = w.Write(buf)
}

// keysToUint64 is a helper for tests.
func keysToUint64(keys []api.Key) []uint64 {
	out := make([]uint64, len(keys))
	for i, k := range keys {
		out[i] = uint64(k)
	}
	return out
}
