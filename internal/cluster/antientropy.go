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
	// BackfillCooldown suppresses re-backfill of a key for this window
	// after it was last backfilled. SIEVE evicts freshly-backfilled keys
	// (low priority: just inserted, never served) before the next round,
	// so without a cooldown the same keys are "missing" again every round
	// and the reconciler re-fetches them — a self-sustaining storm (#187).
	// The cooldown skips a key as "missing" for this duration after a
	// successful backfill, giving SIEVE time to either promote the key
	// (once it is served) or for the operator to grow the hot tier.
	// 0 disables the cooldown (back-compat). The recommended value is 5m
	// (≤ 10 rounds at the default 30s interval).
	BackfillCooldown time.Duration
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

// Storer writes objects to the local store and reports memory pressure.
// Implemented by *storage.TieredStore and *storage.HotStore. OverBudget is
// consulted before backfill so anti-entropy does not fight the eviction
// policy (#175): backfilling into an over-budget hot tier causes SIEVE to
// evict the new keys, which then look "missing" again next round, creating
// a self-sustaining loop.
type Storer interface {
	Put(ctx context.Context, key api.Key, obj *api.Object) error
	OverBudget() bool
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

	// now returns the current time. Defaults to time.Now; overridden in
	// tests to make the cooldown deterministic (AGENTS.md §8: no
	// time.Now() in tests). Only accessed from the reconcile goroutine.
	now func() time.Time
	// cooldown maps a backfilled key to the time until which it must be
	// skipped as "missing" by future reconcile rounds (#187). Pruned at
	// the top of each round to bound memory. Only accessed from the
	// single reconcile goroutine started by Start, so no mutex is
	// needed; tests call reconcile directly without running Start
	// concurrently.
	cooldown map[api.Key]time.Time
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
		cfg:      cfg,
		node:     node,
		keys:     keys,
		fetch:    fetch,
		store:    store,
		peers:    peers,
		metrics:  metrics,
		logger:   logger,
		now:      time.Now,
		cooldown: make(map[api.Key]time.Time),
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
	// Skip the entire round when the local store is already over its memory
	// budget. Anti-entropy should heal drift, not fight the eviction policy:
	// backfilling into an over-budget hot tier causes SIEVE to evict the new
	// keys, which then look "missing" again next round (#175). Checking here
	// avoids N wasted peer key-set fetches when the store is over budget for
	// a sustained period (the common case under memory pressure). The
	// per-peer check in reconcileWithPeer still guards the case where peer 1's
	// backfill pushes the store over budget mid-round.
	if ae.store.OverBudget() {
		ae.logger.Warn("anti-entropy: skipping round, local store over memory budget")
		ae.metrics.SetAntiEntropyKeysRepaired(0)
		return
	}

	// Prune expired cooldown entries at the top of each round to bound
	// memory. The cooldown map is only mutated from this goroutine, so no
	// lock is needed.
	ae.pruneCooldown()

	localKeys := ae.keys.Keys()
	localSet := make(map[api.Key]struct{}, len(localKeys))
	for _, k := range localKeys {
		localSet[k] = struct{}{}
	}

	for _, peer := range ae.peers() {
		if peer.Name == ae.node {
			continue
		}
		// reconcileWithPeer mutates localSet, adding keys it backfills
		// from this peer so subsequent peers in the same round see them
		// as present. Without this, a key backfilled from peer 1 is
		// still "missing" when diffing against peer 2, so it gets
		// backfilled once per peer per round (#175).
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

	missing, cooldownSkips := ae.missingKeys(peerKeys, localSet)

	if cooldownSkips > 0 {
		ae.metrics.AddAntiEntropyCooldownSkips(float64(cooldownSkips))
		ae.logger.Debug("anti-entropy: cooldown skipped missing keys",
			"peer", peer.Name,
			"skipped", cooldownSkips,
			"missing_after_cooldown", len(missing),
		)
	}

	if len(missing) == 0 {
		ae.metrics.SetAntiEntropyKeysRepaired(0)
		return
	}

	if ae.cfg.BackfillLimit > 0 && len(missing) > ae.cfg.BackfillLimit {
		missing = missing[:ae.cfg.BackfillLimit]
	}

	// Mid-round guard: a prior peer's backfill in this round may have pushed
	// the store over budget. Stop backfilling from subsequent peers rather
	// than feeding SIEVE more keys to evict (#175). The top-of-reconcile
	// guard handles the sustained-pressure case; this handles the transition.
	if ae.store.OverBudget() {
		ae.logger.Warn("anti-entropy: skipping backfill, local store over memory budget",
			"peer", peer.Name, "missing", len(missing))
		ae.metrics.SetAntiEntropyKeysRepaired(0)
		return
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
			// Record the backfilled key in localSet so subsequent peers
			// in this round see it as present.
			localSet[key] = struct{}{}
			// Record the key in the cooldown map so it is skipped as
			// "missing" for BackfillCooldown. This is a no-op when the
			// cooldown is disabled (0), preserving back-compat.
			ae.recordBackfill(key)
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

// pruneCooldown removes expired entries from the cooldown map. Called once
// per reconcile round to bound memory. No-op when the cooldown is disabled
// or the map is empty. Only called from the reconcile goroutine.
func (ae *AntiEntropy) pruneCooldown() {
	if ae.cfg.BackfillCooldown <= 0 || len(ae.cooldown) == 0 {
		return
	}
	now := ae.now()
	for k, exp := range ae.cooldown {
		if !now.Before(exp) {
			delete(ae.cooldown, k)
		}
	}
}

// inCooldown reports whether key is within its backfill cooldown window and
// must be skipped as "missing" this round. No-op when BackfillCooldown is 0
// (disabled, back-compat). O(1) map lookup.
func (ae *AntiEntropy) inCooldown(key api.Key) bool {
	if ae.cfg.BackfillCooldown <= 0 {
		return false
	}
	expiry, ok := ae.cooldown[key]
	if !ok {
		return false
	}
	return ae.now().Before(expiry)
}

// recordBackfill marks key as recently backfilled so it is skipped for
// BackfillCooldown. No-op when the cooldown is disabled.
func (ae *AntiEntropy) recordBackfill(key api.Key) {
	if ae.cfg.BackfillCooldown <= 0 {
		return
	}
	ae.cooldown[key] = ae.now().Add(ae.cfg.BackfillCooldown)
}

// missingKeys computes the keys present in peerKeys but absent from
// localSet, skipping any that are within their backfill cooldown window
// (#187). The cooldown check is applied here — before the missing list is
// built — so the peer-fetch RPC is never issued for cooled-down keys, not
// merely skipped at store time. Returns the missing list and the number of
// keys skipped by cooldown. The cooldown check is O(1) per key (one map
// lookup).
func (ae *AntiEntropy) missingKeys(peerKeys []api.Key, localSet map[api.Key]struct{}) (missing []api.Key, cooldownSkips int) {
	for _, key := range peerKeys {
		if _, ok := localSet[key]; ok {
			continue
		}
		if ae.inCooldown(key) {
			cooldownSkips++
			continue
		}
		missing = append(missing, key)
	}
	return missing, cooldownSkips
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
	if err := ae.store.Put(bfCtx, key, obj); err != nil {
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
