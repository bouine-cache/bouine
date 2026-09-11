package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/xxhash/v3"
	"github.com/hashicorp/memberlist"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
)

// defaultHandoffQueueDepth is the memberlist per-peer message buffer
// depth. memberlist@v0.6.0's upstream default is 1024 (config.go:335);
// bouine uses 4096 to absorb production bursts of cache invalidations
// (issue #201).
const defaultHandoffQueueDepth = 4096

// MaxHandoffQueueDepth is the upper bound for HandoffQueueDepth. Each
// slot costs a pointer + message header in memberlist's per-peer linked
// list; 1<<20 × 50 peers ≈ 50 M entries worst case. config.maxHandoffQueueDepth
// mirrors this value for YAML validation.
const MaxHandoffQueueDepth = 1 << 20 // 1,048,576

// DefaultPushPullInterval is the memberlist push/pull sync interval used
// when the config leaves it unset. It replaces memberlist's 30s default
// so invalidations propagate promptly. Also surfaced on the dashboard.
const DefaultPushPullInterval = 5 * time.Second

// DefaultReconcileInterval is how often the background reconcile pass
// self-heals the ring from memberlist's live member view. It backs up
// the NotifyJoin/NotifyLeave event delegates and the push/pull prune,
// which can both miss a peer that restarted without a delivered event.
const DefaultReconcileInterval = 30 * time.Second

// Config controls the cluster membership layer.
//
// Stable.
type Config struct {
	// PeerInfo is the metadata this node broadcasts to peers.
	PeerInfo api.PeerInfo
	// Logger receives gossip and lifecycle records. Defaults to
	// a SampledLogger wrapping slog.Default().
	Logger observability.Logger
	// NodeName is the unique identifier for this node (pod name).
	// Defaults to the hostname if empty.
	NodeName string
	// BindAddr is the gossip listener address (host:port).
	BindAddr string
	// AdvertiseAddr is the address announced to peers (optional;
	// useful behind NAT or in K8s where the pod IP differs).
	AdvertiseAddr string
	// Mode determines how cache keys are distributed across the cluster.
	// "strong" uses a consistent hash ring with peer fetch on miss.
	// "eventual" caches locally with no peer fetch; invalidation by gossip.
	// Defaults to "strong" for backward compatibility.
	Mode string
	// Join is the list of seed addresses for bootstrapping.
	Join []string
	// VirtualNodes is the number of virtual nodes per real node on
	// the consistent hash ring. Default 256.
	VirtualNodes int
	// PushPullInterval is the interval between memberlist push/pull sync
	// rounds. Lower values accelerate gossip convergence at the cost of
	// higher network traffic. Default 5s; production deployments may use
	// higher values. Set to 0 to use memberlist's default (30s).
	PushPullInterval time.Duration
	// GossipApplyTimeout bounds how long a received gossip event may
	// spend applying to the local store before being abandoned. The
	// bound protects memberlist's dispatch goroutine from stalling on a
	// slow store and causing false failure detection. Default 100ms.
	GossipApplyTimeout time.Duration
	// HandoffQueueDepth sets memberlist's per-peer message handoff
	// queue depth. When the receiving node's handler is busy, messages
	// are buffered up to this depth before being dropped. The default
	// (0 = use defaultHandoffQueueDepth = 4096) is 4× the memberlist
	// upstream default of 1024 to absorb production bursts of cache
	// invalidations. See issue #201.
	HandoffQueueDepth int
	// ReconcileInterval is how often the background reconcile pass
	// prunes dead ring entries and refreshes stale peer addresses from
	// memberlist metadata. Default 30s. Negative disables the loop
	// (the push/pull merge prune still runs).
	ReconcileInterval time.Duration
}

// Invalidator holds callbacks for applying purge, ban, and refresh
// events received via gossip. Set via SetInvalidator after cluster
// creation.
type Invalidator struct {
	PurgeFn   func(ctx context.Context, evt api.PurgeEvent) error
	BanFn     func(ctx context.Context, evt api.BanEvent) error
	RefreshFn func(ctx context.Context, evt api.RefreshEvent) error
}

// Member holds runtime state about a peer node in the cluster.
//
// Stable.
type Member struct {
	Info api.PeerInfo
}

// Cluster manages gossip membership and the consistent-hash ring.
//
// Stable.
type Cluster struct {
	local   api.PeerInfo
	inv     Invalidator
	logger  observability.Logger
	ml      *memberlist.Memberlist
	ring    *ring
	peers   map[string]*Member // keyed by NodeName
	metrics *Metrics
	adapter *slogAdapter
	// done is closed by Leave to stop the reconcile loop;
	// closeOnce makes repeated Leave calls safe. The reconcile
	// liveness fields are grouped at the tail so the small values
	// pack after the mutexes instead of padding before pointers.
	done chan struct{}
	// seqs dedups received invalidation events by (Issuer, Seq) per
	// ADR-0044: strong mode double-delivers (HTTP + gossip) and both
	// paths share this tracker so each event applies exactly once.
	seqs *seqTracker
	// gossipQueue holds pending broadcast messages to be delivered via
	// memberlist's compound-message gossip protocol.
	gossipQueue   []gossipBroadcast
	cfg           Config
	mu            sync.RWMutex
	gossipMu      sync.Mutex
	closeOnce     sync.Once
	reconcileLive atomic.Bool
	reconcileWg   sync.WaitGroup
}

// New creates a Cluster and starts the gossip listener. Call Join
// afterwards to connect to existing peers.
//
// Stable.
func New(cfg Config) (*Cluster, error) {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	if cfg.VirtualNodes <= 0 {
		cfg.VirtualNodes = 256
	}
	if cfg.GossipApplyTimeout <= 0 {
		cfg.GossipApplyTimeout = 100 * time.Millisecond
	}
	if cfg.HandoffQueueDepth == 0 {
		cfg.HandoffQueueDepth = defaultHandoffQueueDepth
	}
	if cfg.HandoffQueueDepth < 0 {
		return nil, fmt.Errorf("cluster: HandoffQueueDepth must be >= 0, got %d", cfg.HandoffQueueDepth)
	}
	if cfg.HandoffQueueDepth > MaxHandoffQueueDepth {
		return nil, fmt.Errorf("cluster: HandoffQueueDepth must be <= %d, got %d (each slot costs a pointer + message header per peer)",
			MaxHandoffQueueDepth, cfg.HandoffQueueDepth)
	}

	c := &Cluster{
		cfg:    cfg,
		peers:  make(map[string]*Member),
		local:  cfg.PeerInfo,
		logger: cfg.Logger,
		ring:   newRing(cfg.VirtualNodes),
		seqs:   newSeqTracker(),
		done:   make(chan struct{}),
	}

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.Name = cfg.NodeName
	mlCfg.HandoffQueueDepth = cfg.HandoffQueueDepth
	// Bridge memberlist's stdlib log output into slog so gossip diagnostics are structured.
	// The adapter holds an atomic.Pointer[Metrics] so SetMetrics can be called
	// concurrently with memberlist's logging goroutine (which starts inside
	// memberlist.Create, before the caller can call SetMetrics).
	adapter := newSlogAdapter(c.logger)
	mlCfg.LogOutput = adapter
	c.adapter = adapter
	mlCfg.Delegate = c
	mlCfg.Events = c
	// Use the configured PushPullInterval if set (integration tests use
	// 2 s for fast convergence). If zero, fall back to a faster default
	// of 5 s instead of memberlist's 30 s so invalidations propagate
	// promptly in production deployments as well.
	if cfg.PushPullInterval > 0 {
		mlCfg.PushPullInterval = cfg.PushPullInterval
	} else {
		mlCfg.PushPullInterval = DefaultPushPullInterval
	}

	if cfg.BindAddr != "" {
		host, portStr, err := net.SplitHostPort(cfg.BindAddr)
		if err != nil {
			return nil, fmt.Errorf("cluster: bad bind addr %q: %w", cfg.BindAddr, err)
		}
		var port int
		if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
			return nil, fmt.Errorf("cluster: bad port in %q: %w", cfg.BindAddr, err)
		}
		mlCfg.BindAddr = host
		mlCfg.BindPort = port
	}
	if cfg.AdvertiseAddr != "" {
		host, portStr, err := net.SplitHostPort(cfg.AdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("cluster: bad advertise addr %q: %w", cfg.AdvertiseAddr, err)
		}
		var port int
		if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
			return nil, fmt.Errorf("cluster: bad port in %q: %w", cfg.AdvertiseAddr, err)
		}
		mlCfg.AdvertiseAddr = host
		mlCfg.AdvertisePort = port
	}

	ml, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("cluster: create memberlist: %w", err)
	}
	c.ml = ml
	c.addPeer(cfg.NodeName, cfg.PeerInfo)
	c.startReconcileLoop(cfg.ReconcileInterval)
	return c, nil
}

// startReconcileLoop starts the background reconcile loop unless
// interval is negative (disabled). Zero applies the default interval.
func (c *Cluster) startReconcileLoop(interval time.Duration) {
	if interval < 0 {
		return
	}
	if interval == 0 {
		interval = DefaultReconcileInterval
	}
	c.reconcileLive.Store(true)
	c.reconcileWg.Add(1)
	go c.reconcileLoop(interval)
}

// Join connects to the given seed addresses.
func (c *Cluster) Join(seeds []string) (int, error) {
	if len(seeds) == 0 {
		return 0, nil
	}
	n, err := c.ml.Join(seeds)
	if err != nil {
		return n, fmt.Errorf("cluster: join %v: %w", seeds, err)
	}
	c.logger.Info("cluster joined", "peers", n, "seeds", seeds)
	return n, nil
}

// Members returns a snapshot of all known live peers.
func (c *Cluster) Members() []api.PeerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]api.PeerInfo, 0, len(c.peers))
	for _, m := range c.peers {
		out = append(out, m.Info)
	}
	return out
}

// Owner returns the PeerInfo of the node that owns the given cache
// key according to the consistent-hash ring. Returns the local node
// info if the cluster has only one member. Returns a zero PeerInfo
// when the ring is empty (all peers removed); the caller must treat
// this as "unknown owner" and fall back to origin.
func (c *Cluster) Owner(key api.Key) api.PeerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	name := c.ring.get(key)
	if name == "" {
		c.metrics.IncRingEmpty()
		c.logger.Warn("cluster: ring empty, cannot determine owner",
			"key", key, "peers", len(c.peers))
		return api.PeerInfo{}
	}
	if m, ok := c.peers[name]; ok {
		return m.Info
	}
	return c.local
}

// IsLocal reports whether the given cache key is owned by this node.
func (c *Cluster) IsLocal(key api.Key) bool {
	return c.Owner(key).Name == c.cfg.NodeName
}

// Digest returns the ring digest for gossip comparison.
func (c *Cluster) Digest() api.RingDigest {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.digest()
}

// Leave announces departure and shuts down the gossip layer.
func (c *Cluster) Leave(ctx context.Context) error {
	c.closeOnce.Do(func() { close(c.done) })
	c.reconcileWg.Wait()
	c.adapter.markClosing()
	if err := c.ml.Leave(0); err != nil {
		c.logger.Warn("cluster leave error", "error", err)
	}
	return c.ml.Shutdown()
}

// ---- memberlist.Delegate interface ----

// NodeMeta serialises PeerInfo as the node's user metadata.
func (c *Cluster) NodeMeta(limit int) []byte {
	b, _ := json.Marshal(c.local)
	if len(b) > limit {
		return b[:limit]
	}
	return b
}

// NotifyMsg handles incoming gossip user messages (purge/ban events).
// Binary frames (purge/ban) are dispatched by the msgType byte;
// JSON frames are dispatched by the "type" field. Malformed or unrecognised
// payloads are logged and skipped.
func (c *Cluster) NotifyMsg(msg []byte) {
	if IsBinaryFrame(msg) {
		c.handleBinaryGossip(msg)
		return
	}
	c.handleJSONGossip(msg)
}

// SeenFromPeer reports whether an invalidation event from issuer with
// the given Seq has already been applied, and records it otherwise.
// The HTTP peer endpoints use it to dedup the gossip fallback copy of
// each event (ADR-0044). It returns false for events that carry no
// issuer, so legacy senders are never dropped.
func (c *Cluster) SeenFromPeer(issuer string, seq uint64) bool {
	return c.seqs.seen(issuer, seq)
}

// seenFromPeer is the nil-safe internal form used by the gossip
// receive path. Clusters constructed outside New (tests) have a nil
// seqs tracker; they never dedup.
func (c *Cluster) seenFromPeer(issuer string, seq uint64) bool {
	if c.seqs == nil {
		return false
	}
	return c.seqs.seen(issuer, seq)
}

func (c *Cluster) handleBinaryGossip(msg []byte) {
	switch GossipMsgType(msg) {
	case msgTypePurge:
		c.handleGossipPurge(msg)
	case msgTypePurgeBatch:
		c.handleGossipPurgeBatch(msg)
	case msgTypeBan:
		c.handleGossipBan(msg)
	case msgTypeRefresh:
		c.handleGossipRefresh(msg)
	case msgTypeRefreshBatch:
		c.handleGossipRefreshBatch(msg)
	default:
		c.logger.Debug("cluster: unrecognized binary gossip msgType", "msgType", GossipMsgType(msg), "len", len(msg))
	}
}

// handleGossipPurge applies a single purge gossip frame.
func (c *Cluster) handleGossipPurge(msg []byte) {
	if c.inv.PurgeFn == nil {
		return
	}
	evt, err := DecodePurgeGossip(msg)
	if err != nil {
		c.logger.Warn("cluster: gossip purge decode failed", "error", err)
		return
	}
	if c.seenFromPeer(evt.Issuer, evt.Seq) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.GossipApplyTimeout)
	defer cancel()
	if err := c.inv.PurgeFn(ctx, evt); err != nil {
		c.logger.Warn("cluster: gossip purge apply failed", "error", err)
		return
	}
	c.metrics.IncGossipInvalidation("purge")
	c.logger.Info("received purge from peer",
		"key", evt.Key,
		"issuer", evt.Issuer,
		"seq", evt.Seq,
	)
}

// handleGossipPurgeBatch applies a batched purge gossip frame,
// deduping events already delivered via the HTTP fan-out path
// (ADR-0044).
func (c *Cluster) handleGossipPurgeBatch(msg []byte) {
	if c.inv.PurgeFn == nil {
		return
	}
	evts, err := DecodePurgeBatchGossip(msg)
	if err != nil {
		c.logger.Warn("cluster: gossip purge batch decode failed", "error", err)
		return
	}
	applied := 0
	for _, evt := range evts {
		if c.seenFromPeer(evt.Issuer, evt.Seq) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.GossipApplyTimeout)
		err := c.inv.PurgeFn(ctx, evt)
		cancel()
		if err != nil {
			c.logger.Warn("cluster: gossip purge batch apply failed", "error", err, "issuer", evt.Issuer, "seq", evt.Seq)
			continue
		}
		applied++
	}
	if applied > 0 {
		c.metrics.IncGossipInvalidation("purge_batch")
		c.logger.Info("received purge batch from peer", "events", len(evts), "applied", applied)
	}
}

// handleGossipBan applies a single ban gossip frame.
func (c *Cluster) handleGossipBan(msg []byte) {
	if c.inv.BanFn == nil {
		return
	}
	evt, err := DecodeBanGossip(msg)
	if err != nil {
		c.logger.Warn("cluster: gossip ban decode failed", "error", err)
		return
	}
	if c.seenFromPeer(evt.Issuer, evt.Seq) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.GossipApplyTimeout)
	defer cancel()
	if err := c.inv.BanFn(ctx, evt); err != nil {
		c.logger.Warn("cluster: gossip ban apply failed", "error", err)
		return
	}
	c.metrics.IncGossipInvalidation("ban")
	c.logger.Info("received ban from peer",
		"issuer", evt.Issuer,
		"seq", evt.Seq,
	)
}

// handleGossipRefresh applies a single refresh gossip frame.
func (c *Cluster) handleGossipRefresh(msg []byte) {
	if c.inv.RefreshFn == nil {
		return
	}
	evt, err := DecodeRefreshGossip(msg)
	if err != nil {
		c.logger.Warn("cluster: gossip refresh decode failed", "error", err)
		return
	}
	if c.seenFromPeer(evt.Issuer, evt.Seq) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.GossipApplyTimeout)
	defer cancel()
	if err := c.inv.RefreshFn(ctx, evt); err != nil {
		c.logger.Warn("cluster: gossip refresh apply failed", "error", err)
		return
	}
	c.metrics.IncGossipInvalidation("refresh")
	c.logger.Info("received refresh from peer",
		"key", evt.Key,
		"issuer", evt.Issuer,
		"seq", evt.Seq,
	)
}

// handleGossipRefreshBatch applies a batched refresh gossip frame,
// deduping events already delivered via the HTTP fan-out path.
func (c *Cluster) handleGossipRefreshBatch(msg []byte) {
	if c.inv.RefreshFn == nil {
		return
	}
	evts, err := DecodeRefreshBatchGossip(msg)
	if err != nil {
		c.logger.Warn("cluster: gossip refresh batch decode failed", "error", err)
		return
	}
	applied := 0
	for _, evt := range evts {
		if c.seenFromPeer(evt.Issuer, evt.Seq) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.GossipApplyTimeout)
		err := c.inv.RefreshFn(ctx, evt)
		cancel()
		if err != nil {
			c.logger.Warn("cluster: gossip refresh batch apply failed", "error", err, "issuer", evt.Issuer, "seq", evt.Seq)
			continue
		}
		applied++
	}
	if applied > 0 {
		c.metrics.IncGossipInvalidation("refresh_batch")
		c.logger.Info("received refresh batch from peer", "events", len(evts), "applied", applied)
	}
}

func (c *Cluster) handleJSONGossip(msg []byte) {
	var hdr struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(msg, &hdr); err != nil {
		c.logger.Debug("cluster: malformed gossip message", "error", err)
		return
	}
	c.logger.Debug("cluster: unrecognized gossip message", "type", hdr.Type, "len", len(msg))
}

// QueueBroadcast enqueues a message for gossip delivery. The message is
// sent to all peers by memberlist's compound-message protocol alongside
// normal heartbeat traffic, providing a reliable secondary delivery path
// for purge/ban events even if a peer's admin HTTP port is temporarily
// unreachable.
func (c *Cluster) QueueBroadcast(msg []byte) {
	c.gossipMu.Lock()
	c.gossipQueue = append(c.gossipQueue, gossipBroadcast{data: msg})
	c.gossipMu.Unlock()
	// In strong mode, HTTP fan-out is the primary invalidation delivery
	// path. The direct SendBestEffort is redundant — memberlist's gossip
	// protocol will propagate the message as a fallback. Skipping it
	// halves invalidation network traffic in strong mode.
	//
	// In eventual mode, there is no HTTP fan-out, so the direct
	// SendBestEffort remains the primary delivery path.
	if c.ml != nil && c.cfg.Mode != "strong" {
		for _, n := range c.ml.Members() {
			_ = c.ml.SendBestEffort(n, msg)
		}
	}
}

// GetBroadcasts returns pending broadcast messages up to the byte limit.
// memberlist calls this on every gossip round; we drain the queue.
func (c *Cluster) GetBroadcasts(overhead, limit int) [][]byte {
	c.gossipMu.Lock()
	defer c.gossipMu.Unlock()
	if len(c.gossipQueue) == 0 {
		return nil
	}
	var out [][]byte
	used := 0
	var remaining []gossipBroadcast
	for _, b := range c.gossipQueue {
		if used+overhead+len(b.data) > limit {
			remaining = append(remaining, b)
			continue
		}
		out = append(out, b.data)
		used += overhead + len(b.data)
	}
	c.gossipQueue = remaining
	return out
}

// gossipBroadcast is a single pending gossip message.
type gossipBroadcast struct {
	data []byte
}

// LocalState serialises the ring digest for peer reconciliation.
// Peers exchange digests on every full-state push/pull cycle; if
// digests differ they re-reconcile their peer tables. The join flag
// is true on the first sync after joining.
func (c *Cluster) LocalState(_ bool) []byte {
	digest := c.Digest()
	b, _ := json.Marshal(digest)
	return b
}

// MergeRemoteState reconciles a remote node's ring digest with the
// local peer table. It does two things:
//
//   - Add path (digest mismatch only): if the remote node reports peers
//     this node doesn't know about, re-parse their NodeMeta as PeerInfo
//     and add them to the ring so ownership stays consistent across
//     restarts. Matching digests imply identical peer sets, so the add
//     loop can safely skip.
//   - Prune path (always): remove peers present locally but absent from
//     memberlist's live member set. Dead peers evicted during partition
//     recovery would otherwise stay in the ring forever, routing keys to
//     dead nodes (issue #305). The prune must run even when digests
//     match: when every peer holds the same stale ring (e.g. after an
//     HPA scale-down where push/pull resurrected the dead node during
//     the convergence window), matching digests are precisely the
//     equilibrium that prevents cleanup (issue #648).
func (c *Cluster) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	var remote api.RingDigest
	if err := json.Unmarshal(buf, &remote); err != nil {
		c.logger.Debug("cluster: bad remote state", "error", err)
		return
	}
	liveMembers := c.ml.Members()
	local := c.Digest()
	if local.Hash != remote.Hash {
		c.logger.Debug("cluster: ring digest mismatch, re-syncing",
			"local", local.Hash, "remote", remote.Hash,
			"join", join)
		for _, n := range liveMembers {
			c.mu.RLock()
			_, ok := c.peers[n.Name]
			c.mu.RUnlock()
			if !ok {
				c.NotifyJoin(n)
			}
		}
	}
	c.pruneStalePeers(liveMembers)
}

// pruneStalePeers removes ring entries that are absent from the given
// memberlist live member set (issue #305, #648). Callers pass
// c.ml.Members() so the prune reflects this node's own liveness view,
// not a remote ring's.
func (c *Cluster) pruneStalePeers(liveMembers []*memberlist.Node) {
	liveSet := make(map[string]struct{}, len(liveMembers))
	for _, n := range liveMembers {
		liveSet[n.Name] = struct{}{}
	}
	c.mu.RLock()
	stale := make([]string, 0, len(c.peers))
	for name := range c.peers {
		if _, ok := liveSet[name]; !ok {
			stale = append(stale, name)
		}
	}
	c.mu.RUnlock()
	for _, name := range stale {
		c.removePeer(name)
		c.logger.Info("cluster: pruned stale peer", "name", name)
	}
}

// reconcileOnce self-heals the local peer set from memberlist's live
// member view. It complements the event delegates (NotifyJoin/Leave)
// and the push/pull merge prune, which only run when memberlist pushes
// an event or a remote state exchange happens. Two failure modes are
// healed here:
//
//   - A peer that left without a delivered NotifyLeave (e.g. a pod
//     killed mid-partition, or a resurrected-then-dead entry from
//     issue #648) lingers in the ring between merges, and every
//     peer-fetch routed to it pays a full dial timeout.
//   - A live member whose recorded PeerInfo still carries a
//     pre-restart address (observed in production after a rolling
//     restart: memberlist reports the node alive at its new address,
//     but the ring entry keeps the old AdminAddr, so peer fetches dial
//     a dead IP indefinitely). The entry is re-read from the
//     memberlist node metadata and refreshed in place.
func (c *Cluster) reconcileOnce() {
	liveMembers := c.ml.Members()
	c.pruneStalePeers(liveMembers)
	for _, n := range liveMembers {
		var info api.PeerInfo
		if err := json.Unmarshal(n.Meta, &info); err != nil {
			continue
		}
		info.Name = n.Name
		c.mu.RLock()
		existing, ok := c.peers[n.Name]
		c.mu.RUnlock()
		if !ok {
			c.addPeer(n.Name, info)
			continue
		}
		old := existing.Info
		if info.Addr != old.Addr || info.AdminAddr != old.AdminAddr || info.DataAddr != old.DataAddr {
			c.addPeer(n.Name, info)
			c.logger.Info("cluster: refreshed stale peer address from memberlist",
				"name", n.Name, "addr", info.Addr, "admin_addr", info.AdminAddr,
				"old_admin_addr", old.AdminAddr)
		}
	}
}

// reconcileLoop periodically runs reconcileOnce so stale ring state
// heals within one interval regardless of memberlist event delivery
// or push/pull timing. Started by New when ReconcileInterval is not
// negative; stopped by Leave.
func (c *Cluster) reconcileLoop(interval time.Duration) {
	defer c.reconcileWg.Done()
	defer c.reconcileLive.Store(false)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.reconcileOnce()
		}
	}
}

// reconcileRunning reports whether the reconcile loop goroutine is
// still registered (used by tests to verify shutdown).
func (c *Cluster) reconcileRunning() bool {
	return c.reconcileLive.Load()
}

// ---- memberlist.EventDelegate ----

// NotifyJoin is called when a new node joins.
func (c *Cluster) NotifyJoin(n *memberlist.Node) {
	var info api.PeerInfo
	if err := json.Unmarshal(n.Meta, &info); err != nil {
		c.logger.Warn("cluster: malformed peer meta", "node", n.Name, "error", err)
		info.Name = n.Name
		info.Addr = fmt.Sprintf("%s:%d", n.Addr, n.Port)
	}
	c.addPeer(n.Name, info)
	c.logger.Info("cluster peer joined", "name", n.Name, "addr", info.Addr)
}

// NotifyLeave is called when a node leaves or fails. The local node
// is never removed from its own ring — memberlist calls NotifyLeave
// for self during graceful Leave(), and removing self would empty the
// ring and cause Owner to fail open to single-node ownership (issue #305).
func (c *Cluster) NotifyLeave(n *memberlist.Node) {
	if n.Name == c.cfg.NodeName {
		c.logger.Debug("cluster: ignoring self-leave notification")
		return
	}
	c.removePeer(n.Name)
	c.logger.Info("cluster peer left", "name", n.Name)
}

// NotifyUpdate is called when a node updates its metadata.
func (c *Cluster) NotifyUpdate(n *memberlist.Node) {
	c.NotifyJoin(n)
}

func (c *Cluster) addPeer(name string, info api.PeerInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peers[name] = &Member{Info: info}
	// Remove any existing vnodes first so the ring doesn't accumulate
	// duplicates on resurrection cycles (issue #648). ring.add appends
	// without checking, so a re-add without remove would grow r.nodes
	// unboundedly across HPA scale-up/down cycles.
	c.ring.remove(name)
	c.ring.add(name, c.cfg.VirtualNodes)
}

func (c *Cluster) removePeer(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.peers, name)
	c.ring.remove(name)
}

// ---- Consistent-hash ring ----

type ring struct {
	owners map[uint64]string // virtual hash → real node name
	nodes  []uint64          // sorted virtual node hashes
	vnodes int
}

func newRing(vnodes int) *ring {
	return &ring{
		vnodes: vnodes,
		owners: make(map[uint64]string),
	}
}

func (r *ring) add(name string, vnodes int) {
	for i := range vnodes {
		h := xxhash.Sum64String(fmt.Sprintf("%s-%d", name, i))
		r.nodes = append(r.nodes, h)
		r.owners[h] = name
	}
	slices.Sort(r.nodes)
}

func (r *ring) remove(name string) {
	newNodes := r.nodes[:0]
	for _, h := range r.nodes {
		if r.owners[h] == name {
			delete(r.owners, h)
		} else {
			newNodes = append(newNodes, h)
		}
	}
	r.nodes = newNodes
}

func (r *ring) get(key api.Key) string {
	if len(r.nodes) == 0 {
		return ""
	}
	h := key.Hash64()
	idx := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i] >= h
	})
	if idx == len(r.nodes) {
		idx = 0
	}
	return r.owners[r.nodes[idx]]
}

func (r *ring) digest() api.RingDigest {
	h := xxhash.New()
	for _, n := range r.nodes {
		b := [8]byte{
			byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32), //nolint:gosec // byte truncation intentional for hash input
			byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n), //nolint:gosec
		}
		_, _ = h.Write(b[:])
	}
	return api.RingDigest{
		Hash: h.Sum64(),
		Size: len(r.owners) / r.vnodes,
	}
}

// RingSegments returns the proportional hash-space ownership for every real
// node in the consistent-hash ring, suitable for a visual ring-band diagram.
// Returns nil when the cluster has no members.
//
// Stable.
func (c *Cluster) RingSegments() []api.RingSegment {
	c.mu.RLock()
	segs := c.ring.segments()
	c.mu.RUnlock()
	return segs
}

// segments computes per-node hash-space ownership fractions summing to 1.0.
func (r *ring) segments() []api.RingSegment {
	if len(r.nodes) == 0 {
		return nil
	}
	const maxU64 = float64(1<<64 - 1)
	fracs := make(map[string]float64, len(r.owners)/max(r.vnodes, 1))
	for i, h := range r.nodes {
		var next uint64
		if i+1 < len(r.nodes) {
			next = r.nodes[i+1]
		} else {
			next = ^uint64(0)
		}
		var arc float64
		if next >= h {
			arc = float64(next - h)
		} else {
			arc = float64(^uint64(0)-h) + float64(next) // wrap-around
		}
		fracs[r.owners[h]] += arc / maxU64
	}
	out := make([]api.RingSegment, 0, len(fracs))
	for name, frac := range fracs {
		out = append(out, api.RingSegment{NodeName: name, Frac: frac})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].NodeName < out[j].NodeName
	})
	return out
}

// Config returns the cluster configuration, useful for dashboard metadata.
func (c *Cluster) Config() Config {
	return c.cfg
}

// Mode returns the cluster consistency mode ("strong" or "eventual").
func (c *Cluster) Mode() string { return c.cfg.Mode }

// SetMetrics registers cluster-level Prometheus counters. Must be called
// before Join. Nil receiver is a no-op. Safe to call concurrently with
// memberlist's logging goroutine — the adapter stores the pointer atomically.
func (c *Cluster) SetMetrics(m *Metrics) {
	c.metrics = m
	if c.adapter != nil {
		c.adapter.setMetrics(m)
	}
}

// SetInvalidator registers callbacks for applying purge and ban events
// received via gossip. Must be called before Join.
func (c *Cluster) SetInvalidator(inv Invalidator) { c.inv = inv }
