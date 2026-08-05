package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/hashicorp/memberlist"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
)

// Config controls the cluster membership layer.
//
// Stable.
type Config struct {
	// NodeName is the unique identifier for this node (pod name).
	// Defaults to the hostname if empty.
	NodeName string
	// BindAddr is the gossip listener address (host:port).
	BindAddr string
	// AdvertiseAddr is the address announced to peers (optional;
	// useful behind NAT or in K8s where the pod IP differs).
	AdvertiseAddr string
	// Join is the list of seed addresses for bootstrapping.
	Join []string
	// PeerInfo is the metadata this node broadcasts to peers.
	PeerInfo api.PeerInfo
	// Logger receives gossip and lifecycle records. Defaults to
	// a SampledLogger wrapping slog.Default().
	Logger observability.Logger
	// VirtualNodes is the number of virtual nodes per real node on
	// the consistent hash ring. Default 256.
	VirtualNodes int
	// Mode determines how cache keys are distributed across the cluster.
	// "strong" uses a consistent hash ring with peer fetch on miss.
	// "eventual" caches locally with no peer fetch; invalidation by gossip.
	// Defaults to "strong" for backward compatibility.
	Mode string
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
	// (0 = use 4096) is 4× the memberlist upstream default of 1024 to
	// absorb production bursts of cache invalidations. See issue #201.
	HandoffQueueDepth int
}

// Invalidator holds callbacks for applying purge and ban events received
// via gossip. Set via SetInvalidator after cluster creation.
type Invalidator struct {
	PurgeFn func(ctx context.Context, evt api.PurgeEvent) error
	BanFn   func(ctx context.Context, evt api.BanEvent) error
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
	cfg    Config
	ml     *memberlist.Memberlist
	ring   *ring
	mu     sync.RWMutex
	peers  map[string]*Member // keyed by NodeName
	local  api.PeerInfo
	logger observability.Logger
	// gossipQueue holds pending broadcast messages to be delivered via
	// memberlist's compound-message gossip protocol.
	gossipMu    sync.Mutex
	gossipQueue []gossipBroadcast
	inv         Invalidator
	metrics     *Metrics
}

// defaultHandoffQueueDepth is the memberlist per-peer message buffer
// depth. memberlist's upstream default is 1024; bouine uses 4096 to
// absorb production bursts of cache invalidations (issue #201).
const defaultHandoffQueueDepth = 4096

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

	c := &Cluster{
		cfg:    cfg,
		peers:  make(map[string]*Member),
		local:  cfg.PeerInfo,
		logger: cfg.Logger,
		ring:   newRing(cfg.VirtualNodes),
	}

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.Name = cfg.NodeName
	mlCfg.HandoffQueueDepth = cfg.HandoffQueueDepth
	// Bridge memberlist's stdlib log output into slog so gossip diagnostics are structured.
	// The onDrop callback lets us count "handler queue full" warnings as Prometheus metrics.
	mlCfg.LogOutput = newSlogAdapter(c.logger, c.incGossipDrop)
	mlCfg.Delegate = c
	mlCfg.Events = c
	// Use the configured PushPullInterval if set (integration tests use
	// 2 s for fast convergence). If zero, fall back to a faster default
	// of 5 s instead of memberlist's 30 s so invalidations propagate
	// promptly in production deployments as well.
	if cfg.PushPullInterval > 0 {
		mlCfg.PushPullInterval = cfg.PushPullInterval
	} else {
		mlCfg.PushPullInterval = 5 * time.Second
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
	return c, nil
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
// info if the cluster has only one member.
func (c *Cluster) Owner(key api.Key) api.PeerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	name := c.ring.get(key)
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

func (c *Cluster) handleBinaryGossip(msg []byte) {
	switch GossipMsgType(msg) {
	case msgTypePurge:
		if c.inv.PurgeFn == nil {
			return
		}
		evt, err := DecodePurgeGossip(msg)
		if err != nil {
			c.logger.Warn("cluster: gossip purge decode failed", "error", err)
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
	case msgTypeBan:
		if c.inv.BanFn == nil {
			return
		}
		evt, err := DecodeBanGossip(msg)
		if err != nil {
			c.logger.Warn("cluster: gossip ban decode failed", "error", err)
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
	default:
		c.logger.Debug("cluster: unrecognized binary gossip msgType", "msgType", GossipMsgType(msg), "len", len(msg))
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
// local peer table. If the remote node reports peers this node doesn't
// know about it re-parses their NodeMeta as PeerInfo and adds them to
// the ring so ownership stays consistent across restarts.
func (c *Cluster) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	var remote api.RingDigest
	if err := json.Unmarshal(buf, &remote); err != nil {
		c.logger.Debug("cluster: bad remote state", "error", err)
		return
	}
	local := c.Digest()
	if local.Hash == remote.Hash {
		return // rings already in sync
	}
	c.logger.Debug("cluster: ring digest mismatch, re-syncing",
		"local", local.Hash, "remote", remote.Hash,
		"join", join)
	// Re-synchronise by re-processing all live memberlist nodes so any
	// peer that was missed during network partitions or rolling restarts
	// is added to the ring.
	for _, n := range c.ml.Members() {
		c.mu.RLock()
		_, ok := c.peers[n.Name]
		c.mu.RUnlock()
		if !ok {
			c.NotifyJoin(n)
		}
	}
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

// NotifyLeave is called when a node leaves or fails.
func (c *Cluster) NotifyLeave(n *memberlist.Node) {
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
	vnodes int
	nodes  []uint64          // sorted virtual node hashes
	owners map[uint64]string // virtual hash → real node name
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
	h := uint64(key)
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
// before Join. Nil receiver is a no-op.
func (c *Cluster) SetMetrics(m *Metrics) { c.metrics = m }

// incGossipDrop is the callback for slogAdapter to increment the
// gossip-drops counter when memberlist logs a "handler queue full"
// warning. Safe to call before SetMetrics — the metrics pointer is
// nil until then, and IncGossipDrop handles nil receivers.
func (c *Cluster) incGossipDrop() {
	c.metrics.IncGossipDrop()
}

// SetInvalidator registers callbacks for applying purge and ban events
// received via gossip. Must be called before Join.
func (c *Cluster) SetInvalidator(inv Invalidator) { c.inv = inv }
