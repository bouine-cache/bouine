package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/hashicorp/memberlist"

	"github.com/thylong/bouine/pkg/api"
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
	// Logger is the structured logger.
	Logger *slog.Logger
	// VirtualNodes is the number of virtual nodes per real node on
	// the consistent hash ring. Default 256.
	VirtualNodes int
	// LoadFactor caps the maximum load per node relative to the
	// average. Default 1.25.
	LoadFactor float64
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
	logger *slog.Logger
}

// New creates a Cluster and starts the gossip listener. Call Join
// afterwards to connect to existing peers.
//
// Stable.
func New(cfg Config) (*Cluster, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.VirtualNodes <= 0 {
		cfg.VirtualNodes = 256
	}
	if cfg.LoadFactor <= 0 {
		cfg.LoadFactor = 1.25
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
	mlCfg.Logger = nil // suppress memberlist's stdlib logger; we use slog
	mlCfg.Delegate = c
	mlCfg.Events = c

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

// NotifyMsg handles incoming user messages (purge/ban events).
// Full purge broadcast implementation lands in the purge package.
func (c *Cluster) NotifyMsg([]byte) {}

// GetBroadcasts returns pending broadcast messages.
func (c *Cluster) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState serialises the ring digest for anti-entropy. Peers
// exchange digests on every full-state push/pull cycle; if digests
// differ they re-reconcile their peer tables. The join flag is true
// on the first sync after joining.
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
		if _, ok := c.peers[n.Name]; !ok {
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
	sort.Slice(r.nodes, func(i, j int) bool { return r.nodes[i] < r.nodes[j] })
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
