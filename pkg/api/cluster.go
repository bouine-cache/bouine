package api

import "time"

// PeerInfo describes a single cluster peer as seen by the gossip
// layer. It is broadcast in memberlist user metadata.
//
// Stable.
type PeerInfo struct {
	// Name is the unique node name, typically the pod name in K8s.
	Name string `json:"name"`
	// Addr is the peer-fetch address (host:port, mTLS HTTP/2).
	Addr string `json:"addr"`
	// AdminAddr is the admin HTTP listener (for readiness probing).
	AdminAddr string `json:"admin_addr"`
	// DataAddr is the data-plane HTTP listener.
	DataAddr string `json:"data_addr"`
	// Weight is the relative ring weight (default 1.0).
	Weight float64 `json:"weight"`
	// Version is the bouine binary version string.
	Version string `json:"version"`
	// JoinedAt is the wall-clock time the node joined.
	JoinedAt time.Time `json:"joined_at"`
}

// RingDigest is a lightweight fingerprint of the consistent-hash ring
// gossiped between peers so they can detect stale local views without
// exchanging the full ring.
//
// Stable.
type RingDigest struct {
	// Hash is a xxhash64 of the sorted member list + weights.
	Hash uint64 `json:"hash"`
	// Size is the number of real nodes in the ring.
	Size int `json:"size"`
	// Version increments on every ring mutation.
	Version uint64 `json:"version"`
}

// RingSegment describes a single node's ownership share of the
// consistent-hash ring, used by the cluster dashboard visualization.
//
// Stable.
type RingSegment struct {
	// NodeName is the owning node.
	NodeName string `json:"node_name"`
	// Frac is the fraction of the hash space owned [0.0, 1.0].
	// All segment Frac values sum to 1.0.
	Frac float64 `json:"frac"`
}

// Gossip event type constants. Every gossip message carries a "type" field
// that identifies the payload structure. This is the primary discriminator
// used by NotifyMsg to dispatch events — the previous heuristic (Method != ""
// / Key != 0 / Predicate != {}) was fragile and would silently break if
// overlapping fields were added.
const (
	GossipTypePurge       = "purge"
	GossipTypeBan         = "ban"
	GossipTypeReplication = "replication"
)

// PurgeEvent is broadcast when a key is explicitly invalidated.
//
// Stable.
type PurgeEvent struct {
	// Type identifies this as a purge event in the gossip protocol.
	Type string `json:"type"`
	// Key is the primary cache key to invalidate.
	Key Key `json:"key"`
	// VaryKey, if non-empty, targets only the variant.
	VaryKey string `json:"vary_key,omitempty"`
	// Issuer is the node name that originated the purge.
	Issuer string `json:"issuer"`
	// IssuedAt is the wall-clock time of the purge.
	IssuedAt time.Time `json:"issued_at"`
	// Seq is the monotonic sequence number from the issuer.
	Seq uint64 `json:"seq"`
}

// BanEvent is broadcast when a predicate ban is issued.
//
// Stable.
type BanEvent struct {
	// Type identifies this as a ban event in the gossip protocol.
	Type string `json:"type"`
	// Predicate is the ban expression.
	Predicate BanExpr `json:"predicate"`
	// Issuer is the node name that originated the ban.
	Issuer string `json:"issuer"`
	// IssuedAt is the wall-clock time of the ban.
	IssuedAt time.Time `json:"issued_at"`
	// Seq is the monotonic sequence number from the issuer.
	Seq uint64 `json:"seq"`
}

// PeerFetchRequest is the HTTP request body for a peer cache lookup.
//
// Stable.
type PeerFetchRequest struct {
	// Key is the cache key being requested.
	Key Key `json:"key"`
	// VaryKey is the variant key (empty = any variant).
	VaryKey string `json:"vary_key,omitempty"`
	// Hops is the number of peers already traversed (T36 loop guard).
	Hops int `json:"hops"`
}

// ReplicationEvent is a legacy type retained for wire compatibility.
// Full replication mode has been removed; this struct is no longer
// populated or broadcast.
//
// The Method field acts as a discriminator for gossip deserialisation:
// PurgeEvent has Key != 0 but no Method; BanEvent has non-empty
// Predicate but no Method. Deserialisers try ReplicationEvent first
// (Method != ""), then PurgeEvent (Key != 0), then BanEvent.
type ReplicationEvent struct {
	// Type identifies this as a replication event in the gossip protocol.
	Type string `json:"type"`
	// Method is the HTTP method of the original request ("GET" or "HEAD").
	Method string `json:"method"`
	// Object is the full cached response to be stored locally.
	Object *Object `json:"object,omitempty"`
	// Issuer is the node name that cached the response.
	Issuer string `json:"issuer"`
	// IssuedAt is the wall-clock time the response was stored.
	IssuedAt time.Time `json:"issued_at"`
	// Seq is the monotonic sequence number from the issuer.
	Seq uint64 `json:"seq"`
}
