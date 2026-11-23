package api

import "time"

// PeerInfo describes a single cluster peer as seen by the gossip
// layer. It is broadcast in memberlist user metadata.
//
// Stable.
type PeerInfo struct {
	// JoinedAt is the wall-clock time the node joined.
	JoinedAt time.Time `json:"joined_at"`
	// Name is the unique node name, typically the pod name in K8s.
	Name string `json:"name"`
	// Addr is the peer-fetch address (host:port, mTLS HTTP/1.1, ADR-0035).
	Addr string `json:"addr"`
	// AdminAddr is the admin HTTP listener (for readiness probing).
	AdminAddr string `json:"admin_addr"`
	// DataAddr is the data-plane HTTP listener.
	DataAddr string `json:"data_addr"`
	// Version is the bouine binary version string.
	Version string `json:"version"`
	// Weight is the relative ring weight (default 1.0).
	Weight float64 `json:"weight"`
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

// PurgeEvent is broadcast when a key is explicitly invalidated.
//
// Stable.
type PurgeEvent struct {
	// IssuedAt is the wall-clock time of the purge.
	IssuedAt time.Time `json:"issued_at"`
	// VaryKey is metadata about the variant identity that triggered the
	// purge (the BuildVaryKey assertion hex of the local object), not a
	// purge target. Receivers MUST ignore it and apply the purge to
	// evt.Key and every locally tracked variant under it: an invalidation
	// of a resource removes all its variants (RFC 9111 §4.4), and a
	// variant store key cannot be reconstructed from the assertion hex
	// alone (they use different canonicalization; ADR-0045). Senders
	// currently pass "".
	VaryKey string `json:"vary_key,omitempty"`
	// Issuer is the node name that originated the purge.
	Issuer string `json:"issuer"`
	// Seq is the monotonic sequence number from the issuer.
	Seq uint64 `json:"seq"`
	// Key is the primary cache key to invalidate.
	Key Key `json:"key"`
}

// BanEvent is broadcast when a predicate ban is issued.
//
// Stable.
type BanEvent struct {
	// Predicate is the ban expression.
	Predicate BanExpr `json:"predicate"`
	// IssuedAt is the wall-clock time of the ban.
	IssuedAt time.Time `json:"issued_at"`
	// Issuer is the node name that originated the ban.
	Issuer string `json:"issuer"`
	// Seq is the monotonic sequence number from the issuer.
	Seq uint64 `json:"seq"`
}

// RefreshEvent is broadcast when a key is soft-purged (marked stale for
// background revalidation). Unlike PurgeEvent, the object is not deleted
// — it enters the stale-while-revalidate window so the next request
// serves the stale body and triggers a conditional revalidation.
//
// Stable.
type RefreshEvent struct {
	// IssuedAt is the wall-clock time of the refresh.
	IssuedAt time.Time `json:"issued_at"`
	// Issuer is the node name that originated the refresh.
	Issuer string `json:"issuer"`
	// Seq is the monotonic sequence number from the issuer.
	Seq uint64 `json:"seq"`
	// Key is the primary cache key to soft-purge.
	Key Key `json:"key"`
}

// RefreshSurrogateEvent is broadcast when a surrogate key is refreshed
// (soft-purged by tag). Unlike RefreshEvent it carries no concrete key:
// each peer resolves the surrogate key against its own store
// (KeysBySurrogate) and soft-purges only what it holds, so one event
// covers the whole fleet without an N-key fan-out. It rides a
// dedicated gossip msgType and peer route so old nodes that do not know
// the frame kind drop it at the dispatch default instead of failing
// the decode.
//
// Unstable.
type RefreshSurrogateEvent struct {
	// IssuedAt is the wall-clock time of the refresh.
	IssuedAt time.Time `json:"issued_at"`
	// SurrogateKey is the exact opaque tag to soft-purge; never a
	// pattern (same rule as BanExpr.SurrogateKey).
	SurrogateKey string `json:"surrogate_key"`
	// Issuer is the node name that originated the refresh.
	Issuer string `json:"issuer"`
	// Seq is the monotonic sequence number from the issuer.
	Seq uint64 `json:"seq"`
}

// HotSetEvent is broadcast when a hot-config knob is set at runtime
// (bouine config set): peers apply the same point mutation so the
// fleet's runtime overrides stay identical. Unlike invalidation
// events it rides no gossip — strong-mode HTTP fan-out only — and
// the receiving peer route applies locally without re-broadcasting,
// so the fan-out cannot loop.
//
// Unstable: the knob surface is still growing (#815).
type HotSetEvent struct {
	// Key is the hot-config knob key (e.g. cluster.ban_ttl,
	// pool:api.health.passive.consecutive_5xx).
	Key string `json:"key"`
	// Value is the raw operator-supplied value; the receiving peer
	// validates and normalizes it through the same knob registry.
	Value string `json:"value"`
	// IssuedAt is the wall-clock time of the set.
	IssuedAt time.Time `json:"issued_at"`
	// Issuer is the node name that originated the set.
	Issuer string `json:"issuer"`
	// Seq is the monotonic sequence number from the issuer.
	Seq uint64 `json:"seq"`
}

// ConfigReloadEvent is fanned out when a node applies a config reload
// (issue #951, phase 2): the payload is a generation counter plus the
// SHA-256 of the RESOLVED config (canonical marshal of *config.Config
// after Load+Validate), never the file bytes — config.Load interpolates
// ${VAR} from each daemon's own environment, so identical mounted files
// can resolve differently across peers and only the resolved hash
// detects that divergence. The receiving peer re-reads its own boot
// path, resolves it, and applies only if gen is newer than its local
// generation AND its resolved hash matches; a mismatch replies
// stale_config so the initiator reports the peer as not converged. The
// peer endpoint never re-broadcasts, so the fan-out cannot loop.
//
// Unstable: phase 2 of #817.
type ConfigReloadEvent struct {
	// IssuedAt is the wall-clock time of the reload.
	IssuedAt time.Time `json:"issued_at"`
	// ConfigSHA256 is the hex SHA-256 of the canonical marshal of the
	// resolved *config.Config on the issuing node.
	ConfigSHA256 string `json:"config_sha256"`
	// Issuer is the node name that originated the reload.
	Issuer string `json:"issuer"`
	// Gen is the initiator's monotonically increasing config
	// generation (boot = 1, each applied reload increments).
	Gen uint64 `json:"gen"`
	// Seq is the monotonic sequence number from the issuer.
	Seq uint64 `json:"seq"`
}

// ConfigReloadAck is the POST /v1/peer/config/reload response body: a
// peer reports applied, already-current (its generation is not older
// than gen), or stale_config (its resolved config hash differs from
// the event's, i.e. ${VAR} divergence or a different file).
//
// Unstable: phase 2 of #817.
type ConfigReloadAck struct {
	// Status is "applied", "current", or "stale_config".
	Status string `json:"status"`
	// Gen is the peer's config generation after handling the event.
	Gen uint64 `json:"gen"`
}

// PeerFetchRequest is the HTTP request body for a peer cache lookup.
//
// Stable.
type PeerFetchRequest struct {
	// VaryKey is the variant key (empty = any variant).
	VaryKey string `json:"vary_key,omitempty"`
	// Hops is the number of peers already traversed (T36 loop guard).
	Hops int `json:"hops"`
	// Key is the cache key being requested.
	Key Key `json:"key"`
}
