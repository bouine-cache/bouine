package api

// KeyState reports the storage-level state of one cache key on one
// node (issue #816). It is the payload of the peer keys-query channel
// (GET /v1/peer/keys/find) and the local block of the admin
// GET /v1/keys/find response. Storage reports raw facts only;
// derived values (age, remaining freshness) are computed by the
// reporting layer, which owns the clock.
//
// Unstable.
//
//nolint:govet // fieldalignment: field order mirrors the wire JSON order
type KeyState struct {
	// Present reports whether the key exists in a local tier. A
	// banned object is still Present: it physically sits in the tier
	// until the next lookup lazily evicts it.
	Present bool `json:"present"`
	// Tier is the storage tier holding the object: "hot" or "warm".
	// Empty when Present is false.
	Tier string `json:"tier,omitempty"`
	// SizeBytes is the stored body size. For warm-tier objects this is
	// the body size recorded in the on-disk record, not the record
	// overhead. 0 when Present is false.
	SizeBytes int64 `json:"size_bytes,omitempty"`
	// StoredAt is the fill time, RFC 3339 UTC. Empty when the tier
	// does not record it.
	StoredAt string `json:"stored_at,omitempty"`
	// TTLSeconds is the freshness lifetime stamped at fill time
	// (remaining lifetime at store time, per RFC 9111 §5.1 age
	// accounting).
	TTLSeconds int64 `json:"ttl_seconds,omitempty"`
	// SWRSeconds is the stale-while-revalidate window stamped at fill
	// time (RFC 5861 §3). 0 when the response carried none. Filled by
	// the explain plane's lifecycle gauge (issue #814).
	SWRSeconds int64 `json:"swr_seconds,omitempty"`
	// SIESeconds is the stale-if-error window stamped at fill time
	// (RFC 5861 §4). 0 when the response carried none.
	SIESeconds int64 `json:"sie_seconds,omitempty"`
	// CacheControl is the merged Cache-Control header from the origin
	// response, stored verbatim at cache-fill time (the same string
	// the codec carries, ADR-0053). The explain plane re-derives the
	// cacheability verdict from it (issue #814).
	CacheControl string `json:"cache_control,omitempty"`
	// StatusCode is the origin status the object was stored from.
	StatusCode int `json:"status_code,omitempty"`
	// Hits is the object's lifetime access counter (SIEVE slow path
	// and warm loads; the fast path does not bump it per serve).
	Hits uint64 `json:"hits,omitempty"`
	// WindowHits is the hot tier's per-TTL-window access count; 0 for
	// warm-only objects.
	WindowHits int64 `json:"window_hits,omitempty"`
	// Vary is the stored Vary header value on a variant resolver (the
	// entry that names the Vary fields), or empty on a plain object.
	Vary string `json:"vary,omitempty"`
	// VaryKey is the stored variant key hex on a variant entry, or
	// empty on the resolver or a plain object.
	VaryKey string `json:"vary_key,omitempty"`
	// Pool is the origin pool the object was filled from.
	Pool string `json:"pool,omitempty"`
	// SurrogateKeys are the stored grouped-invalidation labels.
	SurrogateKeys []string `json:"surrogate_keys,omitempty"`
	// KeepGrace marks a stayin_alive object the TTL reaper must spare
	// while its origin pool cannot refill a miss.
	KeepGrace bool `json:"keep_grace,omitempty"`
	// BackedToWarm reports a hot entry with a live warm copy: hot-tier
	// eviction prefers it (demotion over deletion).
	BackedToWarm bool `json:"backed_to_warm,omitempty"`
	// Protected reports a warm entry shielded from warm SIEVE
	// eviction because a live hot entry backs it.
	Protected bool `json:"protected,omitempty"`
	// Banned reports the object matches an active lazy ban: the next
	// lookup evicts it and counts a miss.
	Banned bool `json:"banned,omitempty"`
}
