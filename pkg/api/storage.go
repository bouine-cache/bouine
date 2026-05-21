package api

import (
	"net/http"
	"time"
)

// Key is the canonical cache key. It is a plain uint64 xxhash digest
// of the normalized request attributes (scheme + host + path + query +
// method + Vary headers). See PLAN.md §3.2.
//
// Stable.
type Key uint64

// Object is the cached response stored by the storage layer. It holds
// both the HTTP metadata and the body bytes (or a reference to the
// warm-tier segment for large objects).
//
// Unstable until phase 3 ships.
type Object struct {
	// Key is the primary cache key.
	Key Key `json:"key"`
	// VaryKey is the secondary key derived from Vary headers. Empty
	// string if the response does not Vary.
	VaryKey string `json:"vary_key,omitempty"`
	// StatusCode is the origin HTTP status code.
	StatusCode int `json:"status_code"`
	// Header is the stored response headers. Hop-by-hop headers are
	// stripped at store time.
	Header http.Header `json:"header"`
	// Body is the response body. For objects in the hot tier this is
	// the full body; for warm-tier objects it may be nil (body lives
	// on disk in the mmap segment).
	Body []byte `json:"body,omitempty"`
	// BodySize is the total body size in bytes, whether the body is
	// in-memory or on disk.
	BodySize int64 `json:"body_size"`

	// StoredAt is the wall-clock time the object was first stored.
	StoredAt time.Time `json:"stored_at"`
	// TTL is the remaining freshness lifetime at store time.
	TTL time.Duration `json:"ttl"`
	// StaleWhileRevalidate is the SWR window (RFC 5861).
	StaleWhileRevalidate time.Duration `json:"swr,omitempty"`
	// StaleIfError is the SIE window (RFC 5861).
	StaleIfError time.Duration `json:"sie,omitempty"`

	// ETag is the strong or weak entity tag from the origin.
	ETag string `json:"etag,omitempty"`
	// LastModified is the origin's Last-Modified value.
	LastModified time.Time `json:"last_modified,omitempty"`

	// SurrogateKeys are opaque labels for grouped invalidation (deferred
	// to post-v1.0, see §18).
	SurrogateKeys []string `json:"surrogate_keys,omitempty"`

	// Hits counts how many times this object has been served.
	Hits uint64 `json:"hits"`
}

// Fresh reports whether the object is still within its TTL relative to
// now.
func (o *Object) Fresh(now time.Time) bool {
	return now.Before(o.StoredAt.Add(o.TTL))
}

// StaleButServable reports whether the object is stale but within the
// SWR or SIE window relative to now.
func (o *Object) StaleButServable(now time.Time) bool {
	if o.Fresh(now) {
		return false
	}
	expiry := o.StoredAt.Add(o.TTL)
	maxGrace := o.StaleWhileRevalidate
	if o.StaleIfError > maxGrace {
		maxGrace = o.StaleIfError
	}
	return now.Before(expiry.Add(maxGrace))
}

// BanExpr is a predicate for lazy ban-list matching. The storage layer
// evaluates these on lookup, not on write.
//
// Unstable until phase 4 ships.
type BanExpr struct {
	// HostRegex matches against the request host.
	HostRegex string `json:"host_regex,omitempty"`
	// PathRegex matches against the request path.
	PathRegex string `json:"path_regex,omitempty"`
	// SurrogateKey matches exactly against stored surrogate keys.
	SurrogateKey string `json:"surrogate_key,omitempty"`
	// CreatedAt is the wall-clock time the ban was created. Objects
	// stored after this time are not subject to the ban.
	CreatedAt time.Time `json:"created_at"`
}

// Stats is the runtime snapshot returned by Store.Stats(). Every
// counter is an atomic read.
//
// Unstable.
type Stats struct {
	// HotEntries is the number of objects in the hot tier.
	HotEntries int64 `json:"hot_entries"`
	// HotBytes is the total bytes used by hot-tier objects (bodies +
	// overhead).
	HotBytes int64 `json:"hot_bytes"`
	// WarmEntries is the number of objects in the warm tier.
	WarmEntries int64 `json:"warm_entries"`
	// WarmBytes is the total bytes used by warm-tier segments.
	WarmBytes int64 `json:"warm_bytes"`
	// Hits is the total cache hits since boot.
	Hits int64 `json:"hits"`
	// Misses is the total cache misses since boot.
	Misses int64 `json:"misses"`
	// Evictions is the total number of evictions since boot.
	Evictions int64 `json:"evictions"`
}
