package api

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// Key is the canonical cache key. It is a plain uint64 xxhash digest
// of the normalized request attributes (scheme + host + path + query +
// method + Vary headers).
type Key uint64

// LogValue renders the key as a zero-allocation slog.UintValue so the
// hot path never pays for string conversion. The decimal form is used
// in JSON logs; the runbook documents how to convert it to hex in log
// queries (to_hex() in Loki, format() in Grafana).
func (k Key) LogValue() slog.Value { return slog.Uint64Value(uint64(k)) }

// Hex returns the lowercase hex representation of the key. Intended for
// non-hot-path use (admin API, runbook examples). Allocates a string.
func (k Key) Hex() string { return strconv.FormatUint(uint64(k), 16) }

// String returns the decimal representation, satisfying fmt.Stringer.
func (k Key) String() string { return strconv.FormatUint(uint64(k), 10) }

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

	// CacheControl is the merged Cache-Control header from the origin
	// response, stored verbatim at cache-fill time. Avoids re-reading the
	// header map on every cache hit in Evaluate. Not serialized (it is
	// re-derived from Header on warm-tier load).
	CacheControl string `json:"-"`

	// OriginAge is the Age header value from the origin at cache-fill
	// time. Pre-parsed once so the read path never re-parses it per request.
	// Not serialized (re-derived from Header on warm-tier load).
	OriginAge time.Duration `json:"-"`
}

// Fresh reports whether the object is still within its freshness lifetime
// relative to now. This is the single source of truth for object
// freshness: every other freshness/staleness decision in the cache layer is
// computed against this same StoredAt+TTL expiry instant.
//
// Invariant: TTL is the *remaining* freshness lifetime at store time,
// not the full lifetime advertised by the origin. computeTTL subtracts
// OriginAge (the Age header value received from the upstream cache) at
// cache-fill time, so the origin's age is already baked into TTL and
// MUST NOT be re-applied here. Re-adding OriginAge would double-count it
// and declare objects stale OriginAge seconds too early — the exact bug
// that previously existed in the engine's freshness check before it was
// collapsed onto this method.
func (o *Object) Fresh(now time.Time) bool {
	return now.Before(o.StoredAt.Add(o.TTL))
}

// StaleButServable reports whether the object is stale but within the
// SWR or SIE window relative to now. Use StaleForSWR / StaleForSIE for
// semantically correct separate checks.
func (o *Object) StaleButServable(now time.Time) bool {
	return o.StaleForSWR(now) || o.StaleForSIE(now)
}

// StaleForSWR reports whether the object is stale but within the
// stale-while-revalidate window. The cache MAY serve the stale object
// immediately and refresh in the background (RFC 5861 §3).
func (o *Object) StaleForSWR(now time.Time) bool {
	if o.Fresh(now) || o.StaleWhileRevalidate == 0 {
		return false
	}
	expiry := o.StoredAt.Add(o.TTL)
	return now.Before(expiry.Add(o.StaleWhileRevalidate))
}

// StaleForSIE reports whether the object is stale but within the
// stale-if-error window. The cache MUST attempt revalidation first and
// only serve the stale object when the origin returns an error (RFC 5861 §4).
func (o *Object) StaleForSIE(now time.Time) bool {
	if o.Fresh(now) || o.StaleIfError == 0 {
		return false
	}
	expiry := o.StoredAt.Add(o.TTL)
	return now.Before(expiry.Add(o.StaleIfError))
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
