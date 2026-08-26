package api

import (
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/pkg/header"
)

// Object is the cached response stored by the storage layer. It holds
// both the HTTP metadata and the body bytes (or a reference to the
// warm-tier segment for large objects).
//
// Unstable until phase 3 ships.
type Object struct {
	// StoredAt is the wall-clock time the object was first stored.
	StoredAt time.Time `json:"stored_at"`
	// LastModified is the origin's Last-Modified value.
	LastModified time.Time `json:"last_modified,omitempty"`
	// FastHeader stores a pre-built *fasthttp.ResponseHeader for use by
	// serveObject's CopyTo fast path. Lazily computed on the first hit
	// and cached for subsequent hits. Stored as atomic.Value (any) to
	// avoid importing fasthttp in pkg/api. Not serialized to disk.
	FastHeader atomic.Value `json:"-"`
	// serializedHead is the lazily-computed pre-rendered HTTP response
	// header block (static headers as "Key: Value\r\n" pairs, without
	// status line or trailing \r\n). Computed on the first fast-path
	// cache hit, not at store time — objects never served via the
	// fast-path (misses, net/http path) never pay the ~512-byte cost.
	// Not serialized to disk (json:"-"). Warm-tier loads leave this nil.
	// Accessed via atomic.Pointer for race-safe lazy initialization.
	serializedHead atomic.Pointer[[]byte] `json:"-"`
	// ETag is the strong or weak entity tag from the origin.
	ETag string `json:"etag,omitempty"`
	// VaryKey is the secondary key derived from Vary headers. Empty
	// string if the response does not Vary.
	VaryKey string `json:"vary_key,omitempty"`
	// VaryValue is the stored Vary header value, pre-computed at build
	// time so the fast-path hit can skip a Map.Get scan. Empty string
	// means no Vary header (the common case).
	VaryValue string `json:"-"`
	// CacheControl is the merged Cache-Control header from the origin
	// response, stored verbatim at cache-fill time. Avoids re-reading the
	// header map on every cache hit in Evaluate. Not serialized (it is
	// re-derived from Header on warm-tier load).
	CacheControl string `json:"-"`
	// Header is the stored response headers. Hop-by-hop headers are
	// stripped at store time. Stored as a compact Map (flat slice
	// with interned keys) instead of http.Header to reduce per-entry
	// memory overhead from ~528 B to ~144 B for a typical 10-header
	// response.
	Header header.Map `json:"header"`
	// SurrogateKeys are opaque labels for grouped invalidation (deferred
	// to post-v1.0, see §18).
	SurrogateKeys []string `json:"surrogate_keys,omitempty"`
	// Body is the response body. For objects in the hot tier this is
	// the full body; for warm-tier objects it may be nil (body lives
	// on disk in the mmap segment).
	Body []byte `json:"body,omitempty"`
	// BodySize is the total body size in bytes, whether the body is
	// in-memory or on disk.
	BodySize int64 `json:"body_size"`
	// Hits counts how many times this object has been served.
	Hits uint64 `json:"hits"`
	// TTL is the remaining freshness lifetime at store time.
	TTL time.Duration `json:"ttl"`
	// OriginAge is the Age header value from the origin at cache-fill
	// time. Pre-parsed once so the read path never re-parses it per request.
	// Not serialized (re-derived from Header on warm-tier load).
	OriginAge time.Duration `json:"-"`
	// StaleIfError is the SIE window (RFC 5861).
	StaleIfError time.Duration `json:"sie,omitempty"`
	// StaleWhileRevalidate is the SWR window (RFC 5861).
	StaleWhileRevalidate time.Duration `json:"swr,omitempty"`
	// StatusCode is the origin HTTP status code.
	StatusCode int `json:"status_code"`
	// Key is the primary cache key.
	Key Key `json:"key"`
	// HasConnectionList indicates whether the stored response has a
	// Connection header listing per-connection headers that must be
	// stripped before forwarding (RFC 9110 §7.6.1). Pre-computed at
	// build time so the hit path can skip stripConnectionListedHeaders
	// when false (the common case).
	HasConnectionList bool `json:"-"`
	// HasNoCacheFields indicates whether the stored Cache-Control has a
	// no-cache="..." directive with field names. Pre-computed at build
	// time so the hit path can skip stripNoCacheFields when false.
	HasNoCacheFields bool `json:"-"`
	// HasDate indicates whether the stored response has a Date header.
	// Pre-computed at build time so the fast-path hit can skip a Map.Get
	// scan (and the subsequent AppendFormat for Date synthesis) when the
	// origin already provided a Date.
	HasDate bool `json:"-"`
	// RespNoCache indicates the response Cache-Control has no-cache.
	// Pre-computed at build time so evaluateFromRaw can skip
	// ParseCacheControl on every FastPath hit.
	RespNoCache bool `json:"-"`
	// RespMustRevalidate indicates the response Cache-Control has
	// must-revalidate or proxy-revalidate. Pre-computed at build time.
	RespMustRevalidate bool `json:"-"`
}

// LoadSerializedHead returns the lazily-computed serialized header block,
// or nil if it has not been computed yet. Thread-safe.
func (o *Object) LoadSerializedHead() []byte {
	p := o.serializedHead.Load()
	if p == nil {
		return nil
	}
	return *p
}

// StoreSerializedHead atomically stores the serialized header block.
// Called by the fast-path on first cache hit. Thread-safe.
func (o *Object) StoreSerializedHead(head []byte) {
	o.serializedHead.Store(&head)
}

// CloneForReturn returns a shallow copy of o with Body replaced by
// the given body slice. The clone does not share the original's
// atomic.Pointer, avoiding copylocks violations from shallow-copying
// a struct that contains atomic.Pointer.
//
// If the original has a pre-computed serializedHead, it is shared with
// the clone via a new atomic.Pointer so the fast path does not
// re-compute it on every hit.
//
// Used by the hot store slab path in two contexts:
//   - Get (detachBody): heap-copied body, safe from concurrent eviction.
//   - Put (CloneForStorage): slab-backed body, stored in the shard entry
//     without mutating the caller's *Object.
func (o *Object) CloneForReturn(body []byte) *Object {
	clone := &Object{
		Key:                  o.Key,
		VaryKey:              o.VaryKey,
		StatusCode:           o.StatusCode,
		Header:               o.Header,
		Body:                 body,
		BodySize:             o.BodySize,
		StoredAt:             o.StoredAt,
		TTL:                  o.TTL,
		StaleWhileRevalidate: o.StaleWhileRevalidate,
		StaleIfError:         o.StaleIfError,
		ETag:                 o.ETag,
		LastModified:         o.LastModified,
		SurrogateKeys:        o.SurrogateKeys,
		Hits:                 o.Hits,
		CacheControl:         o.CacheControl,
		OriginAge:            o.OriginAge,
		HasConnectionList:    o.HasConnectionList,
		HasNoCacheFields:     o.HasNoCacheFields,
		HasDate:              o.HasDate,
		VaryValue:            o.VaryValue,
		RespNoCache:          o.RespNoCache,
		RespMustRevalidate:   o.RespMustRevalidate,
	}
	if head := o.serializedHead.Load(); head != nil {
		clone.serializedHead.Store(head)
	}
	if v := o.FastHeader.Load(); v != nil {
		clone.FastHeader.Store(v)
	}
	return clone
}

// CloneForRefresh returns a copy of o with the header map deep-cloned
// (so callers can mutate headers without racing other goroutines that hold
// the same stale *Object) and serializedHead left nil so the clone
// lazy-inits its own header block independently.
//
// Body and other slices are shared with o (immutable after construction);
// only Header is deep-copied because MergeHeaders304 writes to it.
// Exists to avoid copylocks violations from value-copying Object, which
// contains an atomic.Pointer.
func (o *Object) CloneForRefresh() *Object {
	return &Object{
		Key:                  o.Key,
		VaryKey:              o.VaryKey,
		StatusCode:           o.StatusCode,
		Header:               o.Header.Clone(),
		Body:                 o.Body,
		BodySize:             o.BodySize,
		StoredAt:             o.StoredAt,
		TTL:                  o.TTL,
		StaleWhileRevalidate: o.StaleWhileRevalidate,
		StaleIfError:         o.StaleIfError,
		ETag:                 o.ETag,
		LastModified:         o.LastModified,
		SurrogateKeys:        o.SurrogateKeys,
		Hits:                 o.Hits,
		CacheControl:         o.CacheControl,
		OriginAge:            o.OriginAge,
		HasConnectionList:    o.HasConnectionList,
		HasNoCacheFields:     o.HasNoCacheFields,
		HasDate:              o.HasDate,
		VaryValue:            o.VaryValue,
		RespNoCache:          o.RespNoCache,
		RespMustRevalidate:   o.RespMustRevalidate,
	}
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
	// CreatedAt is the wall-clock time the ban was created. Objects
	// stored after this time are not subject to the ban.
	CreatedAt time.Time `json:"created_at"`
	// HostRegex matches against the request host.
	HostRegex string `json:"host_regex,omitempty"`
	// PathRegex matches against the request path.
	PathRegex string `json:"path_regex,omitempty"`
	// SurrogateKey matches exactly against stored surrogate keys.
	SurrogateKey string `json:"surrogate_key,omitempty"`
}

// Stats is the runtime snapshot returned by Store.Stats(). Every
// counter is an atomic read.
//
// Unstable.
type Stats struct {
	// HotEntries is the number of objects in the hot tier.
	HotEntries int64 `json:"hot_entries"`
	// HotBytes is an estimated byte footprint of hot-tier objects used
	// for eviction budgeting. It is NOT a runtime memory metric; it
	// cannot see Go allocator size-class rounding, non-cache heap
	// consumers, or GC fragmentation. For actual heap usage, use
	// go_memstats_heap_alloc_bytes (see docs/runbook/40-memory-accounting.md).
	HotBytes int64 `json:"hot_bytes"`
	// WarmEntries is the number of objects in the warm tier.
	WarmEntries int64 `json:"warm_entries"`
	// WarmBytes is the total bytes used by warm-tier segments.
	WarmBytes int64 `json:"warm_bytes"`
	// WarmDiskBytes is the total on-disk size of all warm-tier segment
	// files, including tombstones and superseded entries. Unlike
	// WarmBytes (live record bytes), this reflects actual disk usage
	// and only shrinks after compaction.
	WarmDiskBytes int64 `json:"warm_disk_bytes"`
	// WarmMaxBytes is the configured warm-tier byte budget. 0 means
	// unlimited (no enforcement).
	WarmMaxBytes int64 `json:"warm_max_bytes"`
	// WarmSelfHeals is the number of stale warm-tier index entries
	// dropped by the self-heal path since boot. A non-zero rate
	// indicates segment-management bugs or disk faults.
	WarmSelfHeals int64 `json:"warm_self_heals"`
	// Hits is the total cache hits since boot.
	Hits int64 `json:"hits"`
	// Misses is the total cache misses since boot.
	Misses int64 `json:"misses"`
	// Evictions is the total number of evictions since boot.
	Evictions int64 `json:"evictions"`
}
