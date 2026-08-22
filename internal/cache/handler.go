// Package cache (handler.go) provides the HTTP handler that wires the
// cache engine into the request pipeline. It sits between the pipeline
// router and the origin pool handler:
//
//	client → listener → accesslog → metrics → router → CacheHandler → origin
//
// For every request the handler:
//  1. Computes the cache key.
//  2. Looks up the store.
//  3. Runs Evaluate() to get a Decision.
//  4. On Hit/StaleHit → serve from cache (with Age header).
//  5. On Miss → fetch from origin via the upstream handler, store if
//     cacheable.
//  6. On Revalidate → conditional fetch; on 304, refresh TTL; on 200,
//     replace.
//  7. On Bypass → pass through to upstream.
//  8. On POST/PUT/DELETE → invalidate matching key, pass through.
package cache

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/observability/responsewriter"
	"github.com/bouine-cache/bouine/internal/observability/tracing"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// defaultRefreshConcurrency bounds concurrent background refresh fetches
// per route. Distinct from revalSem (256, per-handler SWR) and fetchSem
// (32, per-handler foreground). Refresh fetches are typically 304s (no
// body), so memory pressure is minimal.
const defaultRefreshConcurrency = 8

// defaultRefreshTimeout bounds a single background refresh fetch. Since
// there is no client request to inherit a context from, this timeout is
// the only protection against a hung origin.
const defaultRefreshTimeout = 10 * time.Second

// minRefreshTTL is the minimum TTL an object must have to be scheduled
// for proactive refresh. Objects with shorter TTLs have a refresh
// window too tight for a network round-trip.
const minRefreshTTL = 5 * time.Second

// refreshGetTimeout bounds the store.Get call in triggerBgRefresh.
// The Get is a freshness check — if the warm tier disk is slow, skip
// the refresh (the object will expire and the client path handles it).
const refreshGetTimeout = 5 * time.Second

// defaultMaxResponseBytes is the hard cap on response body buffering
// when the operator has not configured max_response_bytes. It prevents
// a single oversized origin response from exhausting memory before the
// post-fetch MaxObjectSize check runs. 4 MiB covers the vast majority of
// cacheable web objects (HTML, JSON, CSS, JS, small images). Operators
// caching larger objects should set max_response_bytes explicitly.
const defaultMaxResponseBytes int64 = 4 << 20 // 4 MiB

// defaultFetchConcurrency bounds concurrent foreground origin fetches.
// Each fetch can buffer up to defaultMaxResponseBytes (with 2x
// over-allocation from bytes.Buffer), so this caps worst-case memory
// at concurrency × 2 × maxResponseBytes = 32 × 2 × 4 MiB = 256 MiB.
const defaultFetchConcurrency = 32

// defaultFetchTimeout bounds the total origin fetch time (header + body)
// when the operator has not configured fetch_timeout. This replaces the
// blanket WriteTimeout on the data plane, which was the wrong tool for a
// caching reverse proxy: too short for slow origins, irrelevant for cache
// hits.
const defaultFetchTimeout = 60 * time.Second

// defaultRevalConcurrency bounds concurrent background
// stale-while-revalidate goroutines per Handler. When full, excess
// revalidation attempts are silently dropped — the next client will
// re-trigger if still stale.
const defaultRevalConcurrency = 256

// Pre-allocated header values to avoid per-request slice literals.
var (
	headerHIT         = []string{"HIT"}
	headerMISS        = []string{"MISS"}
	headerSTALE       = []string{"STALE"}
	headerBYPASS      = []string{"BYPASS"}
	headerREVALIDATED = []string{"REVALIDATED"}

	// Pre-allocated X-Cache-Source header values (zero-alloc hit path).
	sourceHot    = []string{string(api.SourceHot)}
	sourceWarm   = []string{string(api.SourceWarm)}
	sourcePeer   = []string{string(api.SourcePeer)}
	sourceOrigin = []string{string(api.SourceOrigin)}
)

// sourceSlice returns the pre-allocated []string for a given Source,
// avoiding per-request slice allocation on the hit path. Returns nil
// (no header) for the empty source.
func sourceSlice(src api.Source) []string {
	switch src {
	case api.SourceHot:
		return sourceHot
	case api.SourceWarm:
		return sourceWarm
	case api.SourcePeer:
		return sourcePeer
	case api.SourceOrigin:
		return sourceOrigin
	default:
		return nil
	}
}

// ageHeaderCache is a pre-allocated table of Age header values for
// 0–599 seconds. Covers fresh objects with TTLs up to 10 minutes and
// avoids strconv.Itoa + []string allocation on every cache hit.
var ageHeaderCache [600][]string

func init() {
	for i := range ageHeaderCache {
		ageHeaderCache[i] = []string{strconv.Itoa(i)}
	}
}

// ageHeader returns a pre-allocated []string for use as an Age header
// value. Falls back to a fresh allocation only for ages ≥ 600s.
func ageHeader(d time.Duration) []string {
	secs := int(d.Seconds())
	if secs >= 0 && secs < len(ageHeaderCache) {
		return ageHeaderCache[secs]
	}
	return []string{strconv.Itoa(secs)}
}

// fetchResult is the outcome of an origin fetch, shared across collapsed requests.
type fetchResult struct {
	StatusCode int
	Header     headerLookup
	Body       []byte
	Err        error
}

// maxRecorderCap bounds the backing array retained by the recorder pool.
// Recorders that grew past this on a large response are discarded so the
// pool never pins a transiently oversized buffer across GC cycles.
const maxRecorderCap = 1 << 20 // 1 MiB

// recorderPool reuses responseRecorder instances on the miss and
// invalidation paths. The miss path transfers ownership of the
// recorder's header map to the fetchResult (the body is copied via
// make+copy so the recorder's buffer is preserved for pool reuse).
// The hit path never allocates a recorder.
var recorderPool = sync.Pool{
	New: func() any {
		return &responseRecorder{
			header: make(http.Header, 8),
			body:   &bytes.Buffer{},
		}
	},
}

func acquireRecorder(maxBytes int64) *responseRecorder {
	rec := recorderPool.Get().(*responseRecorder)
	rec.statusCode = 0
	rec.truncated = false
	rec.maxBytes = maxBytes
	rec.body.Reset()
	for k := range rec.header {
		delete(rec.header, k)
	}
	return rec
}

func releaseRecorder(rec *responseRecorder) {
	if rec.body.Cap() > maxRecorderCap {
		return
	}
	recorderPool.Put(rec)
}

// Handler is the caching HTTP handler. It wraps an upstream
// http.Handler (the origin pool) and a storage.Store.
type Handler struct {
	upstream         http.Handler
	fastClient       FastClient
	store            storage.Store
	flight           singleflight.Group
	logger           observability.Logger
	negativeTTL      time.Duration
	jitterPercent    int
	stayinAlive      bool
	defaultTTL       time.Duration // operator fallback when origin sends no freshness
	overrideTTL      time.Duration // operator override; wins over origin max-age/Expires when > 0
	defaultSWR       time.Duration // operator-level stale-while-revalidate floor
	defaultSIE       time.Duration // operator-level stale-if-error floor
	allowSetCookie   bool          // when false (default), Set-Cookie blocks caching
	maxObjectSize    int64         // skip storage for responses larger than this; 0 = no limit
	maxResponseBytes int64         // hard cap on body buffering; 0 = defaultMaxResponseBytes
	fetchSem         chan struct{} // bounds concurrent foreground origin fetches
	fetchTimeout     time.Duration // bounds total origin fetch time; 0 = defaultFetchTimeout
	policy           *KeyPolicy    // pre-compiled cache key policy (query + Vary headers); nil = none

	// Refresh-before-expiry fields. When refreshBeforeExpiry is true,
	// a background scheduler fires conditional revalidation at
	// TTL - margin, keeping objects perpetually fresh.
	refreshBeforeExpiry  bool
	refreshRegistry      *refreshRegistry
	scheduler            *RefreshScheduler
	refreshSem           chan struct{}
	refreshMargin        time.Duration
	refreshTimeout       time.Duration
	refreshMinHits       int
	refreshPersistCycles int
	refreshMinScore      int64
	refreshLimiter       *refreshRateLimiter
	refreshReactiveFirst bool
	refreshMetrics       observability.RefreshMetricsForRoute
	routeName            string
	done                 chan struct{}
	closeOnce            sync.Once
	refreshWg            sync.WaitGroup
	revalSem             chan struct{}  // bounds concurrent SWR background goroutines
	revalWg              sync.WaitGroup // tracks in-flight SWR goroutines for shutdown
	// variantSets tracks the live variant store keys per primary key to
	// enforce MaxVariants cap. Entries are removed when the handler observes
	// their eviction via store probes on the cap path, on explicit Delete,
	// or when reserveVariantSlot detects the primary key has been evicted
	// by SIEVE and resets the set.
	// Protected by variantMu.
	variantMu   sync.Mutex
	variantSets map[api.Key]map[api.Key]struct{}
	// VaryCapHits is incremented when a variant is rejected; nil-safe.
	VaryCapHits interface{ Inc() }
	// ownerFn returns the peer that owns a cache key and whether the key
	// is local to this node. Nil in single-node mode.
	ownerFn func(key api.Key) (owner api.PeerInfo, isLocal bool)
	// peerFetch asks a peer for a cached object. Returns nil, nil on
	// peer miss; errors fall through to origin. Nil in single-node mode.
	peerFetch func(ctx context.Context, peer api.PeerInfo, key api.Key) (*api.Object, error)
	// peerPut forwards a freshly origin-fetched object to the owner
	// node so subsequent peer-fetches hit. Best-effort, fire-and-forget.
	// Nil in single-node and eventual modes.
	peerPut func(ctx context.Context, owner api.PeerInfo, obj *api.Object)
}

// HandlerConfig configures a cache Handler.
type HandlerConfig struct {
	Upstream http.Handler
	Store    storage.Store
	Logger   observability.Logger
	// NegativeTTL enables caching of 404/405/410/501 responses.
	NegativeTTL time.Duration
	// JitterPercent adds random ±N% to TTLs (0–50). 0 disables.
	JitterPercent int
	// StayinAlive enables emergency stale mode: serve cached objects
	// indefinitely when the upstream is unreachable or returning 5xx.
	StayinAlive bool
	// DefaultTTL is the operator-configured TTL used when the origin sends
	// no explicit freshness (no max-age, no Expires, no Last-Modified).
	// Zero means fall back to heuristic or treat as uncacheable.
	DefaultTTL time.Duration
	// OverrideTTL, when > 0, forces bouine's internal cache TTL to this
	// value regardless of the upstream's Cache-Control/Expires headers.
	// The upstream's response headers are forwarded unaltered; only
	// the storage lifetime seen by bouine's freshness engine changes.
	// RFC 9111 boolean directives (no-store, private, no-cache,
	// must-revalidate) are always honoured; OverrideTTL only replaces
	// the numeric freshness lifetime.
	OverrideTTL time.Duration
	// DefaultSWR is applied to every stored object when the origin does not
	// send stale-while-revalidate. Zero leaves the object at origin semantics.
	DefaultSWR time.Duration
	// DefaultSIE is applied to every stored object when the origin does not
	// send stale-if-error. Zero disables SIE fallback for this route.
	DefaultSIE time.Duration
	// AllowSetCookie controls caching of responses with Set-Cookie.
	// Default (false): Set-Cookie in the response blocks caching
	// unconditionally, matching nginx's safe default and preventing
	// session-cookie replay across users. When true: caching is
	// permitted per RFC 9111, but Set-Cookie is stripped from the
	// stored object so subsequent HITs do not replay another user's
	// cookies.
	AllowSetCookie bool
	// MaxObjectSize, when > 0, skips caching for responses whose body
	// exceeds this size. The response is still proxied to the client.
	// Zero = no limit.
	MaxObjectSize int64
	// MaxResponseBytes is a hard limit on the amount of response body
	// data buffered in memory during an upstream fetch. When exceeded the
	// fetch is aborted and the client receives a 502. This is distinct
	// from MaxObjectSize, which only prevents storage after the body has
	// already been fully buffered. Zero (default) applies a safe built-in
	// limit (4 MiB).
	MaxResponseBytes int64
	// MaxFetchConcurrency bounds the number of concurrent foreground
	// origin fetches. When the limit is reached, additional fetches
	// block until a slot frees or the request context is cancelled.
	// Zero (default) applies a safe built-in limit (32).
	MaxFetchConcurrency int
	// FetchTimeout bounds the total time for an origin fetch (header +
	// body). When exceeded, the fetch is aborted and the caller receives
	// a fetchResult error. Zero (default) applies a safe built-in limit
	// (60s). This replaces the blanket WriteTimeout on the data plane.
	FetchTimeout time.Duration
	// Policy, when non-nil, encodes cache key construction rules for this
	// route: query param stripping/keeping/prefix/empty/dedup and Vary
	// header exclusion. nil means no query/header policy.
	Policy *KeyPolicy
	// VaryCapHits, if non-nil, is incremented when a variant is rejected
	// because MaxVariants is exceeded.
	VaryCapHits interface{ Inc() }
	// OwnerFn, if non-nil, enables cluster-aware routing. It returns the
	// peer that owns a cache key and whether the key is local. When nil,
	// the handler operates in single-node mode: every miss goes to origin.
	OwnerFn func(key api.Key) (owner api.PeerInfo, isLocal bool)
	// PeerFetch, if non-nil, is called on a miss when OwnerFn reports
	// the key is owned by a remote peer. Returns nil, nil on peer miss;
	// errors are treated as misses (origin fallback, logged at debug).
	PeerFetch func(ctx context.Context, peer api.PeerInfo, key api.Key) (*api.Object, error)
	// PeerPut, if non-nil, forwards a freshly origin-fetched object to
	// the key's owner node via a write-to-owner RPC. Used in strong
	// cluster mode so a non-owner that misses both locally and at the
	// owner still delivers the object to the owner for future
	// peer-fetches. Best-effort, fire-and-forget. Nil in single-node
	// and eventual modes.
	PeerPut func(ctx context.Context, owner api.PeerInfo, obj *api.Object)

	// RefreshBeforeExpiry enables proactive background conditional
	// revalidation. A background timer fires at TTL - margin.
	RefreshBeforeExpiry bool
	// RefreshMargin is the duration before TTL expiry at which the
	// background refresh fires.
	RefreshMargin time.Duration
	// RefreshTimeout bounds a single background refresh fetch.
	RefreshTimeout time.Duration
	// RefreshConcurrency bounds concurrent background refresh fetches.
	RefreshConcurrency int
	// RefreshMinHits is the minimum cache hits required during a TTL
	// window for an object to be re-scheduled after a background
	// refresh. Zero disables the gate.
	RefreshMinHits int
	// RefreshPersistCycles is the number of additional TTL cycles to
	// keep refreshing an object after the popularity gate (refresh_min_hits)
	// would block. Each background refresh that finds Hits < minHits
	// decrements the counter. Any popular refresh (Hits >= minHits)
	// resets it. Zero (default) disables persistence — the gate kills
	// re-scheduling immediately. Requires refresh_min_hits > 0 to take
	// effect.
	RefreshPersistCycles int
	// RefreshMinScore is the minimum refresh priority score (staleHits ×
	// BodySize) required for re-scheduling. Zero disables the score gate.
	RefreshMinScore int64
	// RefreshMaxRPS caps background refresh fetches per second per route.
	// Zero means no limit.
	RefreshMaxRPS int
	// RefreshReactiveFirst skips proactive refresh for new objects, relying
	// on SWR to promote popular objects. Requires StaleWhileRevalidate > 0
	// and RefreshMinHits > 0.
	RefreshReactiveFirst bool
	// RouteName labels refresh metrics. Set from the route's config name.
	RouteName string
	// RefreshMetrics records background refresh activity. Nil when the
	// feature is disabled or metrics are not configured.
	RefreshMetrics *observability.RefreshMetrics
	// FastClient, when non-nil, is used by doFetch to fetch from the
	// origin via fasthttp instead of httputil.ReverseProxy. This
	// eliminates the responseRecorder (http.Header map + bytes.Buffer)
	// and the make+copy body clone. When nil, doFetch falls back to
	// the legacy Upstream http.Handler path.
	FastClient FastClient
}

// FastClient performs an origin fetch using fasthttp, returning a
// pooled *fasthttp.Response. The caller is responsible for releasing
// the response via fasthttp.ReleaseResponse. The request is a
// fasthttp.Request (pooled) with method, URI, and headers set by
// the caller. The context is used for timeout/cancellation.
type FastClient interface {
	Do(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response) error
}

// NewHandler creates a caching handler.
func NewHandler(cfg HandlerConfig) *Handler {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	h := &Handler{
		upstream:             cfg.Upstream,
		fastClient:           cfg.FastClient,
		store:                cfg.Store,
		logger:               cfg.Logger,
		negativeTTL:          cfg.NegativeTTL,
		jitterPercent:        cfg.JitterPercent,
		stayinAlive:          cfg.StayinAlive,
		defaultTTL:           cfg.DefaultTTL,
		overrideTTL:          cfg.OverrideTTL,
		defaultSWR:           cfg.DefaultSWR,
		defaultSIE:           cfg.DefaultSIE,
		variantSets:          make(map[api.Key]map[api.Key]struct{}),
		VaryCapHits:          cfg.VaryCapHits,
		ownerFn:              cfg.OwnerFn,
		peerFetch:            cfg.PeerFetch,
		peerPut:              cfg.PeerPut,
		allowSetCookie:       cfg.AllowSetCookie,
		maxObjectSize:        cfg.MaxObjectSize,
		maxResponseBytes:     cfg.MaxResponseBytes,
		policy:               cfg.Policy,
		refreshBeforeExpiry:  cfg.RefreshBeforeExpiry,
		refreshMargin:        cfg.RefreshMargin,
		refreshTimeout:       cfg.RefreshTimeout,
		refreshMinHits:       cfg.RefreshMinHits,
		refreshPersistCycles: cfg.RefreshPersistCycles,
		refreshMinScore:      cfg.RefreshMinScore,
		refreshReactiveFirst: cfg.RefreshReactiveFirst,
		routeName:            cfg.RouteName,
		done:                 make(chan struct{}),
	}
	if h.maxResponseBytes == 0 {
		h.maxResponseBytes = defaultMaxResponseBytes
	}
	conc := cfg.MaxFetchConcurrency
	if conc <= 0 {
		conc = defaultFetchConcurrency
	}
	h.fetchSem = make(chan struct{}, conc)
	h.fetchTimeout = cfg.FetchTimeout
	if h.fetchTimeout <= 0 {
		h.fetchTimeout = defaultFetchTimeout
	}

	// Wire refresh-before-expiry.
	if h.refreshBeforeExpiry {
		if h.refreshTimeout <= 0 {
			h.refreshTimeout = defaultRefreshTimeout
		}
		rc := cfg.RefreshConcurrency
		if rc <= 0 {
			rc = defaultRefreshConcurrency
		}
		h.refreshSem = make(chan struct{}, rc)
		h.refreshRegistry = newRefreshRegistry()
		h.scheduler = NewRefreshScheduler(
			h.triggerBgRefresh,
			h.lookupForRefresh,
		)
		h.scheduler.Start()
		if cfg.RefreshMaxRPS > 0 {
			h.refreshLimiter = newRefreshRateLimiter(cfg.RefreshMaxRPS)
		}
		if cfg.RefreshMetrics != nil && cfg.RouteName != "" {
			h.refreshMetrics = cfg.RefreshMetrics.ForRoute(cfg.RouteName)
		} else {
			h.refreshMetrics = observability.RefreshMetricsForRoute{
				IncTotal:        func(string) {},
				IncErrors:       func(string) {},
				IncSkips:        func(string) {},
				IncInFlight:     func() {},
				DecInFlight:     func() {},
				SetScheduled:    func(float64) {},
				SetRegistrySize: func(float64) {},
			}
		}
	}

	h.revalSem = make(chan struct{}, defaultRevalConcurrency)

	return h
}

// Close drains in-flight background goroutines (both refresh-before-expiry
// and SWR revalidation) and stops the scheduler. Called during engine
// shutdown before store.Close() to prevent use-after-close panics. Safe to
// call multiple times.
func (h *Handler) Close(ctx context.Context) error {
	h.closeOnce.Do(func() {
		close(h.done)
	})
	if h.scheduler != nil {
		h.scheduler.Stop()
	}

	// Drain both refresh-before-expiry and SWR goroutines. A zero-value
	// WaitGroup (when refresh-before-expiry is disabled) returns immediately
	// from Wait(), so this is safe in all configurations.
	done := make(chan struct{})
	go func() {
		h.refreshWg.Wait()
		h.revalWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Purge invalidates a primary cache key and every Vary variant stored under
// it, enforcing RFC 9111 §4.2.4: when a resource is invalidated, all stored
// variants MUST be invalidated too.
//
// Returns owned=true when this handler had the key (tracked variants or a
// store hit). When owned=false, no store mutation was performed.
func (h *Handler) Purge(ctx context.Context, primaryKey api.Key) (bool, error) {
	h.variantMu.Lock()
	set := h.variantSets[primaryKey]
	delete(h.variantSets, primaryKey)
	h.variantMu.Unlock()

	if len(set) == 0 && !h.store.Has(primaryKey) {
		return false, nil
	}

	// Best-effort variant deletes: a missing key is not an error
	// (SIEVE may have evicted it already).
	for vk := range set {
		_ = h.store.Delete(ctx, vk)
		if h.refreshRegistry != nil {
			h.refreshRegistry.Unregister(vk)
		}
	}
	if err := h.store.Delete(ctx, primaryKey); err != nil {
		return true, err
	}
	if h.refreshRegistry != nil {
		h.refreshRegistry.Unregister(primaryKey)
	}
	return true, nil
}

// SoftPurge marks a cached object as stale without deleting it, so the
// next request serves the stale body via stale-while-revalidate (SWR)
// or triggers a conditional revalidation via stale-if-error (SIE),
// depending on which grace window the object has. This is a "refresh"
// or "soft purge" in Varnish terminology. It is distinct from Purge,
// which hard-deletes the object and forces a synchronous origin fetch
// on the next request.
//
// The object's TTL is reduced to zero, making it immediately stale. If
// the object has a non-zero StaleWhileRevalidate window, it remains
// servable while a conditional revalidation fetch refreshes it in the
// background. If the object has only a StaleIfError window, the next
// request attempts a synchronous conditional fetch and falls back to
// the stale body only if the origin errors. If the object has neither
// SWR nor SIE, SoftPurge falls back to a hard delete (equivalent to
// Purge) — there is no graceful degraded mode without a grace window.
//
// Returns (true, nil) when the key was found and soft-purged,
// (false, nil) when the key was not in the store, and (true, err) when
// the store operation failed.
func (h *Handler) SoftPurge(ctx context.Context, primaryKey api.Key) (bool, error) {
	// Collect variant keys for the primary key.
	h.variantMu.Lock()
	set := h.variantSets[primaryKey]
	h.variantMu.Unlock()

	keys := make([]api.Key, 0, len(set)+1)
	keys = append(keys, primaryKey)
	for vk := range set {
		keys = append(keys, vk)
	}

	owned := false
	for _, key := range keys {
		obj, _, err := h.store.Get(ctx, key)
		if err != nil || obj == nil {
			continue
		}
		owned = true

		// If the object has no SWR or SIE window, a soft purge is
		// meaningless — the stale body would never be servable. Fall
		// back to a hard delete so the next request fetches from origin.
		if obj.StaleWhileRevalidate == 0 && obj.StaleIfError == 0 {
			_ = h.store.Delete(ctx, key)
			if h.refreshRegistry != nil {
				h.refreshRegistry.Unregister(key)
			}
			continue
		}

		// Reduce TTL to zero and set StoredAt one second in the past so
		// Fresh(now) is guaranteed false (now is always after StoredAt).
		// The SWR window (StoredAt + 0 + SWR) is measured from the
		// soft-purge time, giving the object a fresh SWR window for the
		// background revalidation to complete.
		softPurged := obj.CloneForRefresh()
		softPurged.StoredAt = time.Now().Add(-1 * time.Second)
		softPurged.TTL = 0
		softPurged.Hits = 0
		if err := h.store.Put(ctx, key, softPurged); err != nil {
			return owned, err
		}
	}

	if !owned && !h.store.Has(primaryKey) {
		return false, nil
	}
	return owned, nil
}

// key is gone or stale. Used by the scheduler's compaction pass.
func (h *Handler) lookupForRefresh(key api.Key) *api.Object {
	ctx, cancel := context.WithTimeout(context.Background(), refreshGetTimeout)
	defer cancel()
	obj, _, err := h.store.Get(ctx, key)
	if err != nil || obj == nil {
		return nil
	}
	if !obj.Fresh(time.Now()) {
		return nil
	}
	return obj
}

// triggerBgRefresh is called by the scheduler when a key's refreshAt
// has elapsed. It checks if the object is still fresh in the store,
// then spawns a background goroutine to perform the conditional fetch.
func (h *Handler) triggerBgRefresh(key api.Key) {
	select {
	case <-h.done:
		return
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), refreshGetTimeout)
	obj, _, err := h.store.Get(ctx, key)
	cancel()
	if err != nil || obj == nil {
		if h.refreshRegistry != nil {
			h.refreshRegistry.Unregister(key)
		}
		h.refreshMetrics.IncSkips("not_found")
		return
	}
	if !obj.Fresh(time.Now()) {
		if h.refreshRegistry != nil {
			h.refreshRegistry.Unregister(key)
		}
		h.refreshMetrics.IncSkips("stale")
		return
	}

	if h.refreshLimiter != nil && !h.refreshLimiter.Allow(time.Now()) {
		delay := time.Duration(100+rand.IntN(400)) * time.Millisecond //nolint:gosec // G404: jitter for deferral, not crypto
		h.scheduler.Schedule(key, time.Now().Add(delay))
		h.refreshMetrics.IncSkips("rate_limited")
		return
	}

	staleHits := h.store.WindowHits(key)

	select {
	case h.refreshSem <- struct{}{}:
	default:
		h.refreshMetrics.IncSkips("semaphore_full")
		return
	}

	h.refreshWg.Add(1)
	h.refreshMetrics.IncInFlight()
	go func() {
		defer func() {
			h.refreshWg.Done()
			<-h.refreshSem
			h.refreshMetrics.DecInFlight()
		}()

		bgCtx, bgCancel := context.WithTimeout(
			context.Background(),
			h.refreshTimeout,
		)
		defer bgCancel()

		// Cancel the refresh if the handler is shutting down so
		// we don't call store.Put on a closed store.
		go func() {
			select {
			case <-h.done:
				bgCancel()
			case <-bgCtx.Done():
			}
		}()

		h.doBackgroundRefresh(bgCtx, key, obj, staleHits)
	}()
}

// doBackgroundRefresh performs a conditional fetch to refresh the
// object before its TTL expires. On 304, the TTL is refreshed in
// place. On 200, the object is replaced. On error, the entry is
// re-scheduled with backoff.
func (h *Handler) doBackgroundRefresh(ctx context.Context, key api.Key, stale *api.Object, staleHits int64) {
	entry := h.refreshRegistry.Lookup(key)
	if entry == nil {
		h.refreshMetrics.IncSkips("not_registered")
		return
	}

	u, err := url.Parse(entry.url)
	if err != nil {
		h.refreshRegistry.Unregister(key)
		h.refreshMetrics.IncSkips("bad_url")
		return
	}

	req := &http.Request{
		Method: entry.method,
		URL:    u,
		Header: entry.header.Clone(),
		Host:   u.Host,
	}
	req = req.WithContext(ctx)
	ConditionalHeaders(req, stale)

	res := h.collapsedFetch(req, key)
	if res.Err != nil {
		h.refreshMetrics.IncTotal("error")
		h.refreshMetrics.IncErrors(errorType(res.Err))
		remaining := time.Until(stale.StoredAt.Add(stale.TTL))
		if remaining <= 0 {
			return
		}
		delay := min(h.refreshMargin, remaining/2)
		if delay < time.Second {
			delay = time.Second
		}
		h.scheduler.Schedule(key, time.Now().Add(delay))
		return
	}

	// Don't store if the handler is shutting down — the store may
	// close before or during the Put.
	if ctx.Err() != nil {
		return
	}

	if res.StatusCode == http.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res, time.Now())
		h.storeObject(ctx, key, refreshed, req, true, staleHits)
		h.refreshMetrics.IncTotal("304")
		return
	}

	if IsCacheableWithDefault(res.StatusCode, req.Header, res.Header.ToHTTP(), h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && res.Header.Get(header.SetCookie) != "" {
			h.refreshMetrics.IncSkips("set_cookie")
			return
		}
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			h.refreshMetrics.IncSkips("too_large")
			return
		}
		obj := buildObject(key, req, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now())
		obj.Hits = 0
		h.storeObject(ctx, key, obj, req, true, staleHits)
		h.refreshMetrics.IncTotal("200")
		return
	}
	h.refreshMetrics.IncSkips("uncacheable")
}

// errorType classifies a background refresh error for the
// bouine_refresh_errors_total metric. Coarse categories are sufficient
// for alerting and dashboards; the full error is logged.
func errorType(err error) string {
	if err == nil {
		return "unknown"
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "timeout") || strings.Contains(s, "deadline"):
		return "timeout"
	case strings.Contains(s, "connection") || strings.Contains(s, "dial") || strings.Contains(s, "EOF"):
		return "connection"
	default:
		return "other"
	}
}

// RefreshStats returns the current scheduler heap size and registry size
// for gauge metrics. Returns zeros when refresh-before-expiry is disabled.
func (h *Handler) RefreshStats() (scheduled, registry int) {
	if h.scheduler != nil {
		scheduled = h.scheduler.Len()
	}
	if h.refreshRegistry != nil {
		registry = h.refreshRegistry.Len()
	}
	return
}

// RouteName returns the route name assigned to this handler, or empty
// when unset. Used by the engine to label refresh gauge metrics.
func (h *Handler) RouteName() string {
	return h.routeName
}

// RefreshEnabled reports whether this handler was configured with
// refresh-before-expiry. Used by the engine to filter handlers for
// shutdown drain and refresh-metric polling without maintaining a
// separate slice.
func (h *Handler) RefreshEnabled() bool {
	return h.refreshBeforeExpiry
}

// StoreFromPeer stores an object received via the write-to-owner RPC and
// schedules refresh-before-expiry. Called by the PeerPutHandler on the
// owner when a non-owner forwards a freshly origin-fetched object (issue #509).
// The object's Key, headers, and TTL are authoritative — the owner does not
// re-evaluate cache headers. A synthetic request is built from the object's
// own headers so the refresh registry has enough context (Vary, conditional
// headers) for future revalidations.
func (h *Handler) StoreFromPeer(ctx context.Context, obj *api.Object) {
	if obj == nil || obj.Key == (api.Key{}) {
		return
	}
	hdr := make(http.Header, obj.Header.Len())
	obj.Header.WriteTo(hdr)
	req := &http.Request{Method: http.MethodGet, Header: hdr}
	h.storeObject(ctx, obj.Key, obj, req, false, 0)
}

// buildKey constructs the cache key, applying the route's KeyPolicy
// when configured. Inlined to avoid overhead on the hit path when no
// policy is configured (zero added allocs).
func (h *Handler) buildKey(r *http.Request) api.Key {
	return BuildKey(r, h.policy)
}

// ServeRequest implements fasthttp.RequestHandler. It creates a
// synchronous shim *http.Request and http.ResponseWriter from the
// *fasthttp.RequestCtx and delegates to ServeHTTP. This eliminates the
// fasthttpadaptor goroutine race (the adaptor runs http.Handler in a
// goroutine for Flush/Hijack; reading ctx.Response afterward races).
//
// The shim is synchronous — no goroutine, no race. The internal methods
// (ServeHTTP, handleCacheMiss, serveObject, etc.) remain unchanged and
// will be migrated to fasthttp types incrementally.
func (h *Handler) ServeRequest(ctx *fasthttp.RequestCtx) {
	// Build an *http.Request from the fasthttp request.
	// Copy body bytes — ctx.Request.Body() references RequestCtx
	// memory that fasthttp resets after ServeRequest returns. The
	// ReverseProxy's http.Transport goroutine may outlive the handler.
	bodyCopy := make([]byte, len(ctx.Request.Body()))
	copy(bodyCopy, ctx.Request.Body())
	r := &http.Request{
		Method: string(ctx.Method()),
		Proto:  "HTTP/1.1",
		Header: make(http.Header),
		Body:   io.NopCloser(bytes.NewReader(bodyCopy)),
	}
	r.URL, _ = url.Parse(string(ctx.RequestURI()))
	r.Host = string(ctx.Host())
	r.RemoteAddr = ctx.RemoteAddr().String()
	if ctx.IsTLS() {
		r.TLS = &tls.ConnectionState{}
	}
	for k, v := range ctx.Request.Header.All() {
		r.Header.Add(string(k), string(v))
	}
	// Retrieve the OTel span context from the tracing middleware.
	// Use context.Background() as the base — NOT the RequestCtx,
	// which fasthttp resets after ServeRequest returns. The
	// ReverseProxy's http.Transport goroutine may outlive the handler
	// and access r.Context(), which must not reference RequestCtx.
	baseCtx := context.Background()
	if v := ctx.UserValue("otel.ctx"); v != nil {
		if sc, ok := v.(context.Context); ok {
			baseCtx = sc
		}
	}
	r = r.WithContext(baseCtx)

	// Create a buffering http.ResponseWriter. The cache handler and
	// httputil.ReverseProxy may spawn goroutines (via http.Transport)
	// that outlive ServeHTTP. If we wrote directly to ctx.Response,
	// fasthttp's RequestCtx reset would race with those goroutines.
	// Instead, buffer the full response and write to ctx.Response
	// synchronously after ServeHTTP returns.
	w := &fasthttpResponseWriter{}

	// Store the cache key on the ctx for the metrics middleware.
	w.onKeySet = func(key api.Key) {
		ctx.SetUserValue("cacheKey", key)
	}

	h.ServeHTTP(w, r)

	// Flush buffered response to ctx.Response synchronously.
	if w.status != 0 {
		ctx.SetStatusCode(w.status)
	} else {
		ctx.SetStatusCode(200)
	}
	// Strip hop-by-hop headers (RFC 9110 §7.6.1) and copy the rest.
	// Also strip Date — fasthttp auto-sets it to the current time
	// (matching net/http behavior), but only if Date is not already
	// present. If we forward the cached object's Date, fasthttp won't
	// overwrite it, causing conformance test failures (date-update
	// tests expect the current Date, not the stored origin Date).
	//
	// Also strip headers listed in the Connection header (RFC 9110
	// §7.6.1: "Connection" header lists per-hop headers to remove).
	connectionHdrs := []string{}
	if connVals := w.hdr[header.Connection]; len(connVals) > 0 {
		for _, cv := range connVals {
			for _, h := range strings.Split(cv, ",") {
				connectionHdrs = append(connectionHdrs, strings.TrimSpace(h))
			}
		}
	}
	connStrip := make(map[string]bool, len(connectionHdrs))
	for _, h := range connectionHdrs {
		connStrip[http.CanonicalHeaderKey(h)] = true
	}
	for k, vals := range w.hdr {
		switch k {
		case header.Connection, header.KeepAlive, header.TE, header.Trailer,
			header.TransferEncoding, header.Upgrade, "Proxy-Connection",
			header.Date:
			continue
		}
		if connStrip[k] {
			continue
		}
		for _, v := range vals {
			ctx.Response.Header.Add(k, v)
		}
	}
	_, _ = ctx.Response.BodyWriter().Write(w.body)
}

// fasthttpResponseWriter is a buffering http.ResponseWriter. It buffers
// the full response (status, headers, body) in memory. The caller
// flushes the buffer to *fasthttp.RequestCtx synchronously after
// ServeHTTP returns, avoiding races with fasthttp's RequestCtx reset
// and with httputil.ReverseProxy's internal goroutines.
type fasthttpResponseWriter struct {
	hdr      http.Header
	status   int
	body     []byte
	written  bool
	onKeySet func(api.Key)
}

func (w *fasthttpResponseWriter) Header() http.Header {
	if w.hdr == nil {
		w.hdr = make(http.Header)
	}
	return w.hdr
}

func (w *fasthttpResponseWriter) WriteHeader(code int) {
	if w.written {
		return
	}
	w.written = true
	w.status = code
}

func (w *fasthttpResponseWriter) Write(b []byte) (int, error) {
	if !w.written {
		w.WriteHeader(200)
	}
	w.body = append(w.body, b...)
	return len(b), nil
}

func (w *fasthttpResponseWriter) Flush() {
	// No-op — the response is flushed to ctx.Response after ServeHTTP.
}

// SetCacheKey stores the cache key for the metrics middleware.
func (w *fasthttpResponseWriter) SetCacheKey(key api.Key) {
	if w.onKeySet != nil {
		w.onKeySet(key)
	}
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isInvalidating(r.Method) {
		h.invalidateAndProxy(w, r)
		return
	}

	// The bouine.pipeline span (created in builder.go) covers the full
	// request lifecycle including the cache engine. A separate bouine.cache
	// child span was removed — it covered ~200ns of work on hits and added
	// a span creation + r.WithContext allocation per request with no
	// additional tracing insight.

	// Take a single timestamp; thread it through Evaluate and serve
	// functions to avoid a second time.Now() syscall per hit.
	// time.Now() is required (not CoarseNow) because Fresh() compares
	// against StoredAt which was set with nanosecond precision via
	// time.Now() at store time. CoarseNow (1ms resolution, truncated)
	// can produce now < StoredAt when both are in the same millisecond,
	// causing a stale object to appear fresh.
	now := time.Now()
	primaryKey, key, obj, src := h.lookup(r)
	if rw, ok := w.(*responsewriter.ResponseWriter); ok {
		rw.SetCacheKey(key)
	}
	disp := Evaluate(r, obj, now)

	switch disp.Decision {
	case Hit, StaleHit:
		if h.tryConditional304(w, r, disp.Object, src) {
			return
		}
		if ServeRange(w, r, disp.Object, disp.Decision == StaleHit, src) {
			return
		}
		cacheRes := cacheHit
		if disp.Decision == StaleHit {
			cacheRes = cacheStale
		}
		h.serveObject(w, r, disp.Object, now, cacheRes, src)
		// In strong mode, only the owner triggers background revalidation.
		// A non-owner may hold a stale copy from before the owner-gated
		// storage deploy (migration window); it serves the stale object
		// (RFC 5861) but must not fire an origin fetch for a key it does
		// not own — the owner is authoritative (issue #509).
		if disp.Decision == StaleHit && disp.Object.StaleWhileRevalidate > 0 && h.isOwnerOrUnmanaged(key) {
			h.triggerBgRevalidate(r, key, disp.Object)
		}
	case Miss:
		h.handleCacheMiss(w, r, primaryKey, key, obj, now, src)
	case Revalidate:
		// A non-owner with a stale object requiring revalidation must not
		// revalidate locally (origin fetch for a key it does not own).
		// Fall through to handleCacheMiss, which peer-fetches from the
		// owner. The stale local copy is left to natural eviction.
		if !h.isOwnerOrUnmanaged(key) {
			h.handleCacheMiss(w, r, primaryKey, key, obj, now, src)
			return
		}
		h.revalidate(w, r, primaryKey, key, disp.Object, now, src)
	case Bypass:
		h.handleBypass(w, r)
	}
}

// handleCacheMiss handles a cache miss: attempts peer-fetch (L5) first, then
// falls back to origin via fetchAndStore or fetchAndStoreStayinAlive.
// Cluster peer-fetch: if this node does not own the key, ask the owner before
// going to origin. The owner has a much higher hit rate for keys it owns
// (consistent hashing concentrates fills there). On a peer hit the object
// is served to the client but NOT stored locally — in strong mode only
// the owner caches keys it owns, so the fleet cache is partitioned (3×
// distinct keyspace) rather than redundant (3× same keys). The owner
// refreshes stale objects; non-owners must not trigger revalidations for
// keys they do not own (issue #509).
// src is the storage-tier source from lookup (hot/warm); it is overridden
// to "peer" on a successful peer hit.
func (h *Handler) handleCacheMiss(w http.ResponseWriter, r *http.Request, primaryKey api.Key, lookupKey api.Key, obj *api.Object, now time.Time, src api.Source) {
	if h.ownerFn != nil && h.peerFetch != nil {
		if owner, isLocal := h.ownerFn(lookupKey); !isLocal {
			if peerObj, err := h.peerFetch(r.Context(), owner, lookupKey); err == nil && peerObj != nil {
				// Re-evaluate: the peer may have returned a stale object.
				if d2 := Evaluate(r, peerObj, now); d2.Decision == Hit || d2.Decision == StaleHit {
					cacheRes := cacheHit
					if d2.Decision == StaleHit {
						cacheRes = cacheStale
					}
					h.serveObject(w, r, peerObj, now, cacheRes, api.SourcePeer)
					// Do not store or revalidate: the object came from the
					// owner. Caching it on a non-owner would make the fleet
					// cache redundant (issue #509). Revalidation is the
					// owner's responsibility.
					return
				}
			} else if err != nil {
				h.logger.Debug("peer fetch error, falling back to origin",
					"peer", owner.Addr, "key", lookupKey, "error", err)
			}
		}
	}
	// If a stale object exists, use fetchAndStoreStayinAlive which will serve
	// the stale copy on 5xx/error — unless the stored response has
	// must-revalidate / proxy-revalidate / no-cache / s-maxage, which require
	// the error to be forwarded to the client.
	//
	// The miss path uses staleFallbackAllowed as a third OR term so that
	// objects without an explicit SIE window still get stale-on-error
	// fallback, matching the revalidate path's behaviour.
	if obj != nil && (h.stayinAlive || obj.StaleForSIE(now) || staleFallbackAllowed(obj)) {
		h.fetchAndStoreStayinAlive(w, r, lookupKey, primaryKey, obj, now, src)
	} else {
		h.fetchAndStore(w, r, lookupKey, primaryKey)
	}
}

// lookup resolves the cache key and stored object for r, accounting
// for Vary-based secondary keys. Returns the source (hot/warm) from
// the storage tier that served the object.
func (h *Handler) lookup(r *http.Request) (primaryKey api.Key, lookupKey api.Key, obj *api.Object, src api.Source) {
	primaryKey = h.buildKey(r)
	obj, src, err := h.store.Get(r.Context(), primaryKey)
	if err != nil {
		h.logger.Warn("cache lookup error", "key", primaryKey, "error", err)
	}
	if obj == nil || obj.Header.Get(header.Vary) == "" {
		return primaryKey, primaryKey, obj, src
	}
	vk := VariantKey(primaryKey, obj.Header.Get(header.Vary), r.Header, h.policy)
	if vk == primaryKey {
		return primaryKey, primaryKey, obj, src
	}
	vobj, vsrc, verr := h.store.Get(r.Context(), vk)
	if verr == nil && vobj != nil {
		return primaryKey, vk, vobj, vsrc
	}
	return primaryKey, vk, nil, ""
}

// isOwnerOrUnmanaged reports whether this node owns key, or whether the
// handler is in single-node / eventual mode (ownerFn is nil). In strong
// mode only the owner revalidates and stores; non-owners defer to the
// owner via peer-fetch (issue #509).
func (h *Handler) isOwnerOrUnmanaged(key api.Key) bool {
	if h.ownerFn == nil {
		return true
	}
	_, isLocal := h.ownerFn(key)
	return isLocal
}

// forwardToOwnerIfRemote forwards obj to its owner via the write-to-owner
// RPC when this node is a non-owner. Used in strong mode so a non-owner
// that fetches from origin (after a peer-fetch miss) delivers the object
// to the owner for future peer-fetches (issue #509). Best-effort,
// fire-and-forget — the caller's response is never blocked on the RPC.
// No-op in single-node / eventual mode (peerPut or ownerFn is nil) and
// when this node is the owner.
func (h *Handler) forwardToOwnerIfRemote(ctx context.Context, obj *api.Object) {
	if h.peerPut == nil || h.ownerFn == nil {
		return
	}
	owner, isLocal := h.ownerFn(obj.Key)
	if isLocal {
		return
	}
	h.peerPut(ctx, owner, obj)
}

// tryConditional304 returns true and writes a 304 if the client's
// conditional headers match the cached object. Used for both hit and
// revalidate paths. src is the storage-tier source (hot/warm).
func (h *Handler) tryConditional304(w http.ResponseWriter, r *http.Request, obj *api.Object, src api.Source) bool {
	if !ClientConditionalMatch(r, obj) {
		return false
	}
	if obj.ETag != "" {
		w.Header()[header.ETag] = []string{obj.ETag}
	}
	// Direct map assignment avoids http.CanonicalMIMEHeaderKey alloc
	// from .Set() on the hit path.
	w.Header()[header.XCache] = headerHIT
	w.Header()[header.XCacheSource] = sourceSlice(src)
	w.WriteHeader(http.StatusNotModified)
	return true
}

// headerGuard wraps an http.ResponseWriter for the BYPASS path, where
// the upstream handler writes directly to the client. It overwrites
// bouine's attribution headers (X-Cache, X-Cache-Source) at WriteHeader
// time so an origin-supplied value cannot spoof the source metric label
// or the X-Cache result. All optional interfaces (Flusher, Hijacker,
// ReaderFrom) are delegated to preserve streaming and zero-copy paths.
type headerGuard struct {
	http.ResponseWriter
	cache   []string // X-Cache value (always set)
	written bool
}

func (g *headerGuard) WriteHeader(code int) {
	if g.written {
		return
	}
	g.written = true
	h := g.Header()
	delete(h, header.XCache)
	delete(h, header.XCacheSource)
	h[header.XCache] = g.cache
	g.ResponseWriter.WriteHeader(code)
}

func (g *headerGuard) Write(b []byte) (int, error) {
	if !g.written {
		g.WriteHeader(http.StatusOK)
	}
	return g.ResponseWriter.Write(b)
}

func (g *headerGuard) Flush() {
	if !g.written {
		g.WriteHeader(http.StatusOK)
	}
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (g *headerGuard) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := g.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, responsewriter.ErrNotSupported
	}
	return h.Hijack()
}

func (g *headerGuard) ReadFrom(src io.Reader) (int64, error) {
	if !g.written {
		g.WriteHeader(http.StatusOK)
	}
	if rf, ok := g.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}
	return io.Copy(struct{ io.Writer }{g}, src)
}

func (h *Handler) handleBypass(w http.ResponseWriter, r *http.Request) {
	// only-if-cached with no cached response → 504 Gateway Timeout
	// (RFC 9111 §5.2.1.7). Source is empty — origin was not contacted.
	reqCC := ParseCacheControl(mergeHeaderValues(r.Header, header.CacheControl))
	if reqCC.OnlyIfCached {
		w.Header()[header.XCache] = headerMISS
		http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		return
	}
	// Fast path: use FastClient when available, bypassing
	// httputil.ReverseProxy and headerGuard.
	if h.fastClient != nil {
		h.handleBypassFast(w, r)
		return
	}
	// Legacy path: wrap the writer so the upstream cannot clobber
	// bouine's X-Cache (BYPASS) or inject an X-Cache-Source.
	guard := &headerGuard{
		ResponseWriter: w,
		cache:          headerBYPASS,
	}
	h.upstream.ServeHTTP(guard, r)
}

// handleBypassFast handles the BYPASS path using FastClient, bypassing
// httputil.ReverseProxy and headerGuard. The response is fetched via
// fasthttp and written directly to the http.ResponseWriter with
// bouine's attribution headers set after the origin response.
func (h *Handler) handleBypassFast(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.StartSpan(r.Context(), "bouine.origin")
	defer span.End()

	fetchCtx, cancel := context.WithTimeout(ctx, h.fetchTimeout)
	defer cancel()

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(r.Method)
	req.SetRequestURI(r.URL.String())
	req.Header.SetHost(r.Host)
	for k, vals := range r.Header {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	tracing.InjectFastHTTP(fetchCtx, req)

	if err := h.fastClient.Do(fetchCtx, req, resp); err != nil {
		tracing.RecordError(span, err)
		w.Header()[header.XCache] = headerBYPASS
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	// Copy origin response headers, then overwrite bouine's attribution.
	dst := w.Header()
	for k, v := range resp.Header.All() {
		ks := string(k)
		if ks == header.XCache || ks == header.XCacheSource {
			continue
		}
		dst.Add(ks, string(v))
	}
	dst[header.XCache] = headerBYPASS
	w.WriteHeader(resp.StatusCode())
	if r.Method != http.MethodHead {
		_, _ = w.Write(resp.Body())
	}
}

// stripNoCacheFields removes headers named in a `no-cache="…"` field list
// from dst before writing it to the client. Per RFC 9111 §5.2.2.4 a cache
// MUST NOT forward these fields without successful revalidation.
// ccHeader is the merged Cache-Control string for the stored response.
func stripNoCacheFields(dst http.Header, ccHeader string) {
	if ccHeader == "" {
		return
	}
	cc := ParseCacheControl(ccHeader)
	if cc.NoCacheFields == "" {
		return
	}
	for _, field := range strings.FieldsFunc(cc.NoCacheFields, func(r rune) bool {
		return r == ',' || r == ' '
	}) {
		if field != "" {
			del := http.CanonicalHeaderKey(field)
			delete(dst, del)
		}
	}
}

// cacheResult selects the X-Cache header and optional Warning for served objects.
type cacheResult int

const (
	cacheHit         cacheResult = iota
	cacheStale                   // SWR / SIE path — adds Warning: 110
	cacheRevalidated             // origin confirmed freshness via 304
)

// serveObject writes a cached object to the client with the appropriate
// X-Cache header and Age. Stale responses also get Warning: 110 per
// RFC 7234 §5.5.3. src is the storage-tier source (hot/warm/peer),
// set as X-Cache-Source via direct map assignment (zero alloc).
func (h *Handler) serveObject(w http.ResponseWriter, r *http.Request, obj *api.Object, now time.Time, result cacheResult, src api.Source) {
	dst := w.Header()
	obj.Header.WriteTo(dst)
	// Strip internal headers used for ban matching — never forwarded to clients.
	dst.Del(header.XBouinePath)
	dst.Del(header.XBouineHost)
	dst[header.Age] = ageHeader(ComputeAge(obj, now))
	dst[header.XCacheSource] = sourceSlice(src)
	switch result {
	case cacheHit:
		dst[header.XCache] = headerHIT
	case cacheStale:
		dst[header.XCache] = headerSTALE
		dst[header.Warning] = []string{`110 - "Response is Stale"`}
	case cacheRevalidated:
		dst[header.XCache] = headerREVALIDATED
	}
	stripNoCacheFields(dst, obj.CacheControl)
	w.WriteHeader(obj.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = w.Write(obj.Body) // #nosec G705 -- obj.Body is a cached origin response, not user input
	}
}

// collapsedFetch deduplicates concurrent origin fetches for the same key.
func (h *Handler) collapsedFetch(r *http.Request, key api.Key) fetchResult {
	v, _, _ := h.flight.Do(key.SingleFlightKey(0), func() (any, error) {
		res := h.doFetch(r)
		return res, nil
	})
	return v.(fetchResult)
}

// revalKeySuffix XORs the key with a constant to produce a singleflight
// key that won't collide with regular fetches for the same cache key,
// while still deduplicating concurrent revalidations for that key.
const revalKeySuffix uint64 = 0x726576616c // "reval" in ASCII

func (h *Handler) collapsedRevalidate(r *http.Request, key api.Key) fetchResult {
	sfKey := key.SingleFlightKey(revalKeySuffix)
	v, _, _ := h.flight.Do(sfKey, func() (any, error) {
		res := h.doFetch(r)
		return res, nil
	})
	return v.(fetchResult)
}

func (h *Handler) fetchAndStore(w http.ResponseWriter, r *http.Request, lookupKey, primaryKey api.Key) {
	res := h.collapsedFetch(r, lookupKey)
	if res.Err != nil {
		w.Header()[header.XCache] = headerMISS
		w.Header()[header.XCacheSource] = sourceOrigin
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	h.writeAndMaybeStore(w, r, res, primaryKey)
}

// fetchAndStoreStayinAlive is like fetchAndStore but falls back to
// serving the super-stale obj if the upstream is unavailable.
// src is the original storage-tier source from lookup (hot/warm),
// threaded to stale-fallback serveObject calls.
// lookupKey is the key under which the stale object was found (may be a
// Vary variant key); it is used for singleflight dedup so that different
// Vary variants do not collapse into a single fetch.
// primaryKey is the canonical key used for Vary variant storage in
// writeAndMaybeStore.
func (h *Handler) fetchAndStoreStayinAlive(w http.ResponseWriter, r *http.Request, lookupKey, primaryKey api.Key, stale *api.Object, now time.Time, src api.Source) {
	res := h.collapsedFetch(r, lookupKey)
	if res.Err != nil {
		h.logger.Info("stayin-alive: upstream unreachable, serving stale indefinitely",
			"error", res.Err, "key", lookupKey)
		h.serveObject(w, r, stale, now, cacheStale, src)
		return
	}
	if res.StatusCode >= 500 {
		h.logger.Info("stayin-alive: upstream 5xx, serving stale indefinitely",
			"status", res.StatusCode, "key", lookupKey)
		h.serveObject(w, r, stale, now, cacheStale, src)
		return
	}
	h.writeAndMaybeStore(w, r, res, primaryKey)
}

// revalidate sends a conditional request to the origin and refreshes the
// stored object. lookupKey is the key under which the stale object was found
// (may be a Vary variant key); it is used for singleflight dedup and for
// storing the 304-refreshed object. primaryKey is the canonical key used for
// Vary variant storage in writeAndMaybeStore.
func (h *Handler) revalidate(w http.ResponseWriter, r *http.Request, primaryKey api.Key, lookupKey api.Key, stale *api.Object, now time.Time, src api.Source) {
	revalReq := r.Clone(r.Context())
	ConditionalHeaders(revalReq, stale)

	// Collapse concurrent revalidations for the same key. Each concurrent
	// request that finds the same stale object would otherwise fire its
	// own conditional origin request. The singleflight key is suffixed
	// with a constant to avoid colliding with regular fetch collapsing
	// while still deduplicating revalidations for the same cache key.
	res := h.collapsedRevalidate(revalReq, lookupKey)

	// stale-on-error gate: both the connection-error path (res.Err) and
	// the 5xx path use the same staleFallbackAllowed check so the policy
	// cannot drift between the two error sites. stayinAlive bypasses the
	// gate — it is an explicit operator override for emergency mode.
	// staleFallbackAllowed returns false for must-revalidate,
	// proxy-revalidate, no-cache, and s-maxage, all of which require a
	// successful revalidation before serving stale (RFC 9111 §5.2.2).
	// The cache-tests conformance suite expects stale serving on origin
	// errors regardless of an explicit SIE window (stale-close,
	// stale-warning-stored), matching Trafficserver and squid.
	if h.stayinAlive && (res.Err != nil || res.StatusCode >= 500) {
		if res.Err != nil {
			h.logger.Info("stayin-alive: upstream unreachable, serving stale indefinitely",
				"error", res.Err, "key", stale.Key)
		} else {
			h.logger.Info("stayin-alive: upstream 5xx, serving stale indefinitely",
				"status", res.StatusCode, "key", stale.Key)
		}
		h.serveObject(w, r, stale, now, cacheStale, src)
		return
	}
	if res.Err != nil {
		if staleFallbackAllowed(stale) {
			h.serveObject(w, r, stale, now, cacheStale, src)
			return
		}
		w.Header()[header.XCache] = headerMISS
		w.Header()[header.XCacheSource] = sourceOrigin
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	if res.StatusCode >= 500 {
		if staleFallbackAllowed(stale) {
			h.serveObject(w, r, stale, now, cacheStale, src)
			return
		}
	}

	if res.StatusCode == http.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res, now)
		h.storeObject(r.Context(), lookupKey, refreshed, r, false, 0)
		h.serveObject(w, r, refreshed, now, cacheRevalidated, src)
		return
	}

	h.writeAndMaybeStore(w, r, res, primaryKey)
}

// refreshFrom304 builds an updated copy of stale after a 304 Not Modified:
// it merges the validating response's headers (RFC 9111 §3.2) and recomputes
// the stored Cache-Control, TTL, OriginAge, and ETag. The caller stores the
// result with its own context. Shared by foreground revalidate and background
// SWR revalidation so the recompute logic lives in exactly one place.
//
// stale.Header is cloned before mutation: it is shared with any other
// goroutine that looked up the same object, and MergeHeaders304's writes would
// race with their reads. Do not remove the Clone.
func (h *Handler) refreshFrom304(stale *api.Object, res fetchResult, now time.Time) *api.Object {
	refreshed := stale.CloneForRefresh()
	refreshed.Header = stale.Header.Clone()
	refreshed.StoredAt = now
	// Reset Hits to 0 for the new TTL window. Object.Hits is a SIEVE
	// eviction signal; the per-window popularity gate uses windowHits
	// from the store, not Object.Hits.
	refreshed.Hits = 0
	MergeHeaders304(refreshed, res.Header.ToHTTP())
	// Recompute CacheControl string and parsed TTL from the updated headers.
	refreshed.CacheControl = refreshed.Header.Get(header.CacheControl)
	if ttl, ok := FreshnessLifetime(ParseCacheControl(refreshed.CacheControl), refreshed.Header.Get); ok {
		refreshed.TTL = ttl
	}
	// Re-apply route override so a 304 cannot revert bouine's storage lifetime
	// back to the upstream's (potentially shorter) max-age.
	if h.overrideTTL > 0 {
		refreshed.TTL = JitterTTL(h.overrideTTL, h.jitterPercent)
	}
	refreshed.OriginAge = parseOriginAge(refreshed.Header)
	if newETag := res.Header.Get(header.ETag); newETag != "" {
		refreshed.ETag = newETag
	}
	return refreshed
}

// triggerBgRevalidate fires a background goroutine that fetches a fresh
// copy of the stale object and updates the store. Called after serving a
// stale-while-revalidate response. revalSem prevents goroutine explosion.
// The goroutine is tracked by revalWg so Close can drain it before
// store.Close, preventing use-after-close panics.
func (h *Handler) triggerBgRevalidate(r *http.Request, key api.Key, stale *api.Object) {
	// Bail out early if the handler is already shutting down.
	select {
	case <-h.done:
		return
	default:
	}

	select {
	case h.revalSem <- struct{}{}:
	default:
		return // semaphore full — next client will retry
	}
	// Detach from the client's context so the background fetch is not
	// cancelled when the response is sent, but wrap it in a cancellable
	// context so Close can signal shutdown.
	bgCtx, bgCancel := context.WithCancel(context.WithoutCancel(r.Context()))
	bgReq := r.Clone(bgCtx)
	h.revalWg.Add(1)
	go func() {
		defer func() {
			h.revalWg.Done()
			<-h.revalSem
		}()
		defer bgCancel()
		// Cancel the background fetch if the handler is shutting down so
		// we don't call store.Put on a closed store.
		go func() {
			select {
			case <-h.done:
				bgCancel()
			case <-bgCtx.Done():
			}
		}()
		h.doBackgroundRevalidate(bgCtx, bgReq, key, stale)
	}()
}

// doBackgroundRevalidate fetches a fresh copy of stale and stores it.
// Uses the collapse group to deduplicate concurrent SWR triggers for
// the same key.
func (h *Handler) doBackgroundRevalidate(ctx context.Context, r *http.Request, key api.Key, stale *api.Object) {
	revalReq := r.Clone(ctx)
	ConditionalHeaders(revalReq, stale)

	staleHits := h.store.WindowHits(key)

	res := h.collapsedFetch(revalReq, key)
	if res.Err != nil {
		return
	}

	if res.StatusCode == http.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res, time.Now())
		h.storeObject(ctx, key, refreshed, r, true, staleHits)
		return
	}

	// Pre-parse Cache-Control/CDN-Cache-Control once instead of up to 6
	// times (IsCacheable parses, isCacheBlocked re-parses for hasCDN,
	// IsCacheableWithDefault re-parses again).
	bgParsed := newParsedResponse(res.StatusCode, r.Header, res.Header.ToHTTP())

	if bgParsed.isCacheableWithDefault(h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && res.Header.Get(header.SetCookie) != "" {
			return
		}
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			return
		}
		obj := buildObject(key, r, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now())
		h.storeObject(ctx, key, obj, r, true, staleHits)
	}
}

func (h *Handler) writeAndMaybeStore(
	w http.ResponseWriter,
	r *http.Request,
	res fetchResult,
	primaryKey api.Key,
) {
	dst := w.Header()
	res.Header.VisitAll(func(k, v string) {
		dst.Add(k, v)
	})
	dst[header.XCache] = headerMISS
	dst[header.XCacheSource] = sourceOrigin
	// A proxy SHOULD add an Age header to responses it forwards,
	// even on first fetch (Age: 0 + any origin Age).
	if res.Header.Get(header.Age) == "" {
		dst[header.Age] = []string{"0"}
	}
	w.WriteHeader(res.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = w.Write(res.Body)
	}

	// Pre-parse Cache-Control/CDN-Cache-Control once instead of up to 6
	// times (IsCacheable parses, isCacheBlocked re-parses for hasCDN,
	// IsCacheableWithDefault re-parses again).
	parsed := newParsedResponse(res.StatusCode, r.Header, res.Header.ToHTTP())

	if parsed.isCacheableWithDefault(h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && res.Header.Get(header.SetCookie) != "" {
			return
		}
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			return
		}
		// primaryKey is passed in from lookup() to avoid a redundant
		// buildKey call on the same request.
		storeKey := primaryKey
		if vary := res.Header.Get(header.Vary); vary != "" {
			storeKey = VariantKey(primaryKey, vary, r.Header, h.policy)
		}
		// Enforce MaxVariants cap: skip storage if this primary key already
		// has MaxVariants distinct Vary variants. RFC 9110 §12.5.5 — unbounded
		// variants are a DoS vector.
		if storeKey != primaryKey {
			if !h.reserveVariantSlot(r.Context(), primaryKey, storeKey) {
				return
			}
		}
		obj := buildObject(storeKey, r, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now())
		h.storeObject(r.Context(), storeKey, obj, r, false, 0)
		// In strong mode, storeObject is a no-op for non-owners. Forward
		// the freshly fetched object to the owner so subsequent peer-fetches
		// from this or other non-owners hit instead of going to origin
		// (issue #509). The owner resolves from obj.Key (storeKey).
		h.forwardToOwnerIfRemote(r.Context(), obj)
		if storeKey != primaryKey {
			// Shallow-copy the object and change only the Key. This avoids
			// a second full buildObject call (~5 allocs: api.Object,
			// header.FromHTTP, serializeHead, parseSurrogateKeys, etc.).
			// The two objects share Header and Body, which are immutable
			// after buildObject. Hits are per-pointer (HotStore.Get
			// increments entry.obj.Hits on the specific stored pointer).
			// CloneForReturn (not a value copy) avoids copylocks: Object
			// embeds atomic.Pointer[[]byte]. serializedHead is nil for
			// both copies today (buildObject never computes it) and
			// lazy-inits independently on each copy's first fast-path hit;
			// if buildObject ever pre-computes it, CloneForReturn's
			// shared-head branch will start earning its keep.
			primaryObj := obj.CloneForReturn(obj.Body)
			primaryObj.Key = primaryKey
			h.storeObject(r.Context(), primaryKey, primaryObj, r, false, 0)
			// Forward the primary (Vary-resolver) entry to its owner too —
			// the primary key may hash to a different owner than the variant.
			h.forwardToOwnerIfRemote(r.Context(), primaryObj)
		}
	}
}

// reserveVariantSlot ensures a slot exists for storeKey in the variant set
// for primaryKey, reconciling evicted entries when the cap is reached.
// Returns false if the cap is still exceeded after reconciliation.
func (h *Handler) reserveVariantSlot(reqCtx context.Context, primaryKey, storeKey api.Key) bool {
	h.variantMu.Lock()
	set := h.variantSets[primaryKey]
	if set == nil {
		set = make(map[api.Key]struct{})
		h.variantSets[primaryKey] = set
	}
	if _, exists := set[storeKey]; exists {
		h.variantMu.Unlock()
		return true
	}
	// If the set exists from a prior incarnation of this primary key that
	// was since evicted by SIEVE or TTL, the tracked variants are stale.
	// Probe the primary key and reset the set if it is gone from the
	// store. Without this, variantSets grows without bound as primary
	// keys are evicted, and stale entries can cause incorrect MaxVariants
	// enforcement.
	if len(set) > 0 {
		h.variantMu.Unlock()
		probeCtx := context.WithoutCancel(reqCtx)
		pkObj, _, _ := h.store.Get(probeCtx, primaryKey)
		h.variantMu.Lock()
		set = h.variantSets[primaryKey]
		if set == nil {
			set = make(map[api.Key]struct{})
			h.variantSets[primaryKey] = set
		}
		if pkObj == nil {
			set = make(map[api.Key]struct{})
			h.variantSets[primaryKey] = set
		}
	}
	if len(set) < MaxVariants {
		set[storeKey] = struct{}{}
		h.variantMu.Unlock()
		return true
	}
	// Cap reached: snapshot keys, release lock, probe store, reacquire.
	keys := make([]api.Key, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	h.variantMu.Unlock()

	probeCtx := context.WithoutCancel(reqCtx)
	dead := make([]api.Key, 0, len(keys))
	for _, k := range keys {
		obj, _, _ := h.store.Get(probeCtx, k)
		if obj == nil {
			dead = append(dead, k)
		}
	}

	h.variantMu.Lock()
	defer h.variantMu.Unlock()
	set = h.variantSets[primaryKey]
	if set == nil {
		set = make(map[api.Key]struct{})
		h.variantSets[primaryKey] = set
	}
	for _, k := range dead {
		delete(set, k)
	}
	if len(set) < MaxVariants {
		set[storeKey] = struct{}{}
		return true
	}
	h.logger.Warn("vary cap exceeded, skipping variant storage",
		"primary_key", primaryKey, "cap", MaxVariants)
	if h.VaryCapHits != nil {
		h.VaryCapHits.Inc()
	}
	return false
}

func (h *Handler) invalidateAndProxy(w http.ResponseWriter, r *http.Request) {
	// Fast path: use FastClient when available.
	if h.fastClient != nil {
		h.invalidateAndProxyFast(w, r)
		return
	}
	h.invalidateAndProxyLegacy(w, r)
}

// invalidateAndProxyFast handles POST/PUT/DELETE using FastClient,
// bypassing responseRecorder and httputil.ReverseProxy.
func (h *Handler) invalidateAndProxyFast(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.StartSpan(r.Context(), "bouine.origin")
	defer span.End()

	fetchCtx, cancel := context.WithTimeout(ctx, h.fetchTimeout)
	defer cancel()

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(r.Method)
	req.SetRequestURI(r.URL.String())
	req.Header.SetHost(r.Host)
	for k, vals := range r.Header {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		req.SetBodyRaw(body)
	}
	tracing.InjectFastHTTP(fetchCtx, req)

	if err := h.fastClient.Do(fetchCtx, req, resp); err != nil {
		tracing.RecordError(span, err)
		w.Header()[header.XCache] = headerMISS
		w.Header()[header.XCacheSource] = sourceOrigin
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	if h.maxResponseBytes > 0 && int64(len(resp.Body())) > h.maxResponseBytes {
		h.logger.Warn("upstream response exceeded max_response_bytes, aborting",
			"key", h.buildKey(r), "limit", h.maxResponseBytes)
		w.Header()[header.XCache] = headerMISS
		w.Header()[header.XCacheSource] = sourceOrigin
		http.Error(w, "upstream response too large", http.StatusBadGateway)
		return
	}

	// Write the captured response to the client.
	dst := w.Header()
	for k, v := range resp.Header.All() {
		ks := string(k)
		if ks == header.XCache || ks == header.XCacheSource {
			continue
		}
		dst.Add(ks, string(v))
	}
	dst[header.XCache] = headerMISS
	dst[header.XCacheSource] = sourceOrigin
	w.WriteHeader(resp.StatusCode())
	_, _ = w.Write(resp.Body())

	// Only invalidate on 2xx/3xx success.
	statusCode := resp.StatusCode()
	if statusCode >= 200 && statusCode < 400 {
		h.invalidateAfterProxyFast(r, resp)
	}
}

// invalidateAfterProxyFast handles cache invalidation and optional
// POST-response storage after a successful fasthttp origin fetch.
func (h *Handler) invalidateAfterProxyFast(r *http.Request, resp *fasthttp.Response) {
	getReq := r.Clone(r.Context())
	getReq.Method = http.MethodGet
	key := h.buildKey(getReq)
	_, _ = h.Purge(r.Context(), key)

	// Evict Content-Location and Location URLs (RFC 9111 §4.4).
	for _, hdr := range []string{header.ContentLocation, header.Location} {
		if loc := string(resp.Header.Peek(hdr)); loc != "" {
			locKey := h.buildLocationKey(r, loc)
			if !locKey.IsZero() {
				_, _ = h.Purge(r.Context(), locKey)
			}
		}
	}

	// RFC 9111 §4.3.1: store POST response if it has explicit
	// freshness and Content-Location matching the request URI.
	h.maybeStorePostResponseFast(r, getReq, key, resp)
}

// maybeStorePostResponseFast stores a successful POST response under
// the GET key when it has explicit freshness and a matching
// Content-Location (RFC 9111 §4.3.1).
func (h *Handler) maybeStorePostResponseFast(r *http.Request, getReq *http.Request, key api.Key, resp *fasthttp.Response) {
	// Check for Set-Cookie — responses with Set-Cookie are not stored.
	if resp.Header.Peek(header.SetCookie) != nil {
		return
	}
	// Check Content-Location matches request URI (RFC 9111 §4.3.1).
	loc := string(resp.Header.Peek(header.ContentLocation))
	if loc == "" {
		return
	}
	// Build fetchResult from the fasthttp response for buildObject.
	body := make([]byte, len(resp.Body()))
	copy(body, resp.Body())
	hdr := make(http.Header, 8)
	for k, v := range resp.Header.All() {
		hdr.Add(string(k), string(v))
	}
	res := fetchResult{
		StatusCode: resp.StatusCode(),
		Header:     fromHTTPHeader(hdr),
		Body:       body,
	}
	now := time.Now()
	obj := buildObject(key, getReq, res, h.negativeTTL, h.defaultTTL, h.overrideTTL,
		h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, now)
	if obj == nil {
		return
	}
	h.storeObject(r.Context(), key, obj, getReq, false, 0)
}

// invalidateAndProxyLegacy is the original responseRecorder-based path.
func (h *Handler) invalidateAndProxyLegacy(w http.ResponseWriter, r *http.Request) {
	// Capture the upstream response first — only invalidate on success
	// (RFC 9111 §4.4: invalidation MUST NOT happen if the response
	// indicates a server error).
	rec := acquireRecorder(h.maxResponseBytes)
	defer releaseRecorder(rec)
	h.upstream.ServeHTTP(rec, r)

	if rec.truncated {
		h.logger.Warn("upstream response exceeded max_response_bytes, aborting",
			"key", h.buildKey(r), "limit", h.maxResponseBytes)
		w.Header()[header.XCache] = headerMISS
		w.Header()[header.XCacheSource] = sourceOrigin
		http.Error(w, "upstream response too large", http.StatusBadGateway)
		return
	}

	// Write the captured response to the client. Overwrite bouine's
	// attribution headers AFTER the copy so an origin-supplied
	// X-Cache-Source cannot spoof the source metric label.
	dst := w.Header()
	for k, vals := range rec.header {
		dst[k] = vals
	}
	dst[header.XCache] = headerMISS
	dst[header.XCacheSource] = sourceOrigin
	w.WriteHeader(rec.statusCode)
	_, _ = w.Write(rec.body.Bytes())

	// Only invalidate on 2xx/3xx success.
	if rec.statusCode >= 200 && rec.statusCode < 400 {
		getReq := r.Clone(r.Context())
		getReq.Method = http.MethodGet
		key := h.buildKey(getReq)
		_, _ = h.Purge(r.Context(), key)

		// Also evict Content-Location and Location URLs (RFC 9111
		// §4.4). These are URI references (RFC 9110 §8.7) and may be
		// absolute, relative, or same-origin bare paths. Use Purge so
		// any Vary variants stored under those URLs are also deleted.
		for _, hdr := range []string{header.ContentLocation, header.Location} {
			if loc := rec.header.Get(hdr); loc != "" {
				locKey := h.buildLocationKey(r, loc)
				if !locKey.IsZero() {
					_, _ = h.Purge(r.Context(), locKey)
				}
			}
		}

		// RFC 9111 §4.3.1: if the POST response has explicit freshness
		// and a Content-Location matching the request URI, store the
		// response under the GET key so subsequent GETs can reuse it.
		h.maybeStorePostResponse(r, getReq, key, rec)
	}
}

// maybeStorePostResponse stores a successful POST response under the GET
// key when it is cacheable, per RFC 9111 §4.3.1.
func (h *Handler) maybeStorePostResponse(r *http.Request, getReq *http.Request, key api.Key, rec *responseRecorder) {
	if r.Method != http.MethodPost || rec.statusCode < 200 || rec.statusCode >= 300 {
		return
	}
	if !IsCacheable(rec.statusCode, r.Header, rec.header) {
		return
	}
	if !h.allowSetCookie && rec.header.Get(header.SetCookie) != "" {
		return
	}
	if h.maxObjectSize > 0 && int64(rec.body.Len()) > h.maxObjectSize {
		return
	}
	bodyCopy := make([]byte, rec.body.Len())
	copy(bodyCopy, rec.body.Bytes())
	res := fetchResult{
		StatusCode: rec.statusCode,
		Header:     fromHTTPHeader(rec.header.Clone()),
		Body:       bodyCopy,
	}
	obj := buildObject(key, getReq, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now())
	h.storeObject(r.Context(), key, obj, getReq, false, 0)
	// Forward to owner if this is a non-owner (issue #509).
	h.forwardToOwnerIfRemote(r.Context(), obj)
}

func (h *Handler) buildLocationKey(r *http.Request, loc string) api.Key {
	ref, err := url.Parse(loc)
	if err != nil {
		return api.Key{}
	}
	resolved := r.URL.ResolveReference(ref)
	scheme := resolved.Scheme
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := resolved.Host
	if host == "" {
		host = r.Host
	}
	var tlsState *tls.ConnectionState
	if scheme == "https" {
		tlsState = &tls.ConnectionState{}
	}
	locReq := &http.Request{
		Method: http.MethodGet,
		URL:    resolved,
		Host:   host,
		TLS:    tlsState,
	}
	return h.buildKey(locReq)
}

// storeObject stores obj under key. When refresh-before-expiry
// is enabled, it also registers the key in the refresh registry and
// schedules a background refresh at TTL - margin. Every path that
// stores a cacheable object must go through this helper so refresh
// scheduling is never skipped.
//
// isRefresh is true when the call originates from a background refresh
// (doBackgroundRefresh or doBackgroundRevalidate). It enables the
// popularity gate: if the object accumulated fewer hits than
// refreshMinHits during its previous TTL window (measured by staleHits
// from the store's per-window counter), it is not re-scheduled and
// expires naturally. Foreground stores pass false to ensure every
// object gets at least one refresh cycle (unless reactive-first is
// enabled).
//
// staleHits is the per-window hit count from the store's WindowHits
// method, read before the refresh store. For non-refresh stores
// (isRefresh=false), staleHits is 0 and unused.
func (h *Handler) storeObject(ctx context.Context, key api.Key, obj *api.Object, r *http.Request, isRefresh bool, staleHits int64) {
	// In strong cluster mode, only the owner stores. A non-owner that
	// fetches from origin (after a peer-fetch miss) serves the response
	// to the client but does not cache it locally — the object is
	// forwarded to the owner via the write-to-owner RPC (peerPut) so
	// subsequent peer-fetches hit. Without this gate every pod caches
	// the same hot keys, the consistent-hash ring is decorative, and
	// peer fetch has 0% hit rate (issue #509).
	if h.ownerFn != nil {
		if _, isLocal := h.ownerFn(key); !isLocal {
			return
		}
	}
	_ = h.store.Put(ctx, key, obj)
	if h.refreshBeforeExpiry && obj.TTL >= minRefreshTTL {
		if IsNegativeCacheable(obj.StatusCode) {
			return
		}
		if !isRefresh && h.refreshReactiveFirst {
			return
		}
		if isRefresh && h.refreshMinHits > 0 && !h.shouldRefresh(staleHits, obj) {
			if h.refreshPersistCycles > 0 && h.refreshRegistry.DecrementPersist(key) {
				h.scheduler.Schedule(key, obj.StoredAt.Add(obj.TTL-h.refreshMargin))
				h.refreshMetrics.IncTotal("persist_cycle")
				return
			}
			if h.refreshRegistry != nil {
				h.refreshRegistry.Unregister(key)
			}
			h.refreshMetrics.IncSkips("below_min_hits")
			return
		}
		var varyHeader string
		if obj.Header.Len() > 0 {
			varyHeader = obj.Header.Get(header.Vary)
		}
		h.refreshRegistry.Register(key, r, varyHeader, h.refreshPersistCycles)
		h.scheduler.Schedule(key, obj.StoredAt.Add(obj.TTL-h.refreshMargin))
	}
}

// shouldRefresh returns true if the object passes both the hit-count and
// score popularity gates. Returns false if either gate fails; the caller
// handles persist-cycle logic.
func (h *Handler) shouldRefresh(staleHits int64, obj *api.Object) bool {
	if staleHits < int64(h.refreshMinHits) {
		return false
	}
	if h.refreshMinScore > 0 && staleHits*obj.BodySize < h.refreshMinScore {
		return false
	}
	return true
}

func (h *Handler) doFetch(r *http.Request) (res fetchResult) {
	// Fast path: use fasthttp.HostClient when available, bypassing
	// responseRecorder + httputil.ReverseProxy. Eliminates:
	// - http.Header map allocation (2 allocs)
	// - bytes.Buffer intermediate copy (1 alloc)
	// - make+copy body clone (1 alloc)
	// - context.WithValue for target selection (1 alloc)
	if h.fastClient != nil {
		return h.doFetchFast(r)
	}
	return h.doFetchLegacy(r)
}

// doFetchFast performs an origin fetch using the fasthttp client,
// bypassing responseRecorder and httputil.ReverseProxy. The response
// is captured directly in a pooled *fasthttp.Response — no http.Header
// map, no bytes.Buffer, no make+copy body clone.
func (h *Handler) doFetchFast(r *http.Request) (res fetchResult) {
	ctx, span := tracing.StartSpan(r.Context(), "bouine.origin")
	defer span.End()

	select {
	case h.fetchSem <- struct{}{}:
		defer func() { <-h.fetchSem }()
	case <-ctx.Done():
		return fetchResult{Err: fmt.Errorf("origin fetch semaphore: %w", ctx.Err())}
	}

	fetchCtx, cancel := context.WithTimeout(ctx, h.fetchTimeout)
	defer cancel()

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	// Populate the request from *http.Request.
	req.Header.SetMethod(r.Method)
	req.SetRequestURI(r.URL.String())
	req.Header.SetHost(r.Host)
	for k, vals := range r.Header {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	// Inject W3C TraceContext.
	tracing.InjectFastHTTP(fetchCtx, req)

	if err := h.fastClient.Do(fetchCtx, req, resp); err != nil {
		tracing.RecordError(span, err)
		return fetchResult{Err: fmt.Errorf("origin fetch: %w", err)}
	}

	// Check for max response bytes.
	if h.maxResponseBytes > 0 && int64(len(resp.Body())) > h.maxResponseBytes {
		return fetchResult{Err: fmt.Errorf("upstream response exceeds %d bytes", h.maxResponseBytes)}
	}

	// Copy body — resp.Body() references the pooled response's internal
	// buffer, which is reused after ReleaseResponse. The body must
	// survive beyond the pool release (it's stored in the cache).
	body := make([]byte, len(resp.Body()))
	copy(body, resp.Body())

	// Copy response headers into a detached fasthttp.ResponseHeader.
	// resp.Header references the pooled response's internal buffer,
	// which is reused after ReleaseResponse. We need the headers to
	// survive beyond the pool release (they're read by buildObject,
	// writeAndMaybeStore, etc. via headerLookup).
	//
	// This avoids creating an http.Header map (2 allocs) — instead
	// we allocate one fasthttp.ResponseHeader (1 alloc) and copy
	// header key-value pairs via SetBytesKV (no string allocation).
	detachedHdr := &fasthttp.ResponseHeader{}
	for k, v := range resp.Header.All() {
		detachedHdr.SetBytesKV(k, v)
	}

	return fetchResult{
		StatusCode: resp.StatusCode(),
		Header:     fromFastHeader(detachedHdr),
		Body:       body,
	}
}

func (h *Handler) doFetchLegacy(r *http.Request) (res fetchResult) {
	// L5 span: upstream origin pool layer.
	ctx, span := tracing.StartSpan(r.Context(), "bouine.origin")
	defer span.End()

	// Recover http.ErrAbortHandler from httputil.ReverseProxy (fires when
	// the origin connection drops mid-stream or the client disconnects).
	// singleflight wraps all panics in *panicError and re-panics, which
	// breaks http.Server's built-in ErrAbortHandler recovery. Convert it
	// to a normal error so singleflight sees a clean return. Real panics
	// (nil dereference, etc.) are re-panicked so bugs remain visible.
	// The partial response captured by the recorder (if any) is
	// intentionally discarded — serving a half-fetched response would
	// corrupt the cache.
	defer func() {
		if rv := recover(); rv != nil {
			if rv == http.ErrAbortHandler {
				err := fmt.Errorf("upstream connection aborted: %w", http.ErrAbortHandler)
				tracing.RecordError(span, err)
				res = fetchResult{Err: err}
				return
			}
			//nolint:forbidigo // re-panic is intentional: real bugs must crash
			panic(rv)
		}
	}()

	select {
	case h.fetchSem <- struct{}{}:
		defer func() { <-h.fetchSem }()
	case <-ctx.Done():
		return fetchResult{Err: fmt.Errorf("origin fetch semaphore: %w", ctx.Err())}
	}
	// Bound the total origin fetch time. This replaces the blanket
	// WriteTimeout on the data plane with a per-fetch deadline that
	// covers both headers and body without affecting cache hits. The
	// timeout starts after the semaphore acquire so queueing time does
	// not eat into the origin fetch budget.
	fetchCtx, cancel := context.WithTimeout(ctx, h.fetchTimeout)
	defer cancel()
	// Propagate W3C TraceContext into the upstream request so the origin
	// can participate in the distributed trace.
	outReq := r.WithContext(fetchCtx)
	tracing.InjectHTTP(fetchCtx, outReq)
	rec := acquireRecorder(h.maxResponseBytes)
	defer releaseRecorder(rec)
	h.upstream.ServeHTTP(rec, outReq)

	// Check the fetch context after ServeHTTP returns. On timeout
	// (DeadlineExceeded), discard whatever the recorder captured — the
	// fetch exceeded its budget, and the recorder may contain an empty
	// response or a synthetic 502 from the ReverseProxy's ErrorHandler.
	// On client disconnect (Canceled), discard only if the recorder is
	// empty (statusCode == 0). If the origin returned a complete response
	// before the cancellation propagated, keep it — the next client
	// should benefit from the cached object.
	if err := fetchCtx.Err(); err != nil {
		if err == context.DeadlineExceeded || rec.statusCode == 0 {
			tracing.RecordError(span, err)
			return fetchResult{Err: fmt.Errorf("origin fetch: %w", err)}
		}
	}

	if rec.truncated {
		return fetchResult{Err: fmt.Errorf("upstream response exceeds %d bytes", h.maxResponseBytes)}
	}

	// Transfer ownership of the recorder's header map to the fetchResult,
	// then give the recorder a fresh header map before it is returned to
	// the pool. This eliminates the header.Clone() that the previous
	// defensive copy required (Clone allocates 1 map + N value slices).
	// Savings scale with response header count; measured on a 6-header
	// benchmark: 1 full-path alloc saved.
	//
	// The body is copied (make+copy) rather than transferred because
	// bytes.Buffer doubles its backing array, so the buffer's cap can be
	// up to 2x the content length. Transferring the slice directly (even
	// with slices.Clip) would retain the oversized backing array for the
	// lifetime of the cached object — up to 2x memory waste on large
	// responses. The make+copy allocates exactly len(body) bytes.
	//
	// The body buffer itself is NOT replaced — the pool reuses its
	// capacity across fetches. acquireRecorder calls rec.body.Reset()
	// which preserves cap, so subsequent upstream writes reuse the
	// existing backing array without re-growing. Replacing the buffer
	// here would force a new backing-array allocation on every fetch,
	// defeating the pool.
	//
	// Safety: all singleflight waiters read res.Header and res.Body without
	// mutating. The recorder gets a fresh header map so the pool reuse is
	// safe. The body buffer is Reset()ed on next acquire, clearing stale
	// content while preserving capacity.
	body := make([]byte, rec.body.Len())
	copy(body, rec.body.Bytes())
	hdr := rec.header
	rec.header = make(http.Header, 8)

	return fetchResult{
		StatusCode: rec.statusCode,
		Header:     fromHTTPHeader(hdr),
		Body:       body,
	}
}

//nolint:gocyclo // 16: TTL/freshness conditionals are inherently branchy
func buildObject(key api.Key, r *http.Request, res fetchResult, negativeTTL, defaultTTL, overrideTTL, defaultSWR, defaultSIE time.Duration, jitterPct int, policy *KeyPolicy, now time.Time) *api.Object {
	// Parse Cache-Control (may be multiple headers — merge first).
	// CDN-Cache-Control overrides Cache-Control for shared caches (RFC 9211):
	// use it as the authoritative directive source when present.
	ccHeader := mergeHeaderValues(res.Header.ToHTTP(), header.CacheControl)
	var respCC Directives
	if cdnCC, hasCDN := cdnCacheControl(res.Header.ToHTTP()); hasCDN {
		respCC = cdnCC
		// Store CDN-Cache-Control string as the object's pre-parsed CC so
		// Evaluate reads the CDN directives on every hit path.
		ccHeader = mergeHeaderValues(res.Header.ToHTTP(), "CDN-Cache-Control")
	} else {
		respCC = ParseCacheControl(ccHeader)
	}
	originAge := parseOriginAge(res.Header)
	// RFC 9111 §4.2.3: corrected_initial_age = max(apparent_age, age_value).
	// Apparent age is derived from the Date header: max(0, now - Date).
	if dateStr := res.Header.Get(header.Date); dateStr != "" {
		if dt := parseHTTPDate(dateStr); !dt.IsZero() && !dt.After(now) {
			if apparentAge := now.Sub(dt); apparentAge > originAge {
				originAge = apparentAge
			}
		}
	}
	// computeTTL consolidates heuristic, fallback, negative, jitter, and Age subtraction.
	ttl := computeTTL(res.Header.ToHTTP(), res.StatusCode, respCC, negativeTTL, defaultTTL, jitterPct, originAge, now)
	// Route-level override wins over the upstream's freshness directives.
	// Applied after computeTTL so jitter is seeded from the override value,
	// not the origin's max-age. The stored object retains the unaltered
	// upstream Cache-Control header, which is forwarded to downstream clients.
	if overrideTTL > 0 {
		ttl = JitterTTL(overrideTTL, jitterPct)
	}

	obj := &api.Object{
		Key:          key,
		StatusCode:   res.StatusCode,
		Header:       header.FromHTTP(res.Header.ToHTTP()),
		Body:         res.Body,
		BodySize:     int64(len(res.Body)),
		StoredAt:     now,
		TTL:          ttl,
		ETag:         res.Header.Get(header.ETag),
		CacheControl: ccHeader,  // Lead 1: pre-stored, avoids re-parsing on every hit
		OriginAge:    originAge, // Lead 3: pre-stored, avoids re-parsing on the read path
	}
	// Stamp internal headers for ban predicate matching. These are
	// stripped before serving to clients (see serveObject).
	obj.Header.Set(header.XBouinePath, r.URL.Path)
	obj.Header.Set(header.XBouineHost, r.Host)
	// Set-Cookie is always stripped from cached objects: joining multiple
	// Set-Cookie values with ", " is non-conformant per RFC 9110 §5.2,
	// and serving stale cookies to a different client is a security risk.
	// AllowSetCookie controls whether responses with Set-Cookie are cached
	// at all (gated above); it does not preserve Set-Cookie in the cache.
	obj.Header.Del(header.SetCookie)
	// Ensure Content-Length is set so cache hits don't fall back to chunked
	// transfer encoding. Origins that use chunked encoding have their
	// Transfer-Encoding stripped by Go's HTTP client, leaving no
	// Content-Length on the stored response. Varnish computes this from
	// the body; we do the same at store time.
	if obj.Header.Get(header.ContentLength) == "" && obj.BodySize > 0 &&
		obj.StatusCode != http.StatusNotModified {
		obj.Header.Set(header.ContentLength, strconv.FormatInt(obj.BodySize, 10))
	}
	if respCC.StaleWhileRevalidSet {
		obj.StaleWhileRevalidate = respCC.StaleWhileRevalid
	} else if defaultSWR > 0 {
		obj.StaleWhileRevalidate = defaultSWR
	}
	if respCC.StaleIfErrorSet {
		obj.StaleIfError = respCC.StaleIfError
	} else if defaultSIE > 0 {
		obj.StaleIfError = defaultSIE
	}
	if lm := res.Header.Get(header.LastModified); lm != "" {
		if t, err := time.Parse(http.TimeFormat, lm); err == nil {
			obj.LastModified = t
		}
	}
	obj.VaryKey = BuildVaryKey(res.Header.Get(header.Vary), r.Header, policy)

	obj.SurrogateKeys = parseSurrogateKeys(res.Header.ToHTTP())

	// SerializedHead is not computed here — it is lazily computed on
	// the first fast-path cache hit by getOrComputeSerializedHead.
	// This avoids allocating ~512 bytes per object for objects that
	// are never served via the fast-path (misses, net/http path).

	return obj
}

// serializeHead pre-renders the HTTP response header block (static
// headers as "Key: Value\r\n" pairs, without status line or trailing
// \r\n) at cache-fill time. The H1 fast-path uses this to write headers
// directly via net.Buffers without iterating the header map per request.
//
// Excludes dynamic headers (Age, X-Cache, X-Cache-Source, Warning), internal
// headers (X-Bouine-*), and no-cache fields stripped per RFC 9111 §5.2.2.4.
func serializeHead(obj *api.Object) []byte {
	noCacheFields := parseNoCacheFieldNames(obj.CacheControl)
	buf := make([]byte, 0, 512)
	n := obj.Header.Len()
	for i := 0; i < n; i++ {
		key, value := obj.Header.At(i)
		if skipStaticHeader(key, noCacheFields) {
			continue
		}
		buf = append(buf, key...)
		buf = append(buf, ": "...)
		buf = append(buf, value...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

// computeTTL derives the freshness lifetime for a response, applying
// explicit freshness, heuristic TTL, operator defaults, jitter, and
// the origin Age adjustment.
func computeTTL(header http.Header, status int, respCC Directives,
	negativeTTL, defaultTTL time.Duration, jitterPct int,
	originAge time.Duration, now time.Time) time.Duration {
	ttl, explicit := FreshnessLifetimeH(respCC, header)
	if !explicit {
		ttl = HeuristicTTL(header, now)
	}
	if !explicit && ttl == 0 && defaultTTL > 0 {
		ttl = defaultTTL
	} else if !explicit && ttl == 0 && negativeTTL > 0 && IsNegativeCacheable(status) {
		ttl = negativeTTL
	}
	ttl = JitterTTL(ttl, jitterPct)
	ttl -= originAge
	if ttl < 0 {
		ttl = 0
	}
	return ttl
}

// parseSurrogateKeys extracts surrogate/cache-tag keys from standard and
// dialect response headers (Fastly: Surrogate-Key, Cloudflare: Cache-Tag,
// Varnish: X-Cache-Tags). The first non-empty header wins; tokens are
// whitespace/comma-separated and de-duplicated.
func parseSurrogateKeys(h http.Header) []string {
	for _, hdr := range header.SurrogateKeyHeaders {
		v := h.Get(hdr)
		if v == "" {
			continue
		}
		// Use a stack-allocated array for dedup instead of a map allocation.
		// Operators typically tag with 0-5 surrogate keys; >16 is extremely
		// rare. If exceeded, dedup stops but all tags are still returned
		// (potential duplicates in the output for the overflow case).
		var seen [16]string
		var n int
		var keys []string
		for _, tag := range strings.Fields(strings.ReplaceAll(v, ",", " ")) {
			tag = strings.TrimSpace(tag)
			if tag == "" {
				continue
			}
			if n < len(seen) {
				if slices.Contains(seen[:n], tag) {
					continue
				}
				seen[n] = tag
				n++
			}
			keys = append(keys, tag)
		}
		return keys
	}
	return nil
}

// isInvalidating returns true for unsafe methods that should trigger
// staleFallbackAllowed reports whether a stale object may be served as a
// fallback when the upstream returns a 5xx or connection error. It returns
// false when the stored response has must-revalidate, proxy-revalidate,
// no-cache, or s-maxage (all of which require a successful revalidation).
func staleFallbackAllowed(obj *api.Object) bool {
	// Empty Cache-Control parses to the zero Directives (all false), so a
	// response with no directives is allowed — no special case needed.
	cc := objDirectives(obj)
	return !cc.MustRevalidate && !cc.ProxyRevalidate && !cc.NoCache && !cc.SMaxAgeSet
}

// isInvalidating returns true for unsafe methods that should trigger
// cache invalidation (RFC 9111 §4.4). GET, HEAD, and OPTIONS are safe.
func isInvalidating(method string) bool {
	return method != http.MethodGet &&
		method != http.MethodHead &&
		method != "OPTIONS"
}

// responseRecorder captures the upstream response in memory so we can
// both serve it to the client and store it in the cache. For bodies
// larger than 64 KiB, streaming (phase 5+) will replace this.
type responseRecorder struct {
	header     http.Header
	body       *bytes.Buffer
	statusCode int
	maxBytes   int64
	truncated  bool
}

func (r *responseRecorder) Header() http.Header { return r.header }

func (r *responseRecorder) WriteHeader(code int) {
	r.statusCode = code
	// Pre-size the buffer when Content-Length is known, avoiding
	// bytes.Buffer doubling over-allocation that causes transient heap
	// spikes under concurrent miss traffic (issue #141). Only grows
	// if the pooled buffer is too small.
	if r.maxBytes > 0 {
		if cl := r.header.Get("Content-Length"); cl != "" {
			if n, err := strconv.ParseInt(cl, 10, 64); err == nil && n > 0 && n <= r.maxBytes {
				if int64(r.body.Cap()) < n {
					r.body.Grow(int(n))
				}
			}
		}
	}
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if r.statusCode == 0 {
		r.statusCode = 200
	}
	if r.maxBytes > 0 && int64(r.body.Len())+int64(len(b)) > r.maxBytes {
		r.truncated = true
		return 0, fmt.Errorf("response body exceeds %d bytes", r.maxBytes)
	}
	return r.body.Write(b)
}

// Flush implements http.Flusher for streaming compatibility.
func (r *responseRecorder) Flush() {}

// ensure interface compliance.
var _ http.ResponseWriter = (*responseRecorder)(nil)
var _ http.Flusher = (*responseRecorder)(nil)
