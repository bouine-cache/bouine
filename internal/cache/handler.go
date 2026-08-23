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
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"bytes"

	"golang.org/x/sync/singleflight"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/observability/tracing"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// ErrAbortHandler is the fasthttp equivalent of net/http.ErrAbortHandler.
// An upstream handler may panic with this value to signal that the
// response should be aborted. doFetch/doFetchBg recover it and convert
// it to a fetchResult error instead of letting the panic propagate
// through singleflight.
var ErrAbortHandler = errors.New("abort handler")

// headerFromCtx extracts request headers as header.Map from a fasthttp RequestCtx.
func headerFromCtx(ctx *fasthttp.RequestCtx) header.Map {
	hm := header.NewMap(ctx.Request.Header.Len())
	for k, v := range ctx.Request.Header.All() {
		hm.AppendEntryCanonical(string(k), string(v))
	}
	hm.SortEntries()
	return hm
}

// requestInfoFromCtx constructs a RequestInfo from a fasthttp RequestCtx.
// This allocates a header.Map from the request headers — use on the
// miss/revalidate paths where the full header map is needed.
// RemoteAddr is intentionally left empty: it is never read inside the
// cache layer (access logging reads ctx.RemoteAddr() directly), and
// ctx.RemoteAddr().String() allocates via net.JoinHostPort on every call.
func requestInfoFromCtx(ctx *fasthttp.RequestCtx) RequestInfo {
	return RequestInfo{
		Method: string(ctx.Method()),
		URI:    string(ctx.RequestURI()),
		Host:   string(ctx.Host()),
		Path:   string(ctx.Path()),
		TLS:    ctx.IsTLS(),
		Header: headerFromCtx(ctx),
	}
}

// ctxResponseWriter adapts *fasthttp.RequestCtx to the rangeWriter interface.
type ctxResponseWriter struct {
	ctx *fasthttp.RequestCtx
}

func (ctx *ctxResponseWriter) SetHeader(key, value string) {
	ctx.ctx.Response.Header.Set(key, value)
}

func (ctx *ctxResponseWriter) WriteHeader(code int) {
	ctx.ctx.SetStatusCode(code)
}

func (ctx *ctxResponseWriter) Write(b []byte) (int, error) {
	return ctx.ctx.Write(b)
}

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

// ageHeader returns the Age header value string for a duration.
func ageHeader(d time.Duration) string {
	return strconv.Itoa(int(d.Seconds()))
}

// fetchResult is the outcome of an origin fetch, shared across collapsed requests.
type fetchResult struct {
	StatusCode int
	Header     headerLookup
	Body       []byte
	Err        error
	fastResp   *fasthttp.Response // non-nil when the response is kept alive for CopyTo
}

// Handler is the caching HTTP handler. It wraps an upstream
// fasthttp.RequestHandler (the origin pool) and a storage.Store.
//
// maxRecorderCap bounds the backing array retained by the recorder pool.
// Recorders that grew past this on a large response are discarded so the
// pool never pins a transiently oversized buffer across GC cycles.
type Handler struct {
	upstream         fasthttp.RequestHandler
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
	Upstream fasthttp.RequestHandler
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
	// FastClient is used by doFetch to fetch from the origin via
	// fasthttp. The response is captured directly in a pooled
	// *fasthttp.Response, eliminating intermediate header.Map and
	// body buffer allocations.
	FastClient FastClient
}

// FastClient performs an origin fetch using fasthttp, returning a
// pooled *fasthttp.Response. The caller is responsible for releasing
// the response via fasthttp.ReleaseResponse. The request is a
// fasthttp.RequestCtx (pooled) with method, URI, and headers set by
// the caller. The context is used for timeout/cancellation.
type FastClient interface {
	Do(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response) error
}

// NewHandler creates a caching handler.
//
//nolint:funlen // 81 lines: initialization is inherently sequential
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

	ri := RequestInfo{
		Method: entry.method,
		URI:    entry.url,
		Host:   u.Host,
		Path:   u.Path,
		TLS:    u.Scheme == "https",
		Header: entry.header.Clone(),
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(ri.Method)
	req.SetRequestURI(ri.URI)
	req.Header.SetHost(ri.Host)
	ri.Header.Range(func(k, v string) bool {
		req.Header.Set(k, v)
		return true
	})
	setConditionalHeaders(func(k, v string) { req.Header.Set(k, v) }, stale)

	res := h.collapsedFetchBg(ctx, req, key)
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

	if res.StatusCode == fasthttp.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res, time.Now())
		h.storeObject(ctx, key, refreshed, ri, true, staleHits)
		h.refreshMetrics.IncTotal("304")
		return
	}

	resMap := res.Header.ToMap()
	if IsCacheableWithDefault(res.StatusCode, ri.Header, resMap, h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && resMap.Get(header.SetCookie) != "" {
			h.refreshMetrics.IncSkips("set_cookie")
			return
		}
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			h.refreshMetrics.IncSkips("too_large")
			return
		}
		obj := buildObject(key, ri, res, resMap, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now())
		obj.Hits = 0
		h.storeObject(ctx, key, obj, ri, true, staleHits)
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
	ri := RequestInfo{Method: "GET", Header: obj.Header.Clone()}
	h.storeObject(ctx, obj.Key, obj, ri, false, 0)
}

// buildKey constructs the cache key, applying the route's KeyPolicy
// when configured. Reads directly from *fasthttp.RequestCtx byte slices
// to avoid string() conversions — zero-alloc when URL has no query string.
func (h *Handler) buildKey(ctx *fasthttp.RequestCtx) api.Key {
	return BuildKeyFast(ctx.Method(), ctx.RequestURI(), ctx.Host(), ctx.Path(), ctx.IsTLS(), h.policy)
}

// ServeRequest implements fasthttp.RequestHandler. It dispatches
// cache-invalidating methods (POST/PUT/DELETE) to invalidateAndProxy
// and all others to the cache lookup pipeline.
func (h *Handler) ServeRequest(ctx *fasthttp.RequestCtx) {
	if isInvalidating(string(ctx.Method())) {
		h.invalidateAndProxy(ctx)
		return
	}

	now := time.Now()
	primaryKey, key, obj, src := h.lookup(ctx)

	// Fast path: evaluate using direct Peek calls on the request headers,
	// avoiding the headerFromCtx allocation (which builds a header.Map
	// with string conversions for every header on every request).
	// Only construct a full RequestInfo (with header.Map) if we need to
	// leave the hit path (miss, revalidate, bypass, range).
	// SetUserValue is deferred to miss/revalidate/bypass paths to avoid
	// the api.Key→any boxing allocation (1 alloc) on the hit path.
	// Access logging handles a missing cacheKey gracefully (zero-value key).
	disp := evaluateFast(ctx, obj, now)

	switch disp.Decision {
	case Hit, StaleHit:
		if tryConditional304Fast(ctx, disp.Object, src) {
			return
		}
		// Range requests need the full RequestInfo for ServeRange.
		if hasRangeHeader(ctx) {
			ri := requestInfoFromCtx(ctx)
			if ServeRange(&ctxResponseWriter{ctx}, ri, disp.Object, disp.Decision == StaleHit, src) {
				return
			}
		}
		cacheRes := cacheHit
		if disp.Decision == StaleHit {
			cacheRes = cacheStale
		}
		h.serveObject(ctx, disp.Object, now, cacheRes, src)
		if disp.Decision == StaleHit && disp.Object.StaleWhileRevalidate > 0 && h.isOwnerOrUnmanaged(key) {
			ri := requestInfoFromCtx(ctx)
			h.triggerBgRevalidate(ctx, ri, key, disp.Object)
		}
	case Miss:
		ctx.SetUserValue("cacheKey", key)
		ri := requestInfoFromCtx(ctx)
		h.handleCacheMiss(ctx, primaryKey, key, obj, now, src, ri)
	case Revalidate:
		ctx.SetUserValue("cacheKey", key)
		if !h.isOwnerOrUnmanaged(key) {
			ri := requestInfoFromCtx(ctx)
			h.handleCacheMiss(ctx, primaryKey, key, obj, now, src, ri)
			return
		}
		ri := requestInfoFromCtx(ctx)
		h.revalidate(ctx, primaryKey, key, disp.Object, now, src, ri)
	case Bypass:
		ctx.SetUserValue("cacheKey", key)
		h.handleBypass(ctx)
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
func (h *Handler) handleCacheMiss(ctx *fasthttp.RequestCtx, primaryKey api.Key, lookupKey api.Key, obj *api.Object, now time.Time, src api.Source, ri RequestInfo) {
	if h.ownerFn != nil && h.peerFetch != nil {
		if owner, isLocal := h.ownerFn(lookupKey); !isLocal {
			if peerObj, err := h.peerFetch(ctx, owner, lookupKey); err == nil && peerObj != nil {
				// Re-evaluate: the peer may have returned a stale object.
				if d2 := Evaluate(ri, peerObj, now); d2.Decision == Hit || d2.Decision == StaleHit {
					cacheRes := cacheHit
					if d2.Decision == StaleHit {
						cacheRes = cacheStale
					}
					h.serveObject(ctx, peerObj, now, cacheRes, api.SourcePeer)
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
		h.fetchAndStoreStayinAlive(ctx, lookupKey, primaryKey, obj, now, src, ri)
	} else {
		h.fetchAndStore(ctx, lookupKey, primaryKey, ri)
	}
}

// lookup resolves the cache key and stored object for r, accounting
// for Vary-based secondary keys. Returns the source (hot/warm) from
// the storage tier that served the object.
func (h *Handler) lookup(ctx *fasthttp.RequestCtx) (primaryKey api.Key, lookupKey api.Key, obj *api.Object, src api.Source) {
	primaryKey = h.buildKey(ctx)
	obj, src, err := h.store.Get(ctx, primaryKey)
	if err != nil {
		h.logger.Warn("cache lookup error", "key", primaryKey, "error", err)
	}
	if obj == nil || obj.Header.Get(header.Vary) == "" {
		return primaryKey, primaryKey, obj, src
	}
	vk := VariantKey(primaryKey, obj.Header.Get(header.Vary), headerFromCtx(ctx), h.policy)
	if vk == primaryKey {
		return primaryKey, primaryKey, obj, src
	}
	vobj, vsrc, verr := h.store.Get(ctx, vk)
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
func (h *Handler) tryConditional304(ctx *fasthttp.RequestCtx, obj *api.Object, src api.Source) bool {
	ri := requestInfoFromCtx(ctx)
	if !ClientConditionalMatch(ri, obj) {
		return false
	}
	if obj.ETag != "" {
		ctx.Response.Header.Set(header.ETag, obj.ETag)
	}
	// Direct map assignment avoids http.CanonicalMIMEHeaderKey alloc
	// from .Set() on the hit path.
	ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("HIT"))
	ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(src)))
	ctx.SetStatusCode(fasthttp.StatusNotModified)
	return true
}

// handleBypass handles the BYPASS path (requests with no-store or
// no-cache directives). It delegates to handleBypassFast, which fetches
// the origin response via FastClient and writes it directly to the
// *fasthttp.RequestCtx, then overwrites bouine's attribution headers
// (X-Cache, X-Cache-Source) so an origin-supplied value cannot spoof
// the source metric label or X-Cache result.
func (h *Handler) handleBypass(ctx *fasthttp.RequestCtx) {
	reqCC := ParseCacheControl(string(ctx.Request.Header.Peek(header.CacheControl)))
	if reqCC.OnlyIfCached {
		ctx.Error("Gateway Timeout", fasthttp.StatusGatewayTimeout)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		return
	}
	h.handleBypassFast(ctx)
}

func (h *Handler) handleBypassFast(ctx *fasthttp.RequestCtx) {
	if h.fastClient == nil {
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("BYPASS"))
		ctx.Error("upstream error: no fast client configured", fasthttp.StatusBadGateway)
		return
	}
	bgCtx := context.Background()
	fetchCtx, span := tracing.StartSpan(bgCtx, "bouine.origin")
	defer span.End()

	// Use context.WithCancel + time.AfterFunc instead of context.WithTimeout
	// to avoid the timerCtx struct allocation (saves ~3 allocs per fetch).
	fetchCtx, cancel := context.WithCancel(fetchCtx)
	defer cancel()
	if h.fetchTimeout > 0 {
		timer := time.AfterFunc(h.fetchTimeout, cancel)
		defer timer.Stop()
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethodBytes(ctx.Method())
	req.SetRequestURIBytes(ctx.RequestURI())
	req.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		req.Header.AddBytesKV(k, v)
	}
	tracing.InjectFastHTTP(fetchCtx, req)

	if err := h.fastClient.Do(fetchCtx, req, resp); err != nil {
		tracing.RecordError(span, err)
		ctx.Error("upstream error", fasthttp.StatusBadGateway)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("BYPASS"))
		return
	}

	// Copy origin response headers, then overwrite bouine's attribution.
	dst := &ctx.Response.Header
	for k, v := range resp.Header.All() {
		if bytes.Equal(k, []byte(header.XCache)) || bytes.Equal(k, []byte(header.XCacheSource)) {
			continue
		}
		dst.AddBytesKV(k, v)
	}
	dst.SetCanonical(header.S2b(header.XCache), header.S2b("BYPASS"))
	ctx.SetStatusCode(resp.StatusCode())
	if string(ctx.Method()) != "HEAD" {
		_, _ = ctx.Write(resp.Body())
	}
}

// stripNoCacheFields removes headers named in a `no-cache="…"` field list
// from dst before writing it to the client. Per RFC 9111 §5.2.2.4 a cache
// MUST NOT forward these fields without successful revalidation.
// ccHeader is the merged Cache-Control string for the stored response.
func stripNoCacheFields(dst *fasthttp.ResponseHeader, ccHeader string) {
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
			del := header.InternKey(field)
			dst.Del(del)
		}
	}
}

// stripConnectionListedHeaders removes headers listed in the stored
// response's Connection header (RFC 9110 §7.6.1). The Connection header
// lists per-connection headers that must not be forwarded by proxies.
func stripConnectionListedHeaders(dst *fasthttp.ResponseHeader, stored header.Map) {
	conn := stored.Get(header.Connection)
	if conn == "" {
		return
	}
	for _, field := range strings.FieldsFunc(conn, func(r rune) bool {
		return r == ',' || r == ' '
	}) {
		if field != "" {
			del := header.InternKey(field)
			dst.Del(del)
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
// set as X-Cache-Source via SetCanonical (zero alloc).
//
// On the first hit, a pre-built *fasthttp.ResponseHeader is constructed
// from the stored headers (excluding hop-by-hop, internal, no-cache fields,
// and dynamic headers). On subsequent hits, CopyTo copies the pre-built
// header into the response, replacing N SetCanonical calls with a single
// bulk copyArgs. Only 4 dynamic headers (Age, X-Cache, X-Cache-Source,
// Warning) are set per request. The body is set via SetBodyRaw (zero-copy
// from the immutable cached body).
func (h *Handler) serveObject(ctx *fasthttp.RequestCtx, obj *api.Object, now time.Time, result cacheResult, src api.Source) {
	dst := &ctx.Response.Header

	// Use the pre-built header for CopyTo. This replaces per-header
	// SetCanonical calls (each doing initHeaderValueBytes + removeNewLines
	// + setSpecialHeader + setNonSpecial) with a single copyArgs bulk copy.
	fh := getOrComputeFastHeader(obj)
	fh.CopyTo(dst)

	// Set dynamic headers per request.
	var ageBuf [16]byte
	ageSeconds := int64(ComputeAge(obj, now).Seconds())
	ageStr := strconv.AppendInt(ageBuf[:0], ageSeconds, 10)
	dst.SetCanonical(header.S2b(header.Age), ageStr)

	dst.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(src)))

	switch result {
	case cacheHit:
		dst.SetCanonical(header.S2b(header.XCache), header.S2b("HIT"))
	case cacheStale:
		dst.SetCanonical(header.S2b(header.XCache), header.S2b("STALE"))
		dst.SetCanonical(header.S2b(header.Warning), header.S2b(`110 - "Response is Stale"`))
	case cacheRevalidated:
		dst.SetCanonical(header.S2b(header.XCache), header.S2b("REVALIDATED"))
	}

	ctx.SetStatusCode(obj.StatusCode)
	if string(ctx.Method()) != "HEAD" {
		ctx.Response.SetBodyRaw(obj.Body) // #nosec G705 -- obj.Body is an immutable cached origin response
	}
}

// collapsedFetch deduplicates concurrent origin fetches for the same key.
func (h *Handler) collapsedFetch(ctx *fasthttp.RequestCtx, key api.Key) fetchResult {
	v, _, _ := h.flight.Do(key.SingleFlightKey(0), func() (any, error) {
		res := h.doFetch(ctx)
		return res, nil
	})
	return v.(fetchResult)
}

// revalKeySuffix XORs the key with a constant to produce a singleflight
// key that won't collide with regular fetches for the same cache key,
// while still deduplicating concurrent revalidations for that key.
const revalKeySuffix uint64 = 0x726576616c // "reval" in ASCII

func (h *Handler) collapsedFetchBg(ctx context.Context, req *fasthttp.Request, key api.Key) fetchResult {
	v, _, _ := h.flight.Do(key.SingleFlightKey(0), func() (any, error) {
		res := h.doFetchBg(ctx, req)
		return res, nil
	})
	return v.(fetchResult)
}

func (h *Handler) collapsedRevalidateBg(ctx context.Context, req *fasthttp.Request, key api.Key) fetchResult {
	sfKey := key.SingleFlightKey(revalKeySuffix)
	v, _, _ := h.flight.Do(sfKey, func() (any, error) {
		res := h.doFetchBg(ctx, req)
		return res, nil
	})
	return v.(fetchResult)
}

func (h *Handler) doFetchBg(ctx context.Context, req *fasthttp.Request) (res fetchResult) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && errors.Is(err, ErrAbortHandler) {
				res = fetchResult{Err: ErrAbortHandler}
				return
			}
			panic(r) //nolint:forbidigo // re-panic for real (non-abort) panics
		}
	}()
	if h.fastClient == nil {
		return fetchResult{Err: fmt.Errorf("no fast client configured")}
	}
	fetchCtx, span := tracing.StartSpan(ctx, "bouine.origin")
	defer span.End()

	select {
	case h.fetchSem <- struct{}{}:
		defer func() { <-h.fetchSem }()
	default:
	}

	// Use context.WithCancel + time.AfterFunc instead of context.WithTimeout
	// to avoid the timerCtx struct allocation (saves ~3 allocs per fetch).
	fetchCtx, cancel := context.WithCancel(fetchCtx)
	defer cancel()
	if h.fetchTimeout > 0 {
		timer := time.AfterFunc(h.fetchTimeout, cancel)
		defer timer.Stop()
	}

	resp := fasthttp.AcquireResponse()

	if err := h.fastClient.Do(fetchCtx, req, resp); err != nil {
		fasthttp.ReleaseResponse(resp)
		tracing.RecordError(span, err)
		return fetchResult{Err: fmt.Errorf("origin fetch: %w", err)}
	}

	if h.maxResponseBytes > 0 && int64(len(resp.Body())) > h.maxResponseBytes {
		fasthttp.ReleaseResponse(resp)
		return fetchResult{Err: fmt.Errorf("upstream response exceeds %d bytes", h.maxResponseBytes)}
	}

	hdrMap := header.FromFastHTTP(&resp.Header)

	statusCode := resp.StatusCode()
	bodyCopy := make([]byte, len(resp.Body()))
	copy(bodyCopy, resp.Body())
	fasthttp.ReleaseResponse(resp)

	return fetchResult{
		StatusCode: statusCode,
		Header:     fromHeaderMap(hdrMap),
		Body:       bodyCopy,
	}
}

// releaseFetchResult releases the pooled fasthttp.Response if it was
// kept alive in fetchResult.fastResp for CopyTo-based header copying.
func releaseFetchResult(res fetchResult) {
	if res.fastResp != nil {
		fasthttp.ReleaseResponse(res.fastResp)
	}
}

func (h *Handler) fetchAndStore(ctx *fasthttp.RequestCtx, lookupKey, primaryKey api.Key, ri RequestInfo) {
	res := h.collapsedFetch(ctx, lookupKey)
	defer releaseFetchResult(res)
	if res.Err != nil {
		ctx.Error("upstream error", fasthttp.StatusBadGateway)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		return
	}
	h.writeAndMaybeStore(ctx, res, primaryKey, ri)
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
func (h *Handler) fetchAndStoreStayinAlive(ctx *fasthttp.RequestCtx, lookupKey, primaryKey api.Key, stale *api.Object, now time.Time, src api.Source, ri RequestInfo) {
	res := h.collapsedFetch(ctx, lookupKey)
	defer releaseFetchResult(res)
	if res.Err != nil {
		h.logger.Info("stayin-alive: upstream unreachable, serving stale indefinitely",
			"error", res.Err, "key", lookupKey)
		h.serveObject(ctx, stale, now, cacheStale, src)
		return
	}
	if res.StatusCode >= 500 {
		h.logger.Info("stayin-alive: upstream 5xx, serving stale indefinitely",
			"status", res.StatusCode, "key", lookupKey)
		h.serveObject(ctx, stale, now, cacheStale, src)
		return
	}
	h.writeAndMaybeStore(ctx, res, primaryKey, ri)
}

// revalidate sends a conditional request to the origin and refreshes the
// stored object. lookupKey is the key under which the stale object was found
// (may be a Vary variant key); it is used for singleflight dedup and for
// storing the 304-refreshed object. primaryKey is the canonical key used for
// Vary variant storage in writeAndMaybeStore.
func (h *Handler) revalidate(ctx *fasthttp.RequestCtx, primaryKey api.Key, lookupKey api.Key, stale *api.Object, now time.Time, src api.Source, ri RequestInfo) {
	revalReq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(revalReq)
	revalReq.Header.SetMethod(string(ctx.Method()))
	revalReq.SetRequestURI(string(ctx.RequestURI()))
	revalReq.Header.SetHost(string(ctx.Host()))
	for k, v := range ctx.Request.Header.All() {
		revalReq.Header.AddBytesKV(k, v)
	}
	setConditionalHeaders(func(k, v string) { revalReq.Header.Set(k, v) }, stale)

	// Collapse concurrent revalidations for the same key. Each concurrent
	// request that finds the same stale object would otherwise fire its
	// own conditional origin request. The singleflight key is suffixed
	// with a constant to avoid colliding with regular fetch collapsing
	// while still deduplicating revalidations for the same cache key.
	res := h.collapsedRevalidateBg(context.Background(), revalReq, lookupKey)
	defer releaseFetchResult(res)

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
		h.serveObject(ctx, stale, now, cacheStale, src)
		return
	}
	if res.Err != nil {
		if staleFallbackAllowed(stale) {
			h.serveObject(ctx, stale, now, cacheStale, src)
			return
		}
		ctx.Error("upstream error", fasthttp.StatusBadGateway)
		ctx.Response.Header.Set(header.XCache, "MISS")
		ctx.Response.Header.Set(header.XCacheSource, string(api.SourceOrigin))
		return
	}
	if res.StatusCode >= 500 {
		if staleFallbackAllowed(stale) {
			h.serveObject(ctx, stale, now, cacheStale, src)
			return
		}
	}

	if res.StatusCode == fasthttp.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res, now)
		h.storeObject(ctx, lookupKey, refreshed, ri, false, 0)
		h.serveObject(ctx, refreshed, now, cacheRevalidated, src)
		return
	}

	h.writeAndMaybeStore(ctx, res, primaryKey, ri)
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
	MergeHeaders304(refreshed, res.Header.ToMap())
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
func (h *Handler) triggerBgRevalidate(ctx *fasthttp.RequestCtx, ri RequestInfo, key api.Key, stale *api.Object) {
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
	bgCtx, bgCancel := context.WithCancel(context.WithoutCancel(ctx))
	bgReq := ri
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
func (h *Handler) doBackgroundRevalidate(ctx context.Context, ri RequestInfo, key api.Key, stale *api.Object) {
	revalReq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(revalReq)
	revalReq.Header.SetMethod(ri.Method)
	revalReq.SetRequestURI(ri.URI)
	revalReq.Header.SetHost(ri.Host)
	ri.Header.Range(func(k, v string) bool {
		revalReq.Header.Set(k, v)
		return true
	})
	setConditionalHeaders(func(k, v string) { revalReq.Header.Set(k, v) }, stale)

	staleHits := h.store.WindowHits(key)

	res := h.collapsedFetchBg(ctx, revalReq, key)
	if res.Err != nil {
		return
	}

	if res.StatusCode == fasthttp.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res, time.Now())
		h.storeObject(ctx, key, refreshed, ri, true, staleHits)
		return
	}

	// Pre-parse Cache-Control/CDN-Cache-Control once instead of up to 6
	// times (IsCacheable parses, isCacheBlocked re-parses for hasCDN,
	// IsCacheableWithDefault re-parses again).
	bgResMap := res.Header.ToMap()
	bgParsed := newParsedResponse(res.StatusCode, ri.Header, bgResMap)

	if bgParsed.isCacheableWithDefault(h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && bgResMap.Get(header.SetCookie) != "" {
			return
		}
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			return
		}
		obj := buildObject(key, ri, res, bgResMap, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now())
		h.storeObject(ctx, key, obj, ri, true, staleHits)
	}
}

func (h *Handler) writeAndMaybeStore(
	ctx *fasthttp.RequestCtx,
	res fetchResult,
	primaryKey api.Key,
	ri RequestInfo,
) {
	dst := &ctx.Response.Header
	res.Header.CopyToFastHTTP(dst)
	// Overwrite origin-supplied attribution headers (prevent spoofing).
	dst.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
	dst.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
	// Cache the ToMap() result — used below for header lookups (Age,
	// Set-Cookie, Vary) and passed to buildObject. Each ToMap() call
	// invokes FromFastHTTP which allocates a new header.Map.
	resMap := res.Header.ToMap()
	// A proxy SHOULD add an Age header to responses it forwards,
	// even on first fetch (Age: 0 + any origin Age).
	if resMap.Get(header.Age) == "" {
		dst.SetCanonical(header.S2b(header.Age), header.S2b("0"))
	}
	ctx.SetStatusCode(res.StatusCode)
	if !bytes.Equal(ctx.Method(), []byte("HEAD")) {
		ctx.Response.SetBodyRaw(res.Body)
	}

	// Pre-parse Cache-Control/CDN-Cache-Control once instead of up to 6
	// times (IsCacheable parses, isCacheBlocked re-parses for hasCDN,
	// IsCacheableWithDefault re-parses again).
	parsed := newParsedResponse(res.StatusCode, ri.Header, resMap)

	if parsed.isCacheableWithDefault(h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && resMap.Get(header.SetCookie) != "" {
			return
		}
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			return
		}
		// primaryKey is passed in from lookup() to avoid a redundant
		// buildKey call on the same request.
		storeKey := primaryKey
		if vary := resMap.Get(header.Vary); vary != "" {
			storeKey = VariantKey(primaryKey, vary, ri.Header, h.policy)
		}
		// Enforce MaxVariants cap: skip storage if this primary key already
		// has MaxVariants distinct Vary variants. RFC 9110 §12.5.5 — unbounded
		// variants are a DoS vector.
		if storeKey != primaryKey {
			if !h.reserveVariantSlot(ctx, primaryKey, storeKey) {
				return
			}
		}
		obj := buildObject(storeKey, ri, res, resMap, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now())
		h.storeObject(ctx, storeKey, obj, ri, false, 0)
		// In strong mode, storeObject is a no-op for non-owners. Forward
		// the freshly fetched object to the owner so subsequent peer-fetches
		// from this or other non-owners hit instead of going to origin
		// (issue #509). The owner resolves from obj.Key (storeKey).
		h.forwardToOwnerIfRemote(ctx, obj)
		if storeKey != primaryKey {
			// Shallow-copy the object and change only the Key. This avoids
			// a second full buildObject call (~5 allocs: api.Object,
			// header.FromFastHTTP, serializeHead, parseSurrogateKeys, etc.).
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
			h.storeObject(ctx, primaryKey, primaryObj, ri, false, 0)
			// Forward the primary (Vary-resolver) entry to its owner too —
			// the primary key may hash to a different owner than the variant.
			h.forwardToOwnerIfRemote(ctx, primaryObj)
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

// invalidateAndProxy handles POST/PUT/DELETE by fetching the origin
// response via FastClient, proxying it to the client, and invalidating
// cached entries for the affected resource.
func (h *Handler) invalidateAndProxy(ctx *fasthttp.RequestCtx) {
	if h.fastClient == nil {
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		ctx.Error("upstream error: no fast client configured", fasthttp.StatusBadGateway)
		return
	}
	bgCtx := context.Background()
	fetchCtx, span := tracing.StartSpan(bgCtx, "bouine.origin")
	defer span.End()

	// Use context.WithCancel + time.AfterFunc instead of context.WithTimeout
	// to avoid the timerCtx struct allocation (saves ~3 allocs per fetch).
	fetchCtx, cancel := context.WithCancel(fetchCtx)
	defer cancel()
	if h.fetchTimeout > 0 {
		timer := time.AfterFunc(h.fetchTimeout, cancel)
		defer timer.Stop()
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethodBytes(ctx.Method())
	req.SetRequestURIBytes(ctx.RequestURI())
	req.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		req.Header.AddBytesKV(k, v)
	}
	if ctx.Request.Body() != nil {
		body := ctx.Request.Body()
		req.SetBodyRaw(body)
	}
	tracing.InjectFastHTTP(fetchCtx, req)

	if err := h.fastClient.Do(fetchCtx, req, resp); err != nil {
		tracing.RecordError(span, err)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		ctx.Error("upstream error", fasthttp.StatusBadGateway)
		return
	}

	if h.maxResponseBytes > 0 && int64(len(resp.Body())) > h.maxResponseBytes {
		h.logger.Warn("upstream response exceeded max_response_bytes, aborting",
			"key", h.buildKey(ctx), "limit", h.maxResponseBytes)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		ctx.Error("upstream response too large", fasthttp.StatusBadGateway)
		return
	}

	// Write the captured response to the client.
	dst := &ctx.Response.Header
	for k, v := range resp.Header.All() {
		if bytes.Equal(k, []byte(header.XCache)) || bytes.Equal(k, []byte(header.XCacheSource)) {
			continue
		}
		dst.AddBytesKV(k, v)
	}
	dst.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
	dst.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
	ctx.SetStatusCode(resp.StatusCode())
	_, _ = ctx.Write(resp.Body())

	// Only invalidate on 2xx/3xx success.
	statusCode := resp.StatusCode()
	if statusCode >= 200 && statusCode < 400 {
		h.invalidateAfterProxyFast(ctx, resp)
	}
}

// invalidateAfterProxyFast handles cache invalidation and optional
// POST-response storage after a successful fasthttp origin fetch.
func (h *Handler) invalidateAfterProxyFast(ctx *fasthttp.RequestCtx, resp *fasthttp.Response) {
	getRI := requestInfoFromCtx(ctx)
	getRI.Method = "GET"
	key := BuildKey(getRI, h.policy)
	_, _ = h.Purge(ctx, key)

	// Evict Content-Location and Location URLs (RFC 9111 §4.4).
	for _, hdr := range []string{header.ContentLocation, header.Location} {
		if loc := string(resp.Header.Peek(hdr)); loc != "" {
			locKey := h.buildLocationKey(ctx, loc)
			if !locKey.IsZero() {
				_, _ = h.Purge(ctx, locKey)
			}
		}
	}

	// RFC 9111 §4.3.1: store POST response if it has explicit
	// freshness and Content-Location matching the request URI.
	h.maybeStorePostResponseFast(ctx, getRI, key, resp)
}

// maybeStorePostResponseFast stores a successful POST response under
// the GET key when it has explicit freshness and a matching
// Content-Location (RFC 9111 §4.3.1).
func (h *Handler) maybeStorePostResponseFast(ctx *fasthttp.RequestCtx, getRI RequestInfo, key api.Key, resp *fasthttp.Response) {
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
	hdr := header.FromFastHTTP(&resp.Header)
	res := fetchResult{
		StatusCode: resp.StatusCode(),
		Header:     fromHeaderMap(hdr),
		Body:       body,
	}
	now := time.Now()
	obj := buildObject(key, getRI, res, hdr, h.negativeTTL, h.defaultTTL, h.overrideTTL,
		h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, now)
	if obj == nil {
		return
	}
	h.storeObject(ctx, key, obj, getRI, false, 0)
}

func (h *Handler) buildLocationKey(ctx *fasthttp.RequestCtx, loc string) api.Key {
	baseURL, _ := url.Parse(string(ctx.RequestURI()))
	ref, err := url.Parse(loc)
	if err != nil {
		return api.Key{}
	}
	resolved := baseURL.ResolveReference(ref)
	scheme := resolved.Scheme
	if scheme == "" {
		if ctx.IsTLS() {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := resolved.Host
	if host == "" {
		host = string(ctx.Host())
	}
	ri := requestInfoFromURL("GET", resolved.String())
	ri.Host = host
	ri.TLS = scheme == "https"
	return BuildKey(ri, h.policy)
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
func (h *Handler) storeObject(ctx context.Context, key api.Key, obj *api.Object, ri RequestInfo, isRefresh bool, staleHits int64) {
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
		h.refreshRegistry.Register(key, ri, varyHeader, h.refreshPersistCycles)
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

func (h *Handler) doFetch(ctx *fasthttp.RequestCtx) (res fetchResult) {
	// Fetch from origin using FastClient. The response is captured
	// directly in a pooled *fasthttp.Response with no intermediate
	// header.Map or body buffer allocations.
	return h.doFetchFast(ctx)
}

// doFetchFast performs an origin fetch using the fasthttp client.
// The response is captured directly in a pooled *fasthttp.Response.
func (h *Handler) doFetchFast(ctx *fasthttp.RequestCtx) (res fetchResult) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && errors.Is(err, ErrAbortHandler) {
				res = fetchResult{Err: ErrAbortHandler}
				return
			}
			panic(r) //nolint:forbidigo // re-panic for real (non-abort) panics
		}
	}()
	if h.fastClient == nil {
		return fetchResult{Err: fmt.Errorf("no fast client configured")}
	}
	// Use context.Background() as the base because *fasthttp.RequestCtx.Done()
	// panics when the ctx was created manually (not by a real fasthttp server).
	bgCtx := context.Background()
	fetchCtx, span := tracing.StartSpan(bgCtx, "bouine.origin")
	defer span.End()

	select {
	case h.fetchSem <- struct{}{}:
		defer func() { <-h.fetchSem }()
	case <-fetchCtx.Done():
		return fetchResult{Err: fmt.Errorf("origin fetch semaphore: %w", fetchCtx.Err())}
	}

	// Use context.WithCancel + time.AfterFunc instead of context.WithTimeout
	// to avoid the timerCtx struct allocation (saves ~3 allocs per fetch).
	fetchCtx, cancel := context.WithCancel(fetchCtx)
	defer cancel()
	if h.fetchTimeout > 0 {
		timer := time.AfterFunc(h.fetchTimeout, cancel)
		defer timer.Stop()
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	// resp is NOT released via defer — it's returned in fetchResult.fastResp
	// so that Body (which references resp's internal buffer) survives.
	// The caller (collapsedFetch) releases it after all singleflight
	// waiters have finished.

	// Populate the request from *fasthttp.RequestCtx.
	// Use *Bytes variants to avoid string([]byte) conversions.
	req.Header.SetMethodBytes(ctx.Method())
	req.SetRequestURIBytes(ctx.RequestURI())
	req.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		req.Header.AddBytesKV(k, v)
	}
	// Inject W3C TraceContext.
	tracing.InjectFastHTTP(fetchCtx, req)

	if err := h.fastClient.Do(fetchCtx, req, resp); err != nil {
		fasthttp.ReleaseResponse(resp)
		tracing.RecordError(span, err)
		return fetchResult{Err: fmt.Errorf("origin fetch: %w", err)}
	}

	// Check for max response bytes.
	if h.maxResponseBytes > 0 && int64(len(resp.Body())) > h.maxResponseBytes {
		fasthttp.ReleaseResponse(resp)
		return fetchResult{Err: fmt.Errorf("upstream response exceeds %d bytes", h.maxResponseBytes)}
	}

	// Copy the body to an independent slice. The pooled response is
	// kept alive in fetchResult.fastResp so writeAndMaybeStore can use
	// CopyTo for zero-normalization header copying. It is released by
	// releaseFetchResult after all singleflight waiters have finished.
	statusCode := resp.StatusCode()
	bodyCopy := make([]byte, len(resp.Body()))
	copy(bodyCopy, resp.Body())

	return fetchResult{
		StatusCode: statusCode,
		Header:     fromFastHTTPHeader(&resp.Header),
		Body:       bodyCopy,
		fastResp:   resp,
	}
}

//nolint:gocyclo // 16: TTL/freshness conditionals are inherently branchy
func buildObject(key api.Key, ri RequestInfo, res fetchResult, resMap header.Map, negativeTTL, defaultTTL, overrideTTL, defaultSWR, defaultSIE time.Duration, jitterPct int, policy *KeyPolicy, now time.Time) *api.Object {
	// Parse Cache-Control (may be multiple headers — merge first).
	// CDN-Cache-Control overrides Cache-Control for shared caches (RFC 9211):
	// use it as the authoritative directive source when present.
	//
	// Cache the ToMap() result — it was called 5x before, each allocating a
	// new header.Map via FromFastHTTP. Now passed in from writeAndMaybeStore.
	ccHeader := resMap.GetAll(header.CacheControl)
	var respCC Directives
	if cdnCC, hasCDN := cdnCacheControl(resMap); hasCDN {
		respCC = cdnCC
		// Store CDN-Cache-Control string as the object's pre-parsed CC so
		// Evaluate reads the CDN directives on every hit path.
		ccHeader = mergeHeaderValues(resMap, header.CDNCacheControl)
	} else {
		respCC = ParseCacheControl(ccHeader)
	}
	originAge := parseOriginAge(resMap)
	// RFC 9111 §4.2.3: corrected_initial_age = max(apparent_age, age_value).
	// Apparent age is derived from the Date header: max(0, now - Date).
	if dateStr := resMap.Get(header.Date); dateStr != "" {
		if dt := parseHTTPDate(dateStr); !dt.IsZero() && !dt.After(now) {
			if apparentAge := now.Sub(dt); apparentAge > originAge {
				originAge = apparentAge
			}
		}
	}
	// computeTTL consolidates heuristic, fallback, negative, jitter, and Age subtraction.
	ttl := computeTTL(resMap, res.StatusCode, respCC, negativeTTL, defaultTTL, jitterPct, originAge, now)
	// Route-level override wins over the upstream's freshness directives.
	// Applied after computeTTL so jitter is seeded from the override value,
	// not the origin's max-age. The stored object retains the unaltered
	// upstream Cache-Control header, which is forwarded to downstream clients.
	if overrideTTL > 0 {
		ttl = JitterTTL(overrideTTL, jitterPct)
	}

	// Copy body — res.Body may reference a pooled fasthttp.Response
	// buffer that is released after writeAndMaybeStore. The cached
	// object must own its body independently.
	bodyCopy := make([]byte, len(res.Body))
	copy(bodyCopy, res.Body)

	obj := &api.Object{
		Key:          key,
		StatusCode:   res.StatusCode,
		Header:       resMap,
		Body:         bodyCopy,
		BodySize:     int64(len(bodyCopy)),
		StoredAt:     now,
		TTL:          ttl,
		ETag:         resMap.Get(header.ETag),
		CacheControl: ccHeader,  // Lead 1: pre-stored, avoids re-parsing on every hit
		OriginAge:    originAge, // Lead 3: pre-stored, avoids re-parsing on the read path
	}
	// Stamp internal headers for ban predicate matching. These are
	// stripped before serving to clients (see serveObject).
	obj.Header.Set(header.XBouinePath, ri.Path)
	obj.Header.Set(header.XBouineHost, ri.Host)
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
		obj.StatusCode != fasthttp.StatusNotModified {
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
	if lm := obj.Header.Get(header.LastModified); lm != "" {
		if t, err := time.Parse(httpTimeFormat, lm); err == nil {
			obj.LastModified = t
		}
	}
	obj.VaryKey = BuildVaryKey(obj.Header.Get(header.Vary), ri.Header, policy)

	obj.SurrogateKeys = parseSurrogateKeys(resMap)

	// Pre-compute whether strip functions are needed on the hit path.
	// Most objects have neither Connection headers nor no-cache fields,
	// so the hit path can skip those calls entirely.
	obj.HasConnectionList = obj.Header.Get(header.Connection) != ""
	if ccHeader != "" {
		cc := ParseCacheControl(ccHeader)
		obj.HasNoCacheFields = cc.NoCacheFields != ""
	}

	// SerializedHead is not computed here — it is lazily computed on
	// the first fast-path cache hit by getOrComputeSerializedHead.
	// This avoids allocating ~512 bytes per object for objects that
	// are never served via the fast-path (misses, full handler path).

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
func computeTTL(hdr header.Map, status int, respCC Directives,
	negativeTTL, defaultTTL time.Duration, jitterPct int,
	originAge time.Duration, now time.Time) time.Duration {
	ttl, explicit := FreshnessLifetimeH(respCC, hdr)
	if !explicit {
		ttl = HeuristicTTL(hdr, now)
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
func parseSurrogateKeys(h header.Map) []string {
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
	return method != "GET" &&
		method != "HEAD" &&
		method != "OPTIONS"
}
