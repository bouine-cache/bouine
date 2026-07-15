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
)

// defaultRefreshConcurrency bounds concurrent background refresh fetches
// per route. Distinct from bgRevalSem (256, shared) and fetchSem (32,
// per-handler foreground). Refresh fetches are typically 304s (no body),
// so memory pressure is minimal.
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

// bgRevalSem bounds concurrent background stale-while-revalidate
// goroutines. When full, excess revalidation attempts are silently
// dropped — the next client will re-trigger if still stale.
var bgRevalSem = make(chan struct{}, 256)

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
	Header     http.Header
	Body       []byte
	Err        error
}

// maxRecorderCap bounds the backing array retained by the recorder pool.
// Recorders that grew past this on a large response are discarded so the
// pool never pins a transiently oversized buffer across GC cycles.
const maxRecorderCap = 1 << 20 // 1 MiB

// recorderPool reuses responseRecorder instances on the miss and
// invalidation paths. The miss path transfers ownership of the
// recorder's header map and body buffer to the fetchResult, then gives
// the recorder fresh internals before returning it to the pool. The
// hit path never allocates a recorder.
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
	store            storage.Store
	flight           singleflight.Group
	logger           observability.Logger
	negativeTTL      time.Duration
	jitterPercent    int
	stayinAlive      bool
	defaultTTL       time.Duration   // operator fallback when origin sends no freshness
	overrideTTL      time.Duration   // operator override; wins over origin max-age/Expires when > 0
	defaultSWR       time.Duration   // operator-level stale-while-revalidate floor
	defaultSIE       time.Duration   // operator-level stale-if-error floor
	allowSetCookie   bool            // when false (default), Set-Cookie blocks caching
	maxObjectSize    int64           // skip storage for responses larger than this; 0 = no limit
	maxResponseBytes int64           // hard cap on body buffering; 0 = defaultMaxResponseBytes
	fetchSem         chan struct{}   // bounds concurrent foreground origin fetches
	fetchTimeout     time.Duration   // bounds total origin fetch time; 0 = defaultFetchTimeout
	stripQueryParams map[string]bool // query params to exclude from cache key; nil = none
	excludeHeaders   map[string]bool // headers to exclude from Vary variant key; nil = none

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
	// limit (64 MiB).
	MaxResponseBytes int64
	// MaxFetchConcurrency bounds the number of concurrent foreground
	// origin fetches. When the limit is reached, additional fetches
	// block until a slot frees or the request context is cancelled.
	// Zero (default) applies a safe built-in limit (64).
	MaxFetchConcurrency int
	// FetchTimeout bounds the total time for an origin fetch (header +
	// body). When exceeded, the fetch is aborted and the caller receives
	// a fetchResult error. Zero (default) applies a safe built-in limit
	// (60s). This replaces the blanket WriteTimeout on the data plane.
	FetchTimeout time.Duration
	// StripQueryParams, when non-nil, excludes the listed query parameter
	// names from the cache key. The parameters are still forwarded to
	// the upstream. Allocated once at handler construction.
	StripQueryParams map[string]bool
	// ExcludeHeaders, when non-nil, excludes the listed request header
	// names from the Vary-based variant key. The headers are still
	// forwarded to the upstream and the Vary response header is left
	// intact — only the key computation skips them.
	ExcludeHeaders map[string]bool
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
}

// NewHandler creates a caching handler.
func NewHandler(cfg HandlerConfig) *Handler {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	h := &Handler{
		upstream:             cfg.Upstream,
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
		allowSetCookie:       cfg.AllowSetCookie,
		maxObjectSize:        cfg.MaxObjectSize,
		maxResponseBytes:     cfg.MaxResponseBytes,
		stripQueryParams:     cfg.StripQueryParams,
		excludeHeaders:       cfg.ExcludeHeaders,
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

	return h
}

// Close drains in-flight refresh goroutines and stops the scheduler.
// Called during engine shutdown before store.Close() to prevent
// use-after-close panics. Safe to call multiple times.
func (h *Handler) Close(ctx context.Context) error {
	h.closeOnce.Do(func() {
		close(h.done)
	})
	if h.scheduler != nil {
		h.scheduler.Stop()
	}

	if h.refreshSem != nil {
		done := make(chan struct{})
		go func() {
			h.refreshWg.Wait()
			close(done)
		}()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// lookupForRefresh returns the live object for key, or nil if the
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
		refreshed := h.refreshFrom304(stale, res)
		h.storeObject(ctx, key, refreshed, req, true, staleHits)
		h.refreshMetrics.IncTotal("304")
		return
	}

	if IsCacheableWithDefault(res.StatusCode, req.Header, res.Header, h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && res.Header.Get(header.SetCookie) != "" {
			h.refreshMetrics.IncSkips("set_cookie")
			return
		}
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			h.refreshMetrics.IncSkips("too_large")
			return
		}
		obj := buildObject(key, req, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.excludeHeaders)
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

// buildKey constructs the cache key, applying strip_query_params when
// configured. Inlined to avoid variadic spread overhead on the hit path
// when no strip is configured (zero added allocs).
func (h *Handler) buildKey(r *http.Request) api.Key {
	if h.stripQueryParams != nil {
		return BuildKey(r, h.stripQueryParams)
	}
	return BuildKey(r)
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
	key, obj, src := h.lookup(r)
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
		h.serveObject(w, r, disp.Object, now, cacheHit, src)
		if disp.Decision == StaleHit && disp.Object.StaleWhileRevalidate > 0 {
			h.triggerBgRevalidate(r, key, disp.Object)
		}
	case Miss:
		h.handleCacheMiss(w, r, key, obj, now, src)
	case Revalidate:
		h.revalidate(w, r, key, disp.Object, now, src)
	case Bypass:
		h.handleBypass(w, r)
	}
}

// handleCacheMiss handles a cache miss: attempts peer-fetch (L5) first, then
// falls back to origin via fetchAndStore or fetchAndStoreStayinAlive.
// Cluster peer-fetch: if this node does not own the key, ask the owner before
// going to origin. The owner has a much higher hit rate for keys it owns
// (consistent hashing concentrates fills there). On a peer hit the object is
// stored locally for future requests on this node (L0 promotion).
// src is the storage-tier source from lookup (hot/warm); it is overridden
// to "peer" on a successful peer hit.
func (h *Handler) handleCacheMiss(w http.ResponseWriter, r *http.Request, key api.Key, obj *api.Object, now time.Time, src api.Source) {
	if h.ownerFn != nil && h.peerFetch != nil {
		if owner, isLocal := h.ownerFn(key); !isLocal {
			if peerObj, err := h.peerFetch(r.Context(), owner, key); err == nil && peerObj != nil {
				// Re-evaluate: the peer may have returned a stale object.
				if d2 := Evaluate(r, peerObj, now); d2.Decision == Hit || d2.Decision == StaleHit {
					h.serveObject(w, r, peerObj, now, cacheHit, api.SourcePeer)
					// Promote to local hot tier (best-effort; ignore error).
					_ = h.store.Put(r.Context(), key, peerObj)
					if d2.Decision == StaleHit && peerObj.StaleWhileRevalidate > 0 {
						h.triggerBgRevalidate(r, key, peerObj)
					}
					return
				}
			} else if err != nil {
				h.logger.Debug("peer fetch error, falling back to origin",
					"peer", owner.Addr, "key", key, "error", err)
			}
		}
	}
	// If a stale object exists, use fetchAndStoreStayinAlive which will serve
	// the stale copy on 5xx/error — unless the stored response has
	// must-revalidate / proxy-revalidate / no-cache / s-maxage, which require
	// the error to be forwarded to the client.
	if obj != nil && (h.stayinAlive || obj.StaleForSIE(now) || staleFallbackAllowed(obj)) {
		h.fetchAndStoreStayinAlive(w, r, key, obj, now, src)
	} else {
		h.fetchAndStore(w, r, key)
	}
}

// lookup resolves the cache key and stored object for r, accounting
// for Vary-based secondary keys. Returns the source (hot/warm) from
// the storage tier that served the object.
func (h *Handler) lookup(r *http.Request) (api.Key, *api.Object, api.Source) {
	key := h.buildKey(r)
	obj, src, err := h.store.Get(r.Context(), key)
	if err != nil {
		h.logger.Warn("cache lookup error", "key", key, "error", err)
	}
	if obj == nil || obj.Header.Get(header.Vary) == "" {
		return key, obj, src
	}
	vk := VariantKey(key, obj.Header.Get(header.Vary), r.Header, h.excludeHeaders)
	if vk == key {
		return key, obj, src
	}
	vobj, vsrc, verr := h.store.Get(r.Context(), vk)
	if verr == nil && vobj != nil {
		return vk, vobj, vsrc
	}
	return vk, nil, ""
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
	// Wrap the writer so the upstream cannot clobber bouine's X-Cache
	// (BYPASS) or inject an X-Cache-Source that would spoof the source
	// metric label. Source is empty for BYPASS — origin contact is not
	// attributed to a cache tier.
	guard := &headerGuard{
		ResponseWriter: w,
		cache:          headerBYPASS,
	}
	h.upstream.ServeHTTP(guard, r)
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
	v, _, _ := h.flight.Do(strconv.FormatUint(uint64(key), 36), func() (any, error) {
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
	sfKey := strconv.FormatUint(uint64(key)^revalKeySuffix, 36)
	v, _, _ := h.flight.Do(sfKey, func() (any, error) {
		res := h.doFetch(r)
		return res, nil
	})
	return v.(fetchResult)
}

func (h *Handler) fetchAndStore(w http.ResponseWriter, r *http.Request, key api.Key) {
	res := h.collapsedFetch(r, key)
	if res.Err != nil {
		w.Header()[header.XCache] = headerMISS
		w.Header()[header.XCacheSource] = sourceOrigin
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	h.writeAndMaybeStore(w, r, res)
}

// fetchAndStoreStayinAlive is like fetchAndStore but falls back to
// serving the super-stale obj if the upstream is unavailable.
// src is the original storage-tier source from lookup (hot/warm),
// threaded to stale-fallback serveObject calls.
func (h *Handler) fetchAndStoreStayinAlive(w http.ResponseWriter, r *http.Request, key api.Key, stale *api.Object, now time.Time, src api.Source) {
	res := h.collapsedFetch(r, key)
	if res.Err != nil {
		h.logger.Warn("stayin-alive: upstream unreachable, serving stale indefinitely",
			"error", res.Err, "key", key)
		h.serveObject(w, r, stale, now, cacheStale, src)
		return
	}
	if res.StatusCode >= 500 {
		h.logger.Warn("stayin-alive: upstream 5xx, serving stale indefinitely",
			"status", res.StatusCode, "key", key)
		h.serveObject(w, r, stale, now, cacheStale, src)
		return
	}
	h.writeAndMaybeStore(w, r, res)
}

func (h *Handler) revalidate(w http.ResponseWriter, r *http.Request, key api.Key, stale *api.Object, now time.Time, src api.Source) {
	revalReq := r.Clone(r.Context())
	ConditionalHeaders(revalReq, stale)

	// Collapse concurrent revalidations for the same key. Each concurrent
	// request that finds the same stale object would otherwise fire its
	// own conditional origin request. The singleflight key is suffixed
	// with a constant to avoid colliding with regular fetch collapsing
	// while still deduplicating revalidations for the same cache key.
	res := h.collapsedRevalidate(revalReq, key)
	if res.Err != nil {
		h.serveObject(w, r, stale, now, cacheStale, src)
		return
	}

	// stale-if-error (RFC 5861 §4) and general stale-on-error: if origin
	// returns 5xx, serve stale unless must-revalidate/proxy-revalidate
	// explicitly forbids it (RFC 9111 §5.2.2.2).
	if res.StatusCode >= 500 {
		if h.stayinAlive {
			h.logger.Warn("stayin-alive: upstream 5xx, serving stale indefinitely",
				"status", res.StatusCode, "key", stale.Key)
			h.serveObject(w, r, stale, now, cacheStale, src)
			return
		}
		// Serve stale unless the stored response demands revalidation.
		// Use the same gate as the miss path (staleFallbackAllowed) so the
		// policy can't drift between the two stale-on-error sites.
		if staleFallbackAllowed(stale) {
			h.serveObject(w, r, stale, now, cacheStale, src)
			return
		}
	}

	if res.StatusCode == http.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res)
		h.storeObject(r.Context(), key, refreshed, r, false, 0)
		h.serveObject(w, r, refreshed, now, cacheRevalidated, src)
		return
	}

	h.writeAndMaybeStore(w, r, res)
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
func (h *Handler) refreshFrom304(stale *api.Object, res fetchResult) *api.Object {
	refreshed := *stale
	refreshed.Header = stale.Header.Clone()
	refreshed.StoredAt = time.Now()
	// Reset Hits to 0 for the new TTL window. Object.Hits is a SIEVE
	// eviction signal; the per-window popularity gate uses windowHits
	// from the store, not Object.Hits.
	refreshed.Hits = 0
	MergeHeaders304(&refreshed, res.Header)
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
	return &refreshed
}

// triggerBgRevalidate fires a background goroutine that fetches a fresh
// copy of the stale object and updates the store. Called after serving a
// stale-while-revalidate response. bgRevalSem prevents goroutine explosion.
func (h *Handler) triggerBgRevalidate(r *http.Request, key api.Key, stale *api.Object) {
	select {
	case bgRevalSem <- struct{}{}:
	default:
		return // semaphore full — next client will retry
	}
	// Detach from the client's context so the background fetch is not
	// cancelled when the response is sent.
	bgCtx := context.WithoutCancel(r.Context())
	bgReq := r.Clone(bgCtx)
	go func() {
		defer func() { <-bgRevalSem }()
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
		refreshed := h.refreshFrom304(stale, res)
		h.storeObject(ctx, key, refreshed, r, true, staleHits)
		return
	}

	if IsCacheableWithDefault(res.StatusCode, r.Header, res.Header, h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && res.Header.Get(header.SetCookie) != "" {
			return
		}
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			return
		}
		obj := buildObject(key, r, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.excludeHeaders)
		h.storeObject(ctx, key, obj, r, true, staleHits)
	}
}

func (h *Handler) writeAndMaybeStore(
	w http.ResponseWriter,
	r *http.Request,
	res fetchResult,
) {
	dst := w.Header()
	for k, vals := range res.Header {
		dst[k] = vals
	}
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

	if IsCacheableWithDefault(res.StatusCode, r.Header, res.Header, h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && res.Header.Get(header.SetCookie) != "" {
			return
		}
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			return
		}
		// Always compute from the canonical URL so the cap is enforced
		// against the primary key regardless of whether lookup() already
		// resolved the variant key.
		primaryKey := h.buildKey(r)
		storeKey := primaryKey
		if vary := res.Header.Get(header.Vary); vary != "" {
			storeKey = VariantKey(primaryKey, vary, r.Header, h.excludeHeaders)
		}
		// Enforce MaxVariants cap: skip storage if this primary key already
		// has MaxVariants distinct Vary variants. RFC 9110 §12.5.5 — unbounded
		// variants are a DoS vector.
		if storeKey != primaryKey {
			if !h.reserveVariantSlot(r.Context(), primaryKey, storeKey) {
				return
			}
		}
		obj := buildObject(storeKey, r, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.excludeHeaders)
		h.storeObject(r.Context(), storeKey, obj, r, false, 0)
		if storeKey != primaryKey {
			// Shallow-copy the object and change only the Key. This avoids
			// a second full buildObject call (~5 allocs: api.Object,
			// header.FromHTTP, serializeHead, parseSurrogateKeys, etc.).
			// The two objects share Header and Body, which are immutable
			// after buildObject. Hits are per-pointer (HotStore.Get
			// increments entry.obj.Hits on the specific stored pointer).
			primaryObj := *obj
			primaryObj.Key = primaryKey
			h.storeObject(r.Context(), primaryKey, &primaryObj, r, false, 0)
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
		_ = h.store.Delete(r.Context(), key)
		h.variantMu.Lock()
		delete(h.variantSets, key)
		h.variantMu.Unlock()
		if h.refreshRegistry != nil {
			h.refreshRegistry.Unregister(key)
		}

		// Also evict Content-Location and Location URLs (RFC 9111
		// §4.4). These are URI references (RFC 9110 §8.7) and may be
		// absolute, relative, or same-origin bare paths.
		for _, hdr := range []string{header.ContentLocation, header.Location} {
			if loc := rec.header.Get(hdr); loc != "" {
				locKey := h.buildLocationKey(r, loc)
				if locKey != 0 {
					_ = h.store.Delete(r.Context(), locKey)
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
		Header:     rec.header.Clone(),
		Body:       bodyCopy,
	}
	obj := buildObject(key, getReq, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.excludeHeaders)
	h.storeObject(r.Context(), key, obj, getReq, false, 0)
}

func (h *Handler) buildLocationKey(r *http.Request, loc string) api.Key {
	ref, err := url.Parse(loc)
	if err != nil {
		return 0
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
	_ = h.store.Put(ctx, key, obj)
	if h.refreshBeforeExpiry && obj.TTL >= minRefreshTTL {
		if IsNegativeCacheable(obj.StatusCode) {
			return
		}
		if h.ownerFn != nil {
			if _, isLocal := h.ownerFn(key); !isLocal {
				return
			}
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

	// Transfer ownership of the recorder's header map and body buffer to
	// the fetchResult, then give the recorder fresh internals before it is
	// returned to the pool. This eliminates the body make+copy and the
	// header.Clone() that the previous defensive copy required (~10 allocs
	// for a typical response with N headers: original 2+N allocs → 2 allocs
	// for the fresh map and buffer).
	//
	// The body slice is clipped to cap==len so the stored object doesn't
	// retain the buffer's excess capacity (bytes.Buffer doubles its backing
	// array, so cap can be up to 2x the content length).
	//
	// Safety: all singleflight waiters read res.Header and res.Body without
	// mutating. The recorder gets fresh internals so the pool reuse is safe.
	body := slices.Clip(rec.body.Bytes())
	hdr := rec.header
	rec.header = make(http.Header, 8)
	rec.body = &bytes.Buffer{}

	return fetchResult{
		StatusCode: rec.statusCode,
		Header:     hdr,
		Body:       body,
	}
}

func buildObject(key api.Key, r *http.Request, res fetchResult, negativeTTL, defaultTTL, overrideTTL, defaultSWR, defaultSIE time.Duration, jitterPct int, excludeHeaders map[string]bool) *api.Object {
	now := time.Now()
	// Parse Cache-Control (may be multiple headers — merge first).
	// CDN-Cache-Control overrides Cache-Control for shared caches (RFC 9211):
	// use it as the authoritative directive source when present.
	ccHeader := mergeHeaderValues(res.Header, header.CacheControl)
	var respCC Directives
	if cdnCC, hasCDN := cdnCacheControl(res.Header); hasCDN {
		respCC = cdnCC
		// Store CDN-Cache-Control string as the object's pre-parsed CC so
		// Evaluate reads the CDN directives on every hit path.
		ccHeader = mergeHeaderValues(res.Header, "CDN-Cache-Control")
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
	ttl := computeTTL(res.Header, res.StatusCode, respCC, negativeTTL, defaultTTL, jitterPct, originAge, now)
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
		Header:       header.FromHTTP(res.Header),
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
	obj.VaryKey = BuildVaryKey(res.Header.Get(header.Vary), r.Header, excludeHeaders)

	obj.SurrogateKeys = parseSurrogateKeys(res.Header)

	obj.SerializedHead = serializeHead(obj)

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
		seen := make(map[string]bool)
		var keys []string
		for _, tag := range strings.Fields(strings.ReplaceAll(v, ",", " ")) {
			tag = strings.TrimSpace(tag)
			if tag != "" && !seen[tag] {
				keys = append(keys, tag)
				seen[tag] = true
			}
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
