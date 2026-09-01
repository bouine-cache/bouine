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
	"sync/atomic"
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

// ErrFetchShed is returned when a foreground origin fetch gave up waiting
// for a fetch-semaphore slot after fetchWaitTimeout. Callers map it to
// stale-on-shed (RFC 5861-style) when a stale object is in scope, or to
// 503 + Retry-After otherwise — distinct from the 502 origin-failure
// mapping. The wait bound is the fix for the goroutine-pileup livelock
// (issue #562): previously the foreground paths parked on the semaphore
// with a dead cancellation arm and nothing ever un-parked them.
var ErrFetchShed = errors.New("origin fetch queue wait timeout")

// defaultFetchWaitTimeout bounds how long a foreground miss may wait for
// a fetch-semaphore slot before shedding. It is deliberately independent
// of fetch_timeout, which starts only after the slot is acquired (pinned
// by TestDoFetchTimeoutStartsAfterSemaphore): the wait bound exists to
// keep request goroutines from piling up when origin latency inflates
// concurrent fetch demand far above the semaphore size (issue #562).
// 100ms sheds the excess while still absorbing short queueing bursts.
const defaultFetchWaitTimeout = 100 * time.Millisecond

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
		TLS:         ctx.IsTLS(),
		Header:      headerFromCtx(ctx),
		methodBytes: ctx.Method(),
		uriBytes:    ctx.RequestURI(),
		hostBytes:   ctx.Host(),
		pathBytes:   ctx.Path(),
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

// defaultMaxStreamingBufferBytes is the fallback cap on total bytes held
// in live streaming tee buffers when neither max_streaming_buffer_bytes
// nor GOMEMLIMIT is configured. When GOMEMLIMIT is set, the cap is
// derived at startup as 5% of the runtime soft memory limit (see
// config.ResolveMaxStreamingBufferBytes). Each tee buffer is also
// individually capped at maxResponseBytes (see teeStreamToClient), so
// the worst case is concurrency × maxResponseBytes = 32 × 4 MiB =
// 128 MiB. This fallback (64 MiB) is half the theoretical worst case,
// so the global cap triggers before all 32 slots fill with full buffers.
const defaultMaxStreamingBufferBytes int64 = 64 << 20 // 64 MiB

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
	Err        error
	fastResp   *fasthttp.Response // non-nil when the response is kept alive for CopyTo
	Header     headerLookup
	Body       []byte
	StatusCode int
}

// Handler is the caching HTTP handler. It wraps an upstream
// fasthttp.RequestHandler (the origin pool) and a storage.Store.
//
// maxRecorderCap bounds the backing array retained by the recorder pool.
// Recorders that grew past this on a large response are discarded so the
// pool never pins a transiently oversized buffer across GC cycles.
type Handler struct {
	refreshMetrics observability.RefreshMetricsForRoute
	// StreamingFallbackInc is incremented when a cacheable miss falls
	// back to synchronous buffering because the streaming memory cap
	// was exceeded; nil-safe.
	StreamingFallbackInc interface{ Inc() }
	// FetchShedInc is incremented when a foreground origin fetch sheds
	// after waiting fetchWaitTimeout for a fetch-semaphore slot; nil-safe.
	FetchShedInc interface{ Inc() }
	store        storage.Store
	flight       singleflight.Group
	logger       observability.Logger
	fastClient   FastClient
	// StreamingBufferBytesSet updates the streaming buffer bytes gauge;
	// nil-safe. Polled by the engine's background metrics loop.
	StreamingBufferBytesSet interface{ Set(float64) }
	// VaryCapHits is incremented when a variant is rejected; nil-safe.
	VaryCapHits interface{ Inc() }
	upstream    fasthttp.RequestHandler
	done        chan struct{}
	revalSem    chan struct{} // bounds concurrent SWR background goroutines
	// peerFetch asks a peer for a cached object. Returns nil, nil on
	// peer miss; errors fall through to origin. Nil in single-node mode.
	peerFetch func(ctx context.Context, peer api.PeerInfo, key api.Key) (*api.Object, error)
	// peerPut forwards a freshly origin-fetched object to the owner
	// node so subsequent peer-fetches hit. Best-effort, fire-and-forget.
	// Nil in single-node and eventual modes.
	peerPut func(ctx context.Context, owner api.PeerInfo, obj *api.Object)
	// variantSets tracks the live variant store keys per primary key to
	// enforce MaxVariants cap. Entries are removed when the handler observes
	// their eviction via store probes on the cap path, on explicit Delete,
	// or when reserveVariantSlot detects the primary key has been evicted
	// by SIEVE and resets the set.
	// Protected by variantMu.
	variantSets     map[api.Key]map[api.Key]struct{}
	refreshSem      chan struct{}
	refreshRegistry *refreshRegistry
	fetchSem        chan struct{} // bounds concurrent foreground origin fetches
	scheduler       *RefreshScheduler
	policy          *KeyPolicy // pre-compiled cache key policy (query + Vary headers); nil = none
	refreshLimiter  *refreshRateLimiter
	// ownerFn returns the peer that owns a cache key and whether the key
	// is local to this node. Nil in single-node mode.
	ownerFn func(key api.Key) (owner api.PeerInfo, isLocal bool)
	// stripPrefix is removed from the start of the origin-bound request
	// URI (path+query) on every fetch/revalidate/refresh. The cache key
	// and all client-facing surfaces keep the original path. Nil means
	// no stripping (zero cost on routes without strip_prefix).
	stripPrefix []byte
	routeName   string
	// inflightStreams tracks in-progress streaming fetches for
	// singleflight dedup. The leader streams the origin response to
	// its client while buffering for the cache; followers wait on
	// the done channel and serve the buffered result.
	// Sharded by key hash: a sync.Map's read-map misses under high miss
	// rates allocate an entry node per miss and funnel all miss
	// goroutines through its internal locks; sharded maps do a direct
	// map insert under a per-shard mutex (0 allocs) with far less
	// contention.
	inflightStreams         inflightTable
	refreshWg               sync.WaitGroup
	revalWg                 sync.WaitGroup // tracks in-flight SWR goroutines for shutdown
	defaultTTL              time.Duration  // operator fallback when origin sends no freshness
	defaultSIE              time.Duration  // operator-level stale-if-error floor
	maxObjectSize           int64          // skip storage for responses larger than this; 0 = no limit
	maxStreamingBufferBytes int64
	// maxStreamingBufferBytes caps total streaming buffer memory. 0 means
	// defaultMaxStreamingBufferBytes.
	streamingBufferBytes atomic.Int64
	// streamingBufferBytes tracks the total bytes currently held in live
	// streaming tee buffers across all concurrent streamMissTee calls.
	// When it exceeds maxStreamingBufferBytes, new misses fall back to
	// the synchronous buffered path to prevent OOMKill under slow-origin
	// conditions (see status-0-investigation.md).
	refreshPersistCycles int
	refreshMinScore      int64
	refreshTimeout       time.Duration
	refreshMargin        time.Duration
	defaultSWR           time.Duration // operator-level stale-while-revalidate floor
	jitterPercent        int
	maxResponseBytes     int64         // hard cap on body buffering; 0 = defaultMaxResponseBytes
	overrideTTL          time.Duration // operator override; wins over origin max-age/Expires when > 0
	refreshMinHits       int
	fetchTimeout         time.Duration // bounds total origin fetch time; 0 = defaultFetchTimeout
	fetchWaitTimeout     time.Duration // bounds the fetch-semaphore wait; 0 = defaultFetchWaitTimeout
	negativeTTL          time.Duration
	closeOnce            sync.Once
	variantMu            sync.Mutex
	stayinAlive          bool
	// logCacheKeys gates SetUserValue("cacheKey") — the value is only
	// read by the access-log sampler (DataPlaneMetrics.shouldLogAccess).
	logCacheKeys   bool
	allowSetCookie bool // when false (default), Set-Cookie blocks caching
	// Refresh-before-expiry fields. When refreshBeforeExpiry is true,
	// a background scheduler fires conditional revalidation at
	// TTL - margin, keeping objects perpetually fresh.
	refreshBeforeExpiry  bool
	refreshReactiveFirst bool
}

// HandlerConfig configures a cache Handler.
type HandlerConfig struct {
	// StreamingFallback, if non-nil, is incremented when a cacheable
	// miss falls back to synchronous buffering because the streaming
	// memory cap was exceeded.
	StreamingFallback interface{ Inc() }
	// FetchShed, if non-nil, is incremented when a foreground origin fetch
	// sheds after waiting fetch_wait_timeout for a fetch-semaphore slot.
	FetchShed interface{ Inc() }
	Store     storage.Store
	Logger    observability.Logger
	// FastClient is used by doFetch to fetch from the origin via
	// fasthttp. The response is captured directly in a pooled
	// *fasthttp.Response, eliminating intermediate header.Map and
	// body buffer allocations.
	FastClient FastClient
	// StripPrefix, when non-empty, is removed from the start of the
	// request URI before fetching from the origin. The cache key and
	// every client-facing surface keep the original path (see
	// config.RouteRequest.StripPrefix). Empty means no stripping.
	StripPrefix string
	// VaryCapHits, if non-nil, is incremented when a variant is rejected
	// because MaxVariants is exceeded.
	VaryCapHits interface{ Inc() }
	// StreamingBufferBytes, if non-nil, is set to the current total
	// bytes held in live streaming tee buffers. Polled by the engine's
	// background metrics loop.
	StreamingBufferBytes interface{ Set(float64) }
	// PeerPut, if non-nil, forwards a freshly origin-fetched object to
	// the key's owner node via a write-to-owner RPC. Used in strong
	// cluster mode so a non-owner that misses both locally and at the
	// owner still delivers the object to the owner for future
	// peer-fetches. Best-effort, fire-and-forget. Nil in single-node
	// and eventual modes.
	PeerPut func(ctx context.Context, owner api.PeerInfo, obj *api.Object)
	// PeerFetch, if non-nil, is called on a miss when OwnerFn reports
	// the key is owned by a remote peer. Returns nil, nil on peer miss;
	// errors are treated as misses (origin fallback, logged at debug).
	PeerFetch func(ctx context.Context, peer api.PeerInfo, key api.Key) (*api.Object, error)
	Upstream  fasthttp.RequestHandler
	// OwnerFn, if non-nil, enables cluster-aware routing. It returns the
	// peer that owns a cache key and whether the key is local. When nil,
	// the handler operates in single-node mode: every miss goes to origin.
	OwnerFn func(key api.Key) (owner api.PeerInfo, isLocal bool)
	// Policy, when non-nil, encodes cache key construction rules for this
	// route: query param stripping/keeping/prefix/empty/dedup and Vary
	// header exclusion. nil means no query/header policy.
	Policy *KeyPolicy
	// RefreshMetrics records background refresh activity. Nil when the
	// feature is disabled or metrics are not configured.
	RefreshMetrics *observability.RefreshMetrics
	// RouteName labels refresh metrics. Set from the route's config name.
	RouteName string
	// DefaultSWR is applied to every stored object when the origin does not
	// send stale-while-revalidate. Zero leaves the object at origin semantics.
	DefaultSWR time.Duration
	// RefreshMinScore is the minimum refresh priority score (staleHits ×
	// BodySize) required for re-scheduling. Zero disables the score gate.
	RefreshMinScore int64
	// MaxStreamingBufferBytes caps the total bytes held in live streaming
	// tee buffers across all concurrent miss-fetches on this route. When
	// exceeded, new cacheable misses fall back to synchronous buffering.
	// Zero (default) applies a safe built-in limit (64 MiB).
	MaxStreamingBufferBytes int64
	// MaxFetchConcurrency bounds the number of concurrent foreground
	// origin fetches. When the limit is reached, additional fetches wait
	// up to FetchWaitTimeout for a slot and then shed (503 + Retry-After,
	// or stale content when a stale object exists).
	// Zero (default) applies a safe built-in limit (32).
	MaxFetchConcurrency int
	// MaxResponseBytes is a hard limit on the amount of response body
	// data buffered in memory during an upstream fetch. When exceeded the
	// fetch is aborted and the client receives a 502. This is distinct
	// from MaxObjectSize, which only prevents storage after the body has
	// already been fully buffered. Zero (default) applies a safe built-in
	// limit (4 MiB).
	MaxResponseBytes int64
	// MaxObjectSize, when > 0, skips caching for responses whose body
	// exceeds this size. The response is still proxied to the client.
	// Zero = no limit.
	MaxObjectSize int64
	// NegativeTTL enables caching of 404/405/410/501 responses.
	NegativeTTL time.Duration
	// DefaultSIE is applied to every stored object when the origin does not
	// send stale-if-error. Zero disables SIE fallback for this route.
	DefaultSIE time.Duration
	// OverrideTTL, when > 0, forces bouine's internal cache TTL to this
	// value regardless of the upstream's Cache-Control/Expires headers.
	// The upstream's response headers are forwarded unaltered; only
	// the storage lifetime seen by bouine's freshness engine changes.
	// RFC 9111 boolean directives (no-store, private, no-cache,
	// must-revalidate) are always honoured; OverrideTTL only replaces
	// the numeric freshness lifetime.
	OverrideTTL time.Duration
	// DefaultTTL is the operator-configured TTL used when the origin sends
	// no explicit freshness (no max-age, no Expires, no Last-Modified).
	// Zero means fall back to heuristic or treat as uncacheable.
	DefaultTTL time.Duration
	// JitterPercent adds random ±N% to TTLs (0–50). 0 disables.
	JitterPercent int
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
	// FetchTimeout bounds the total time for an origin fetch (header +
	// body). When exceeded, the fetch is aborted and the caller receives
	// a fetchResult error. Zero (default) applies a safe built-in limit
	// (60s). This replaces the blanket WriteTimeout on the data plane.
	FetchTimeout time.Duration
	// FetchWaitTimeout bounds how long a foreground miss may wait for
	// an origin-fetch semaphore slot before shedding. On expiry the
	// request serves stale when a stale object exists, or returns 503
	// with Retry-After. Zero (default) applies a safe built-in limit
	// (100ms). Negative values are rejected by config validation.
	// Like fetch_timeout, this is a route-level cache setting.
	FetchWaitTimeout time.Duration
	// RefreshMaxRPS caps background refresh fetches per second per route.
	// Zero means no limit.
	RefreshMaxRPS int
	// RefreshReactiveFirst skips proactive refresh for new objects, relying
	// on SWR to promote popular objects. Requires StaleWhileRevalidate > 0
	// and RefreshMinHits > 0.
	RefreshReactiveFirst bool
	// StayinAlive enables emergency stale mode: serve cached objects
	// indefinitely when the upstream is unreachable or returning 5xx.
	StayinAlive bool
	// LogCacheKeys gates the SetUserValue("cacheKey") store on the
	// miss/revalidate/bypass paths. The value is only read by the
	// access-log sampler; when no access logger is configured the store
	// is a wasted per-request allocation (api.Key boxed into any).
	LogCacheKeys bool
	// RefreshBeforeExpiry enables proactive background conditional
	// revalidation. A background timer fires at TTL - margin.
	RefreshBeforeExpiry bool
	// AllowSetCookie controls caching of responses with Set-Cookie.
	// Default (false): Set-Cookie in the response blocks caching
	// unconditionally, matching nginx's safe default and preventing
	// session-cookie replay across users. When true: caching is
	// permitted per RFC 9111, but Set-Cookie is stripped from the
	// stored object so subsequent HITs do not replay another user's
	// cookies.
	AllowSetCookie bool
}

// FastClient performs an origin fetch using fasthttp, returning a
// pooled *fasthttp.Response. The caller is responsible for releasing
// the response via fasthttp.ReleaseResponse. The request is a
// fasthttp.RequestCtx (pooled) with method, URI, and headers set by
// the caller. The context is used for timeout/cancellation.
type FastClient interface {
	Do(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response) error
	// DoDeadline performs the fetch bounded by an absolute deadline,
	// enforced via the connection read/write deadlines (kernel-level)
	// rather than a context timer. It is the preferred method for
	// foreground fetches: it needs no context.WithTimeout allocation
	// and no goroutine, and unlike Do with a context whose cancellation
	// is not observed mid-fetch, the deadline is actually enforced on
	// production transports.
	DoDeadline(req *fasthttp.Request, resp *fasthttp.Response, deadline time.Time) error
}

// acquireFetchSlot takes a fetch-semaphore slot on the foreground miss
// path, bounded by fetchWaitTimeout. The happy path is a non-blocking
// send (zero allocs — the miss-path alloc budget must not move); the
// timer is created only when the semaphore is momentarily full. When
// the bound expires the fetch sheds with ErrFetchShed and the caller
// maps it to stale-on-shed or 503 + Retry-After (issue #562). The
// previous shape parked here forever with a dead cancellation arm
// (context.Background never fires), so a slow-origin event piled
// goroutines without bound.
//
// Fetch_timeout deliberately still starts after this returns: it bounds
// the fetch itself, not the queue (pinned by
// TestDoFetchTimeoutStartsAfterSemaphore).
func (h *Handler) acquireFetchSlot() error {
	select {
	case h.fetchSem <- struct{}{}:
		return nil
	default:
	}
	timer := time.NewTimer(h.fetchWaitTimeout)
	defer timer.Stop()
	select {
	case h.fetchSem <- struct{}{}:
		return nil
	case <-timer.C:
		if h.FetchShedInc != nil {
			h.FetchShedInc.Inc()
		}
		return fmt.Errorf("origin fetch queue wait exceeded %s: %w", h.fetchWaitTimeout, ErrFetchShed)
	}
}

// doFastFetch invokes the fast client with the handler's fetch
// timeout as an absolute deadline. When no timeout is configured
// (only possible via direct Handler field mutation — NewHandler
// always defaults it), it falls back to an undeadlined context Do.
func (h *Handler) doFastFetch(req *fasthttp.Request, resp *fasthttp.Response) error {
	if h.fetchTimeout > 0 {
		return h.fastClient.DoDeadline(req, resp, time.Now().Add(h.fetchTimeout))
	}
	return h.fastClient.Do(context.Background(), req, resp)
}

// strippedURI returns uri with the route's strip_prefix removed from the
// start. Used for origin-bound request URIs only: the cache key, ban
// matching, and Location resolution all keep the original path (config
// contract in config.RouteRequest.StripPrefix). Stripping only happens on
// a path boundary: when the remainder is empty ("/" is sent instead) or
// starts with "/" or "?". A mid-segment match ("/api/v1x" vs "/api/v1")
// passes through unchanged rather than producing a non-absolute
// request-line. Nil prefix returns uri unchanged.
func (h *Handler) strippedURI(uri []byte) []byte {
	if len(h.stripPrefix) == 0 || !bytes.HasPrefix(uri, h.stripPrefix) {
		return uri
	}
	trimmed := uri[len(h.stripPrefix):]
	switch {
	case len(trimmed) == 0:
		return []byte("/")
	case trimmed[0] == '/':
		return trimmed
	case trimmed[0] == '?':
		return append([]byte("/"), trimmed...)
	default:
		return uri
	}
}

// NewHandler creates a caching handler.
//
//nolint:funlen // 81 lines: initialization is inherently sequential
func NewHandler(cfg HandlerConfig) *Handler {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	h := &Handler{
		upstream:                cfg.Upstream,
		fastClient:              cfg.FastClient,
		stripPrefix:             []byte(cfg.StripPrefix),
		store:                   cfg.Store,
		logger:                  cfg.Logger,
		negativeTTL:             cfg.NegativeTTL,
		jitterPercent:           cfg.JitterPercent,
		stayinAlive:             cfg.StayinAlive,
		logCacheKeys:            cfg.LogCacheKeys,
		defaultTTL:              cfg.DefaultTTL,
		overrideTTL:             cfg.OverrideTTL,
		defaultSWR:              cfg.DefaultSWR,
		defaultSIE:              cfg.DefaultSIE,
		variantSets:             make(map[api.Key]map[api.Key]struct{}),
		VaryCapHits:             cfg.VaryCapHits,
		StreamingBufferBytesSet: cfg.StreamingBufferBytes,
		StreamingFallbackInc:    cfg.StreamingFallback,
		FetchShedInc:            cfg.FetchShed,
		ownerFn:                 cfg.OwnerFn,
		peerFetch:               cfg.PeerFetch,
		peerPut:                 cfg.PeerPut,
		allowSetCookie:          cfg.AllowSetCookie,
		maxObjectSize:           cfg.MaxObjectSize,
		maxResponseBytes:        cfg.MaxResponseBytes,
		policy:                  cfg.Policy,
		refreshBeforeExpiry:     cfg.RefreshBeforeExpiry,
		refreshMargin:           cfg.RefreshMargin,
		refreshTimeout:          cfg.RefreshTimeout,
		refreshMinHits:          cfg.RefreshMinHits,
		refreshPersistCycles:    cfg.RefreshPersistCycles,
		refreshMinScore:         cfg.RefreshMinScore,
		refreshReactiveFirst:    cfg.RefreshReactiveFirst,
		routeName:               cfg.RouteName,
		done:                    make(chan struct{}),
	}
	if h.maxResponseBytes == 0 {
		h.maxResponseBytes = defaultMaxResponseBytes
	}
	h.maxStreamingBufferBytes = cfg.MaxStreamingBufferBytes
	if h.maxStreamingBufferBytes <= 0 {
		h.maxStreamingBufferBytes = defaultMaxStreamingBufferBytes
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
	h.fetchWaitTimeout = cfg.FetchWaitTimeout
	if h.fetchWaitTimeout <= 0 {
		h.fetchWaitTimeout = defaultFetchWaitTimeout
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
	h.inflightStreams = newInflightTable()

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
	req.Header.SetMethod(ri.GetMethod())
	req.SetRequestURI(string(h.strippedURI([]byte(ri.GetURI()))))
	req.Header.SetHost(ri.GetHost())
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

// StreamingBufferBytes returns the total bytes currently held in live
// streaming tee buffers across all concurrent streamMissTee calls.
// The engine polls this to update the bouine_streaming_buffer_bytes
// Prometheus gauge.
func (h *Handler) StreamingBufferBytes() int64 {
	return h.streamingBufferBytes.Load()
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
	if isInvalidatingBytes(ctx.Method()) {
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
			h.triggerBgRevalidate(ri, key, disp.Object)
		}
	case Miss:
		if h.logCacheKeys {
			ctx.SetUserValue("cacheKey", key)
		}
		ri := requestInfoFromCtx(ctx)
		h.handleCacheMiss(ctx, primaryKey, key, obj, now, src, ri)
	case Revalidate:
		if h.logCacheKeys {
			ctx.SetUserValue("cacheKey", key)
		}
		if !h.isOwnerOrUnmanaged(key) {
			ri := requestInfoFromCtx(ctx)
			h.handleCacheMiss(ctx, primaryKey, key, obj, now, src, ri)
			return
		}
		ri := requestInfoFromCtx(ctx)
		h.revalidate(ctx, primaryKey, key, disp.Object, now, src, ri)
	case Bypass:
		if h.logCacheKeys {
			ctx.SetUserValue("cacheKey", key)
		}
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
	if obj == nil || obj.VaryValue == "" {
		return primaryKey, primaryKey, obj, src
	}
	vk := VariantKeyFast(primaryKey, obj.VaryValue, &ctx.Request.Header, h.policy)
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

// TriggerBgRevalidateFromFastPath serves as the fast-path StaleHit SWR
// hook: it materializes the RawRequest fields (which alias the h1parser
// connection's read buffer and are invalidated as soon as this request's
// iteration ends) into an owned RequestInfo, then triggers the same
// background revalidation the miss path uses (triggerBgRevalidate).
// Non-blocking: the refresh limit / revalSem backpressure inside
// triggerBgRevalidate applies unchanged.
func (h *Handler) TriggerBgRevalidateFromFastPath(req *api.RawRequest, key api.Key, stale *api.Object) {
	if !h.isOwnerOrUnmanaged(key) {
		return
	}
	// Materialize before the request buffer is reused by the next
	// keep-alive request on the same connection.
	ri := requestInfoFromRaw(req)
	h.triggerBgRevalidate(ri, key, stale)
}

// requestInfoFromRaw builds an owned RequestInfo from a RawRequest.
// The RawRequest's string fields alias the h1parser read buffer, so all
// strings are copied into owned memory — the fast-path equivalent of
// the materialize-before-escape rule the SWR goroutine follows in
// triggerBgRevalidate.
func requestInfoFromRaw(req *api.RawRequest) RequestInfo {
	uri := req.Path
	if req.Query != "" {
		uri += "?" + req.Query
	}
	method := req.Method
	if method == "HEAD" {
		method = "GET"
	}
	ri := RequestInfo{
		Method: method,
		URI:    uri,
		Host:   req.Host,
		Path:   req.Path,
		TLS:    req.Scheme == "https",
	}
	ri.Header = header.NewMap(req.NHeaders)
	for i := 0; i < req.NHeaders; i++ {
		ri.Header.Set(req.Headers[i].Key, req.Headers[i].Value)
	}
	return ri
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
	// Read conditional headers directly from ctx.Request.Header via Peek
	// to avoid building a full RequestInfo (headerFromCtx allocates a
	// header.Map with string() for every request header).
	inm := ctx.Request.Header.Peek(header.IfNoneMatch)
	if len(inm) > 0 {
		if obj.ETag != "" && etagMatch(string(inm), obj.ETag) {
			if obj.ETag != "" {
				ctx.Response.Header.SetCanonical(header.S2b(header.ETag), header.S2b(obj.ETag))
			}
			ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("HIT"))
			ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(src)))
			ctx.SetStatusCode(fasthttp.StatusNotModified)
			return true
		}
		return false
	}
	ims := ctx.Request.Header.Peek(header.IfModifiedSince)
	if len(ims) > 0 {
		imsTime := parseHTTPDate(string(ims))
		if imsTime.IsZero() {
			return false
		}
		if !obj.LastModified.IsZero() && !obj.LastModified.After(imsTime) {
			if obj.ETag != "" {
				ctx.Response.Header.SetCanonical(header.S2b(header.ETag), header.S2b(obj.ETag))
			}
			ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("HIT"))
			ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(src)))
			ctx.SetStatusCode(fasthttp.StatusNotModified)
			return true
		}
		if obj.LastModified.IsZero() {
			if d := obj.Header.Get(header.Date); d != "" {
				if dt := parseHTTPDate(d); !dt.IsZero() && !dt.After(imsTime) {
					if obj.ETag != "" {
						ctx.Response.Header.SetCanonical(header.S2b(header.ETag), header.S2b(obj.ETag))
					}
					ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("HIT"))
					ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(src)))
					ctx.SetStatusCode(fasthttp.StatusNotModified)
					return true
				}
			}
		}
	}
	return false
}

// handleBypass handles the BYPASS path (requests with no-store or
// no-cache directives). It delegates to handleBypassFast, which fetches
// the origin response via FastClient and writes it directly to the
// *fasthttp.RequestCtx, then overwrites bouine's attribution headers
// (X-Cache, X-Cache-Source) so an origin-supplied value cannot spoof
// the source metric label or X-Cache result.
func (h *Handler) handleBypass(ctx *fasthttp.RequestCtx) {
	reqCC := ParseCacheControlBytes(ctx.Request.Header.Peek(header.CacheControl))
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
	h.streamBypass(ctx, "BYPASS")
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
	if !bytes.Equal(ctx.Method(), []byte("HEAD")) {
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
	spanCtx, span := tracing.StartSpan(ctx, "bouine.origin")
	defer span.End()

	select {
	case h.fetchSem <- struct{}{}:
		defer func() { <-h.fetchSem }()
	default:
	}

	// Deadline-based timeout: transport.Client.Do maps ctx.Deadline() to
	// fasthttp's kernel-level connection deadlines. The previous
	// context.WithCancel + time.AfterFunc never reached production transports
	// (WithCancel has no deadline, so transport.Client.Do fell back to a
	// fixed 60s DoTimeout) — fetch_timeout was silently ignored in
	// production. Unlike the foreground paths (doFastFetch — no context
	// at all), this background path uses context.WithTimeout instead of
	// DoDeadline because background callers hold a ctx they cancel on
	// handler shutdown, and that cancellation must reach the transport.
	if h.fetchTimeout > 0 {
		var cancel context.CancelFunc
		spanCtx, cancel = context.WithTimeout(spanCtx, h.fetchTimeout)
		defer cancel()
	}

	resp := fasthttp.AcquireResponse()

	if err := h.fastClient.Do(spanCtx, req, resp); err != nil {
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
	// Exact-size copy: the body may be stored in the cache (background
	// revalidation fills), and the hot tier pins slice slack for the
	// object's lifetime.
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
	// Try to become the streaming leader for this key.
	// If another request is already streaming, wait for its buffered result.
	inflight := &inflightStream{done: make(chan struct{})}
	if actual, loaded := h.inflightStreams.loadOrStore(lookupKey, inflight); loaded {
		// Follower: wait for the leader's buffered result.
		existing := actual
		<-existing.done
		res := existing.res
		if res.Err != nil {
			if errors.Is(res.Err, ErrFetchShed) {
				h.writeShed503(ctx, "MISS")
				return
			}
			ctx.Error("upstream error", fasthttp.StatusBadGateway)
			ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
			ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
			return
		}
		// Write the buffered result without re-storing (leader already stored).
		h.writeBufferedResult(ctx, res, primaryKey, ri)
		return
	}
	// Leader: remove from inflight map when done.
	defer h.inflightStreams.delete(lookupKey)
	h.streamMiss(ctx, primaryKey, ri, inflight)
}

// writeShed503 writes the 503 + Retry-After response for a shed request
// (the fetch-semaphore wait bound expired and no stale object is in
// scope). Distinct from the 502 origin-failure mapping: the origin was
// never contacted, so clients should retry shortly (issue #562). No
// X-Cache-Source is set — the origin was never reached.
func (h *Handler) writeShed503(ctx *fasthttp.RequestCtx, xCache string) {
	ctx.Error("origin fetch queue full", fasthttp.StatusServiceUnavailable)
	ctx.Response.Header.SetCanonical(header.S2b(header.RetryAfter), header.S2b("1"))
	ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b(xCache))
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
// writeBufferedResult writes a fetchResult to the client without
// storing (the leader already stored it). Used by singleflight
// followers in the streaming miss path.
func (h *Handler) writeBufferedResult(
	ctx *fasthttp.RequestCtx,
	res fetchResult,
	_ api.Key,
	_ RequestInfo,
) {
	dst := &ctx.Response.Header
	res.Header.CopyToFastHTTP(dst)
	dst.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
	dst.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
	resMap := res.Header.ToMap()
	if resMap.Get(header.Age) == "" {
		dst.SetCanonical(header.S2b(header.Age), header.S2b("0"))
	}
	ctx.SetStatusCode(res.StatusCode)
	if !bytes.Equal(ctx.Method(), []byte("HEAD")) {
		ctx.Response.SetBodyRaw(res.Body)
	}
}

func (h *Handler) fetchAndStoreStayinAlive(ctx *fasthttp.RequestCtx, lookupKey, primaryKey api.Key, stale *api.Object, now time.Time, src api.Source, ri RequestInfo) {
	res := h.collapsedFetch(ctx, lookupKey)
	defer releaseFetchResult(res)
	if res.Err != nil {
		if errors.Is(res.Err, ErrFetchShed) {
			// Shed — the fetch queue was full for fetchWaitTimeout. The
			// shed counter (incremented at the shed point) carries the
			// signal; keep this at Debug so a shed storm cannot INFO-spam.
			h.logger.Debug("stayin-alive: fetch wait timeout, serving stale",
				"key", lookupKey)
		} else {
			h.logger.Info("stayin-alive: upstream unreachable, serving stale indefinitely",
				"error", res.Err, "key", lookupKey)
		}
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
	revalReq.Header.SetMethodBytes(ctx.Method())
	revalReq.SetRequestURIBytes(h.strippedURI(ctx.RequestURI()))
	revalReq.Header.SetHostBytes(ctx.Host())
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
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
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
// stale.Header is cloned by CloneForRefresh before mutation: it is shared
// with any other goroutine that looked up the same object, and
// MergeHeaders304's writes would race with their reads.
func (h *Handler) refreshFrom304(stale *api.Object, res fetchResult, now time.Time) *api.Object {
	refreshed := stale.CloneForRefresh()
	refreshed.StoredAt = now
	// Reset Hits to 0 for the new TTL window. Object.Hits is a SIEVE
	// eviction signal; the per-window popularity gate uses windowHits
	// from the store, not Object.Hits.
	refreshed.Hits = 0
	MergeHeaders304(refreshed, res.Header.ToMap())
	// Recompute HasDate in case the 304 response added or changed Date.
	refreshed.HasDate = refreshed.Header.Has(header.Date)
	refreshed.VaryValue = refreshed.Header.Get(header.Vary)
	// Recompute CacheControl string and parsed TTL from the updated headers.
	refreshed.CacheControl = refreshed.Header.Get(header.CacheControl)
	newCC := ParseCacheControl(refreshed.CacheControl)
	refreshed.RespNoCache = newCC.NoCache
	refreshed.RespMustRevalidate = newCC.MustRevalidate || newCC.ProxyRevalidate
	if ttl, ok := FreshnessLifetime(newCC, refreshed.Header.Get); ok {
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
func (h *Handler) triggerBgRevalidate(ri RequestInfo, key api.Key, stale *api.Object) {
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
	// Materialize the request fields now: ri's []byte fields alias the
	// *fasthttp.RequestCtx's internal buffers (request.go), which are
	// reused by the next keep-alive request the moment our handler
	// returns. The background goroutine must not read them after that.
	// ri.Header is an owned header.Map (headerFromCtx copies) — safe as is.
	bgReq := RequestInfo{
		Method: ri.GetMethod(),
		URI:    ri.GetURI(),
		Host:   ri.GetHost(),
		Path:   ri.GetPath(),
		Header: ri.Header,
		TLS:    ri.TLS,
	}
	// Detach from the client's context so the background fetch is not
	// cancelled when the response is sent, but wrap it in a cancellable
	// context so Close can signal shutdown.
	//
	// Use context.Background, not context.WithoutCancel(ctx): the
	// RequestCtx is handed back to fasthttp's worker the moment our
	// handler returns and is Reset for the next request. Retaining it
	// as a context parent keeps a live reference whose Value lookups
	// race with that reset (SpanFromContext on the derived context
	// walks into the RequestCtx's userdata map).
	bgCtx, bgCancel := context.WithCancel(context.Background())
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
// the same key. The stored/registry URI stays original; only the
// origin-bound request URI is stripped.
func (h *Handler) doBackgroundRevalidate(ctx context.Context, ri RequestInfo, key api.Key, stale *api.Object) {
	revalReq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(revalReq)
	revalReq.Header.SetMethod(ri.GetMethod())
	revalReq.SetRequestURI(string(h.strippedURI([]byte(ri.GetURI()))))
	revalReq.Header.SetHost(ri.GetHost())
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

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethodBytes(ctx.Method())
	req.SetRequestURIBytes(h.strippedURI(ctx.RequestURI()))
	req.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		req.Header.AddBytesKV(k, v)
	}
	if ctx.Request.Body() != nil {
		body := ctx.Request.Body()
		req.SetBodyRaw(body)
	}
	tracing.InjectFastHTTP(fetchCtx, req)

	if err := h.doFastFetch(req, resp); err != nil {
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
			varyHeader = obj.VaryValue
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
	fetchCtx, span := tracing.StartSpan(context.Background(), "bouine.origin")
	defer span.End()

	if err := h.acquireFetchSlot(); err != nil {
		return fetchResult{Err: err}
	}
	defer func() { <-h.fetchSem }()

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
	req.SetRequestURIBytes(h.strippedURI(ctx.RequestURI()))
	req.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		req.Header.AddBytesKV(k, v)
	}
	// Inject W3C TraceContext.
	tracing.InjectFastHTTP(fetchCtx, req)

	if err := h.doFastFetch(req, resp); err != nil {
		fasthttp.ReleaseResponse(resp)
		tracing.RecordError(span, err)
		return fetchResult{Err: fmt.Errorf("origin fetch: %w", err)}
	}

	// Check for max response bytes.
	if h.maxResponseBytes > 0 && int64(len(resp.Body())) > h.maxResponseBytes {
		fasthttp.ReleaseResponse(resp)
		return fetchResult{Err: fmt.Errorf("upstream response exceeds %d bytes", h.maxResponseBytes)}
	}

	// Copy the body into an exact-size slice. The pooled response is
	// kept alive in fetchResult.fastResp so writeAndMaybeStore can use
	// CopyTo for zero-normalization header copying. It is released by
	// releaseFetchResult after all singleflight waiters have finished.
	// The exact-size copy is load-bearing: this body is stored in the
	// cache on cacheable revalidate/stayin-alive fills, and the hot tier
	// pins whatever slack the slice carries for the object's lifetime.
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
	dateStr := resMap.Get(header.Date)
	hasDate := dateStr != ""
	if hasDate {
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

	// res.Body is already an independently-owned copy: all production
	// callers (doFetchFast, streamMissBuffered, streamMissTee,
	// doFetchBg, maybeStorePostResponseFast) copy the body from the
	// pooled fasthttp.Response buffer before calling buildObject.
	// Using res.Body directly avoids a redundant make+copy per miss.
	obj := &api.Object{
		Key:                key,
		StatusCode:         res.StatusCode,
		Header:             resMap,
		Body:               res.Body,
		BodySize:           int64(len(res.Body)),
		StoredAt:           now,
		TTL:                ttl,
		ETag:               resMap.Get(header.ETag),
		CacheControl:       ccHeader,  // Lead 1: pre-stored, avoids re-parsing on every hit
		OriginAge:          originAge, // Lead 3: pre-stored, avoids re-parsing on the read path
		HasDate:            hasDate,
		VaryValue:          resMap.Get(header.Vary),
		RespNoCache:        respCC.NoCache,
		RespMustRevalidate: respCC.MustRevalidate || respCC.ProxyRevalidate,
	}
	// Stamp internal headers for ban predicate matching. These are
	// stripped before serving to clients (see serveObject).
	// SetEntryRaw skips interning: paths and hosts are unique per object,
	// so the intern lookup never hits, costs an allocation per attempt,
	// and serializes all miss goroutines on the global intern-table
	// mutex. The strings are freshly materialized by GetPath/GetHost
	// (string conversions owned by this object) and only read after
	// storage.
	obj.Header.SetEntryRaw(header.XBouinePath, ri.GetPath())
	obj.Header.SetEntryRaw(header.XBouineHost, ri.GetHost())
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
		var clBuf [16]byte
		clStr := strconv.AppendInt(clBuf[:0], obj.BodySize, 10)
		obj.Header.Set(header.ContentLength, string(clStr))
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
	obj.HasNoCacheFields = respCC.NoCacheFields != ""

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

// isInvalidatingBytes is the zero-allocation variant of isInvalidating,
// accepting the raw []byte from fasthttp's ctx.Method() directly.
func isInvalidatingBytes(method []byte) bool {
	return !bytes.Equal(method, []byte("GET")) &&
		!bytes.Equal(method, []byte("HEAD")) &&
		!bytes.Equal(method, []byte("OPTIONS"))
}
