// Package cache (handler.go) provides the HTTP handler that wires the
// cache engine into the request pipeline. It sits between the pipeline
// router and the origin pool handler:
//
//	client → listener → accesslog → metrics → router → CacheHandler → origin
//
// For every request the handler computes the cache key, looks up the
// store, and serves, revalidates, fetches, or passes through according
// to the Decision returned by Evaluate. POST/PUT/DELETE invalidate
// matching keys and pass through.
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
// 503 + Retry-After otherwise — distinct from the 502/504 origin-failure
// mapping (writeUpstreamError). The wait bound is the fix for the
// goroutine-pileup livelock (issue #562): previously the foreground paths parked on the semaphore
// with a dead cancellation arm and nothing ever un-parked them.
var ErrFetchShed = errors.New("origin fetch queue wait timeout")

// ErrStreamUnshareable is published to singleflight followers when the
// leader's response is delivered unbuffered (non-hinted SSE, Vary variant
// overflow): nothing is ever buffered, so there is no complete result to
// share. Followers fetch their own response instead — an unbuffered
// stream is per-connection by design and cannot be replayed from a
// buffer that does not exist (ADR-0042).
var ErrStreamUnshareable = errors.New("streaming response not shareable")

// StripRequestURI removes prefix from the start of uri on a path
// boundary, passing a mid-segment match ("/api/v1x" vs "/api/v1")
// through unchanged rather than producing a non-absolute request-line.
// A nil or empty prefix returns uri unchanged. Allocation-free for the
// common (trimmed or unchanged) cases.
//
// This is the single definition of strip-prefix boundary semantics for the
// whole binary: the cache handler applies it to origin-bound request URIs
// (cache keys, ban matching, X-Bouine-Path, and Location resolution keep
// the original path — config.RouteRequest.StripPrefix contract), and
// cmd/bouine/cmd's stripPrefixFastHTTP applies it when lowering
// static-route requests before the cache layer sees them.
// Stable.
func StripRequestURI(prefix, uri []byte) []byte {
	if len(prefix) == 0 || !bytes.HasPrefix(uri, prefix) {
		return uri
	}
	trimmed := uri[len(prefix):]
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

// defaultRewarmConcurrency bounds concurrent shed-refill (re-warm)
// goroutines per Handler. This is the post-purge recovery allowance: a
// bounded side pool that refills the store while foreground misses shed,
// so a surrogate-ban miss storm converges back to hits instead of
// pinning at the shed equilibrium. Deliberately much smaller than
// revalSem: each refill is a full origin GET, and the pool only exists
// under saturation — its whole point is to add a little extra origin
// load, bounded, exactly then.
const defaultRewarmConcurrency = 32

// ageHeader returns the Age header value string for a duration.
func ageHeader(d time.Duration) string {
	return strconv.Itoa(int(d.Seconds()))
}

// fetchResult is the outcome of an origin fetch, shared across collapsed
// requests. It must own its data: singleflight may hand it to several
// concurrent callers, so a live pointer into a pooled *fasthttp.Response
// would race with the pool reusing that response for another request.
type fetchResult struct {
	Err        error
	Header     headerLookup
	Body       []byte
	StatusCode int
	// Delay is the time from sending the origin request to receiving
	// the response headers (RFC 9111 §4.2.3 response_delay). It feeds
	// the corrected initial age of the stored/served response: a slow
	// origin leaves the response already aged on arrival. Zero on the
	// upstream-fallback path (in-process handler, no network).
	Delay time.Duration
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
	// RewarmFillInc is incremented when a shed foreground miss schedules
	// a bounded background refill of the store (the post-ban re-warm
	// allowance); nil-safe. Paired with FetchShedInc: sheds are no longer
	// lost refills, so a rising shed rate during a purge storm no longer
	// implies the hit ratio is pinned at the shed equilibrium.
	RewarmFillInc interface{ Inc() }
	// VaryDriftInc counts detected Vary-declaration drifts (ADR-0058),
	// where the stale resolver and its variants were purged; nil-safe.
	VaryDriftInc varyDriftInc
	store        storage.Store
	flight       singleflight.Group
	logger       observability.Logger
	fastClient   FastClient
	// rewarmSem bounds concurrent shed-refill (re-warm) goroutines. It
	// is deliberately NOT the foreground fetchSem: the refill exists to
	// clear the post-ban miss backlog, and sharing the foreground budget
	// would keep the re-warm starved by the very demand it is recovering
	// from. Bounded like revalSem so a storm cannot explode goroutines;
	// tracked by revalWg for shutdown draining.
	rewarmSem chan struct{}
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
	peerFetch func(ctx context.Context, peer api.PeerInfo, key api.Key, varyKey string) (*api.Object, error)
	// purgeBroadcast fans a data-plane invalidation out to peers; one
	// call per purged key. Nil in single-node mode.
	purgeBroadcast func(key api.Key)
	// onPeerVariantMismatch is called when servePeerHit rejects a
	// foreign-variant object. Nil in single-node mode.
	onPeerVariantMismatch func()
	// onResolverTwoStep is called when serveResolverPeerVariant completes
	// the ADR-0068 two-step resolution and serves the variant. Nil in
	// single-node mode.
	onResolverTwoStep func()
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
	// pathRewrite applies the request.path_rewrite regex to every
	// origin-bound request URI after strip_prefix. Nil on routes without
	// path_rewrite (zero cost). See PathRewrite for the hardening rules.
	pathRewrite *PathRewrite
	// uaBypass holds the compiled cache.bypass_on_user_agent patterns
	// (issue #771, ADR-0055); nil on pattern-less routes, making both
	// request-path gates (ServeRequest, FastPathHandler.TryHit) a
	// single nil check.
	uaBypass *uaBypass
	// reqHeaderSet sets (overrides) request headers on every origin-bound
	// fetch (request.header_set). nil = no-op.
	reqHeaderSet map[string]string
	// reqHeaderRemove skips the listed (lower-cased) request headers on
	// every origin-bound fetch (request.header_remove). nil = no-op.
	reqHeaderRemove map[string]bool
	// respHeaderSet sets response headers on every client-facing
	// response, after the stored headers are written (response.header_set).
	// nil = no-op.
	respHeaderSet map[string]string
	// respHeaderRemove deletes the listed response headers from every
	// client-facing response (response.header_remove). nil = no-op.
	respHeaderRemove map[string]bool // canonical name set for Del
	routeName        string
	poolName         string
	// inflightStreams tracks in-progress streaming fetches for
	// singleflight dedup. The leader streams the origin response to
	// its client while buffering for the cache; followers wait on
	// the done channel and serve the buffered result.
	// Sharded by key hash: a sync.Map's read-map misses under high miss
	// rates allocate an entry node per miss and funnel all miss
	// goroutines through its internal locks; sharded maps do a direct
	// map insert under a per-shard mutex (0 allocs) with far less
	// contention.
	// bypassCookieNames is the compiled matcher for
	// cache.bypass_on_cookie_names (issue #768): routes a request
	// carrying any listed cookie name around the cache, same contract
	// as bypassOnCookie, while requests with only unlisted cookies
	// participate normally. Empty scanner (no names configured) pays
	// a single bool read per request.
	inflightStreams   inflightTable
	bypassCookieNames cookieNameScanner
	// fp carries the route's freshness policy (fillPolicy): the TTL
	// ladder knobs shared by buildObject, the sync-mode fill cap, and
	// the 304-refresh override. A value, not a pointer, so zero-value
	// Handlers in tests get the zero policy exactly as before.
	fp                      fillPolicy
	refreshWg               sync.WaitGroup
	revalWg                 sync.WaitGroup // tracks in-flight SWR goroutines for shutdown
	maxObjectSize           int64          // skip storage for responses larger than this; 0 = no limit
	maxVariants             int            // cap on stored variants per primary key; DefaultMaxVariants when unset
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
	maxResponseBytes     int64 // hard cap on body buffering; 0 = defaultMaxResponseBytes
	refreshMinHits       int
	fetchTimeout         time.Duration // bounds total origin fetch time; 0 = defaultFetchTimeout
	fetchWaitTimeout     time.Duration // bounds the fetch-semaphore wait; 0 = defaultFetchWaitTimeout
	closeOnce            sync.Once
	variantMu            sync.Mutex
	stayinAlive          bool
	// syncCDC mirrors routes.*.cache.downstream_freshness (ADR-0063):
	// when true, client-facing serves carry a synthesized
	// CDN-Cache-Control (RFC 9213) capping downstream freshness at
	// bouine's own clock, and the origin's CDC-family lines are removed
	// (downstreamcc.go). Set once at NewHandler from HandlerConfig.
	syncCDC bool
	// logCacheKeys gates SetUserValue("cacheKey") — the value is only
	// read by the access-log sampler (DataPlaneMetrics.shouldLogAccess).
	logCacheKeys   bool
	allowSetCookie bool // when false (default), client-state headers (Set-Cookie, Set-Login) block caching
	// bypassOnCookie routes cookied requests entirely around the cache
	// (ADR-0054, cache.bypass_on_cookie): no lookup, no storage, no
	// in-flight sharing. False (default) keeps RFC 9111 semantics — the
	// cache-tests other-cookie optimal case.
	bypassOnCookie bool
	// Refresh-before-expiry fields. When refreshBeforeExpiry is true,
	// a background scheduler fires conditional revalidation at
	// TTL - margin, keeping objects perpetually fresh.
	refreshBeforeExpiry  bool
	refreshReactiveFirst bool
	// forwarded is the compiled request.forwarded policy (issue #769):
	// client-identity header injection on origin-bound fetches only.
	// Zero value = disabled (single bool read per fetch). Lives in the
	// value region with the other non-pointer fields (fieldalignment).
	forwarded ForwardedPolicy
}

// HandlerConfig configures a cache Handler.
// HandlerConfig is the per-route cache handler configuration.
//
//nolint:govet // fieldalignment: fields follow the config-file order
type HandlerConfig struct {
	// StreamingFallback, if non-nil, is incremented when a cacheable
	// miss falls back to synchronous buffering because the streaming
	// memory cap was exceeded.
	StreamingFallback interface{ Inc() }
	// FetchShed, if non-nil, is incremented when a foreground origin fetch
	// sheds after waiting fetch_wait_timeout for a fetch-semaphore slot.
	FetchShed interface{ Inc() }
	// RewarmFill, if non-nil, is incremented when a shed miss schedules a
	// bounded background store refill (the post-ban re-warm allowance).
	RewarmFill interface{ Inc() }
	Store      storage.Store
	Logger     observability.Logger
	// Negative resolves per-status negative-caching TTLs from the
	// route's negative_ttl policy (both forms normalized to one map by
	// the config layer). Nil disables negative caching.
	Negative *StatusTTL
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
	// PathRewrite, when non-nil, applies the route's request.path_rewrite
	// regex to every origin-bound request URI after StripPrefix. Nil
	// means no rewrite. Mutually exclusive with StripPrefix at the
	// config layer; validated and compiled by config.validatePathRewrite.
	PathRewrite *PathRewrite
	// BypassOnUserAgent routes requests whose User-Agent matches one
	// of the glob patterns entirely around the cache (issue #771,
	// ADR-0055): the same contract as BypassOnCookie, triggered by a
	// User-Agent match instead of Cookie presence. Patterns are
	// validated by config.validateBypassOnUserAgent (capped count and
	// length, `*` as the only wildcard, no lone `*`, no duplicates) and
	// compiled once at handler build. Default (nil): no UA-conditioned
	// behavior, RFC 9111 semantics unchanged.
	BypassOnUserAgent []string
	// VaryCapHits, if non-nil, is incremented when a variant is rejected
	// because the variant cap is exceeded.
	VaryCapHits interface{ Inc() }
	// VaryDrift, if non-nil, counts detected Vary-declaration drifts
	// (ADR-0058); wired to bouine_vary_drift_total.
	VaryDrift varyDriftInc
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
	// the key is owned by a remote peer. RequestKey carries the cache key
	// (primary or variant) and varyKey the computed Vary assertion: a
	// peer MUST NOT return an object whose Vary dimension set differs
	// from the one asserted (RFC 9111 §4.1 — variants are keyed per
	// selecting header set). Implementations assert this via the
	// PeerFetchRequest.VaryKey field; the handler re-verifies on receipt
	// and treats a mismatch as a miss. Returns nil, nil on peer miss;
	// errors are treated as misses (origin fallback, logged at debug).
	PeerFetch func(ctx context.Context, peer api.PeerInfo, key api.Key, varyKey string) (*api.Object, error)
	// PurgeBroadcast, if non-nil, fans a data-plane purge out to cluster
	// peers. Called once per key invalidated by an unsafe method
	// (POST/PUT/DELETE, RFC 9111 §4.4) — including Location/
	// Content-Location-derived keys — after the local purge, regardless
	// of the local owned result: in strong mode the owner holds the
	// object while the invalidating request may land on a non-owner, so
	// owned=false never means "nobody has it". Nil in single-node mode.
	// Implementations coalesce bursts (the cluster broadcaster batches
	// behind a 10 ms flush window), so per-request cost is one enqueue.
	PurgeBroadcast func(key api.Key)
	// OnPeerVariantMismatch, if non-nil, is called when the handler
	// rejects a peer-fetched object because its stored variant does
	// not select this request (RFC 9111 §4.1 assertion re-verified on
	// receipt). Wired to the peer-fetch variant-mismatch metric
	// (consumer side) by the engine; nil in single-node mode.
	OnPeerVariantMismatch func()
	// OnResolverTwoStep, if non-nil, is called when the handler completes
	// the ADR-0068 two-step variant resolution: it fetched the
	// metadata-only resolver from the primary key's owner, derived this
	// request's variant key from the learned Vary list, and served the
	// variant. Wired to the peer-fetch resolver-answers metric
	// (consumer side) by the engine; nil in single-node mode.
	OnResolverTwoStep func()
	Upstream          fasthttp.RequestHandler
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
	// PoolName is the route's upstream pool name, stamped onto fast-path
	// hit responses for the metrics middleware's upstream_pool label.
	// Empty for routes without a pool (static-file routes).
	PoolName string

	// RequestHeaderSet sets the listed request headers (name → value)
	// on every origin-bound fetch, overriding client values. Applied
	// before the fetch, bypass, revalidation, and the Vary variant
	// computation, so the rewritten value is what every downstream
	// surface sees (config.RouteRequest.HeaderSet contract). Header
	// names are canonicalized by fasthttp's setters at application.
	RequestHeaderSet map[string]string
	// RequestHeaderRemove removes the listed request headers from the
	// origin-bound fetch (config.RouteRequest.HeaderRemove contract).
	// Names are lower-cased at construction for O(1) skip checks.
	RequestHeaderRemove []string
	// BypassOnCookieNames routes requests carrying any cookie whose
	// name is in the list entirely around the cache — the same
	// contract as BypassOnCookie (no lookup, no storage, no
	// in-flight sharing), scoped to the listed names (issue #768).
	// Cookie names are matched case-insensitively on the name token
	// only, never on values or substrings. Requests carrying only
	// unlisted cookies participate in the cache per RFC 9111 — the
	// knob exists so ubiquitous analytics/consent cookies do not
	// force the blunt presence trigger.
	//
	// In-flight sharing is refused unconditionally for every cookied
	// request by collapseDenied (ADR-0054) regardless of this list:
	// a listed-cookie request can never receive another user's
	// in-flight body, and an unlisted-cookie request cannot either.
	BypassOnCookieNames []string
	// ResponseHeaderSet sets the listed response headers (name → value)
	// on every response this handler emits — hit, stale, revalidated,
	// miss, and bypass — after the stored headers are written
	// (config.RouteResponse.HeaderSet contract). Set replaces any
	// origin value for the same name.
	ResponseHeaderSet map[string]string
	// ResponseHeaderRemove removes the listed response headers from
	// every response this handler emits, after the stored headers are
	// written (config.RouteResponse.HeaderRemove contract).
	ResponseHeaderRemove []string
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
	// MaxVariants is the cap on stored variants per primary key
	// (cache.max_variants). Zero (default) applies DefaultMaxVariants.
	// RFC 9110 §12.5.5 — unbounded variants are a DoS vector, hence the cap.
	MaxVariants int
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
	// Forwarded injects client-identity headers (X-Forwarded-For/Proto/
	// Host, Via) on every origin-bound fetch (config.ForwardedConfig
	// contract, issue #769). Injection is append/set at outbound request
	// construction only — never the hit path, never the cache key, never
	// the stored RequestInfo headers. Zero value = no-op. Lives in the
	// value region with the other non-pointer fields (fieldalignment).
	Forwarded ForwardedPolicy
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
	// DownstreamFreshness mirrors routes.*.cache.downstream_freshness
	// (ADR-0063): DownstreamFreshnessSync ("sync") synthesizes
	// CDN-Cache-Control (RFC 9213) on every client-facing serve class so
	// a downstream CDN's clock cannot outlive bouine's; any other value
	// (including the zero value) forwards headers as today. Values are
	// validated upstream by config.Validate. Lives in the value region
	// with the other non-pointer fields (fieldalignment).
	DownstreamFreshness string
	// RefreshBeforeExpiry enables proactive background conditional
	// revalidation. A background timer fires at TTL - margin.
	RefreshBeforeExpiry bool
	// AllowSetCookie controls caching of responses with client-state
	// headers: Set-Cookie, and Set-Login (WICG Sign-In Cookies,
	// ADR-0067) — both carry per-user session state.
	// Default (false): such a response blocks caching
	// unconditionally, matching nginx's safe default and preventing
	// session-state replay across users. When true: caching is
	// permitted per RFC 9111, but the client-state header is stripped
	// from the stored object so subsequent HITs do not replay another
	// user's session state.
	AllowSetCookie bool
	// BypassOnCookie routes requests carrying a non-empty Cookie
	// header entirely around the cache (ADR-0054): no lookup, no
	// storage, no in-flight sharing. Designed for personalized SSR
	// routes where the origin renders per-user content from the
	// request cookie. Default (false): cookied requests participate in
	// the cache per RFC 9111.
	BypassOnCookie bool
	// BypassOnCookieNames routes requests carrying any cookie whose
	// name is in the list entirely around the cache — the same
	// contract as BypassOnCookie (no lookup, no storage, no
	// in-flight sharing), scoped to the listed names (issue #768).
	// Cookie names are matched case-insensitively on the name token
	// only, never on values or substrings. Requests carrying only
	// unlisted cookies participate in the cache per RFC 9111 — the
	// knob exists so ubiquitous analytics/consent cookies do not
	// force the blunt presence trigger.
	// In-flight sharing is refused unconditionally for every cookied
	// request by collapseDenied (ADR-0054) regardless of this list:
	// a listed-cookie request can never receive another user's
	// in-flight body, and an unlisted-cookie request cannot either.
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

// originURI returns the origin-bound form of uri: the route's strip
// prefix removed (when configured) and the path_rewrite regex applied
// (when configured). This is the single choke point every origin-bound
// URI passes through — miss, invalidating proxy, revalidate, background
// revalidate, background refresh, shed refill, stream, the in-process
// upstream fetches, and the upstream bypass fallbacks
// (handleBypassFast / streamBypass), which apply it in place on the
// live request before invoking the in-process upstream. Cache keys, ban
// matching, purges, Location resolution, and every client-facing surface
// keep the original uri (config contract in RouteRequest.StripPrefix and
// RouteRequest.PathRewrite).
func (h *Handler) originURI(uri []byte) []byte {
	uri = StripRequestURI(h.stripPrefix, uri)
	if h.pathRewrite != nil {
		return h.pathRewrite.RewriteURI(uri)
	}
	return uri
}

// rewriteRequestCtx applies the request-side rewrite directives directly
// to the client's RequestCtx headers before they are copied to the
// origin-bound request. Used by the foreground paths, which copy headers
// out of the ctx one by one: rewriting the source is simpler and covers
// the Vary variant computation, which reads ctx.Request.Header.
// No-op for routes without directives.
func (h *Handler) rewriteRequestCtx(ctx *fasthttp.RequestCtx) {
	if h.reqHeaderRemove != nil {
		for name := range h.reqHeaderRemove {
			ctx.Request.Header.Del(name)
		}
	}
	if h.reqHeaderSet != nil {
		for k, v := range h.reqHeaderSet {
			ctx.Request.Header.Set(k, v)
		}
	}
}

// rewriteOutboundAE normalizes Accept-Encoding on an origin-bound
// request to the canonical token for its bucket: "zstd", "br", "gzip",
// or removed for "identity". This is the pairing half of AE bucketing
// (docs/plans/accept-encoding-bucketing.md §2.1): the variant key
// claims a bucket, so the origin must be asked for exactly that coding,
// otherwise a deflate-only origin could store `Content-Encoding:
// deflate` bytes under the `gzip` bucket and a later gzip-only client
// would receive bytes it cannot decode. An origin that cannot produce
// the coding falls back to identity for the whole bucket.
//
// Called only on miss/revalidate/refill paths, never the hit path, and
// only when the route buckets (encoding_policy: bucket, the default).
// identity: the header is removed rather than set to "identity" — some
// origins treat a bare identity token as "client explicitly refuses
// compression" and add Vary noise; removal is the absence signal.
//
// Accept-Language gets the same pairing treatment (plan §10.4): the
// bucket claims one variant, so the origin must see one spelling per
// bucket — the winner tag — or a chain-echoing origin stores
// chain-dependent bodies under a single bucket. Unbucketable chains
// (absent, "*", malformed, all q=0) are left untouched: they key via
// the legacy normalization and their origin spelling is what the
// legacy keying already saw.
func (h *Handler) rewriteOutboundAE(hdr *fasthttp.RequestHeader) {
	if h.policy.verbatimEncoding() {
		return
	}
	bucket := encodingBucket(string(hdr.Peek(header.AcceptEncoding)))
	if bucket == "identity" {
		hdr.Del(header.AcceptEncoding)
	} else {
		hdr.Set(header.AcceptEncoding, bucket)
	}
	if lang, ok := langBucket(string(hdr.Peek(header.AcceptLanguage))); ok {
		hdr.Set(header.AcceptLanguage, lang)
	}
}

// applyResponseRewrites mutates a client-facing response in place with
// the route's response header rewrite directives: remove first (Del is a
// no-op for absent headers), then set (Set replaces any origin value).
// Called on every response the handler emits; no-op for routes without
// directives.
func (h *Handler) applyResponseRewrites(dst *fasthttp.ResponseHeader) {
	if h.respHeaderRemove != nil {
		for name := range h.respHeaderRemove {
			dst.Del(name)
		}
	}
	if h.respHeaderSet != nil {
		for k, v := range h.respHeaderSet {
			dst.Set(k, v)
		}
	}
}

// NewHandler creates a caching handler.
//
//nolint:funlen,gocyclo // 81 lines: initialization is inherently sequential; the rewrite-directive blocks add four flat branches
func NewHandler(cfg HandlerConfig) *Handler {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	fp := fillPolicy{
		neg:         cfg.Negative,
		defaultTTL:  cfg.DefaultTTL,
		overrideTTL: cfg.OverrideTTL,
		defaultSWR:  cfg.DefaultSWR,
		defaultSIE:  cfg.DefaultSIE,
		jitterPct:   cfg.JitterPercent,
	}
	h := &Handler{
		upstream:                cfg.Upstream,
		fastClient:              cfg.FastClient,
		stripPrefix:             []byte(cfg.StripPrefix),
		pathRewrite:             cfg.PathRewrite,
		store:                   cfg.Store,
		logger:                  cfg.Logger,
		fp:                      fp,
		stayinAlive:             cfg.StayinAlive,
		logCacheKeys:            cfg.LogCacheKeys,
		variantSets:             make(map[api.Key]map[api.Key]struct{}),
		VaryCapHits:             cfg.VaryCapHits,
		VaryDriftInc:            cfg.VaryDrift,
		StreamingBufferBytesSet: cfg.StreamingBufferBytes,
		StreamingFallbackInc:    cfg.StreamingFallback,
		FetchShedInc:            cfg.FetchShed,
		RewarmFillInc:           cfg.RewarmFill,
		ownerFn:                 cfg.OwnerFn,
		peerFetch:               cfg.PeerFetch,
		onPeerVariantMismatch:   cfg.OnPeerVariantMismatch,
		onResolverTwoStep:       cfg.OnResolverTwoStep,
		peerPut:                 cfg.PeerPut,
		purgeBroadcast:          cfg.PurgeBroadcast,
		allowSetCookie:          cfg.AllowSetCookie,
		syncCDC:                 cfg.DownstreamFreshness == downstreamSync,
		bypassOnCookie:          cfg.BypassOnCookie,
		uaBypass:                compileUABypass(cfg.BypassOnUserAgent),
		bypassCookieNames:       newCookieNameScanner(cfg.BypassOnCookieNames),
		maxObjectSize:           cfg.MaxObjectSize,
		maxVariants:             cfg.MaxVariants,
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
		poolName:                cfg.PoolName,
		done:                    make(chan struct{}),
	}
	if h.maxResponseBytes == 0 {
		h.maxResponseBytes = defaultMaxResponseBytes
	}
	if h.maxVariants <= 0 {
		h.maxVariants = DefaultMaxVariants
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
	h.rewarmSem = make(chan struct{}, defaultRewarmConcurrency)
	h.inflightStreams = newInflightTable()

	// Header rewrite policy (request/response header_set/header_remove).
	// Pre-canonicalized at construction so the per-request work is map
	// lookups and fasthttp setter calls only, and the nil checks cost
	// nothing on routes without rewrite directives.
	if len(cfg.RequestHeaderSet) > 0 {
		h.reqHeaderSet = cfg.RequestHeaderSet
	}
	if len(cfg.RequestHeaderRemove) > 0 {
		m := make(map[string]bool, len(cfg.RequestHeaderRemove))
		for _, name := range cfg.RequestHeaderRemove {
			m[strings.ToLower(name)] = true
		}
		h.reqHeaderRemove = m
	}
	// Client-identity forwarding (request.forwarded, issue #769). The
	// chain cap is defaulted here as well as in config.Validate so a
	// hand-constructed HandlerConfig (tests) cannot bypass the bound.
	if cfg.Forwarded.Enabled() {
		h.forwarded = cfg.Forwarded
		if h.forwarded.MaxAppend <= 0 {
			h.forwarded.MaxAppend = defaultForwardedMaxAppend
		}
	}
	if len(cfg.ResponseHeaderSet) > 0 {
		h.respHeaderSet = cfg.ResponseHeaderSet
	}
	if len(cfg.ResponseHeaderRemove) > 0 {
		m := make(map[string]bool, len(cfg.ResponseHeaderRemove))
		for _, name := range cfg.ResponseHeaderRemove {
			m[header.InternKey(name)] = true
		}
		h.respHeaderRemove = m
	}

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

	// Drain refresh-before-expiry, SWR, and shed-refill goroutines. A
	// zero-value WaitGroup (when refresh-before-expiry is disabled)
	// returns immediately from Wait(), so this is safe in all
	// configurations.
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
// it, enforcing RFC 9111 §4.4: when a resource is invalidated, all stored
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

// SoftPurge marks a cached object as stale without deleting it — a
// "refresh" or "soft purge" in Varnish terminology, distinct from
// Purge's hard delete and forced synchronous origin fetch. The next
// request serves the stale body under SWR or revalidates under SIE,
// depending on the object's grace windows; with neither SWR nor SIE,
// SoftPurge falls back to a hard delete — there is no graceful
// degraded mode without a grace window.
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

// lookupForRefresh returns the stored object for key when it is still
// fresh; nil when the key is gone or stale. Used by the scheduler's
// compaction pass.
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
	req.SetRequestURI(string(h.originURI([]byte(ri.GetURI()))))
	req.Header.SetHost(ri.GetHost())
	ri.Header.Range(func(k, v string) bool {
		req.Header.Set(k, v)
		return true
	})
	// Normalize AE to the bucket token so the refreshed variant
	// matches the key's bucket claim (see rewriteOutboundAE).
	h.rewriteOutboundAE(&req.Header)
	// Client-identity headers (request.forwarded, issue #769) from the
	// captured request info — no XFF append on background fetches (see
	// applyForwardedInfo).
	h.applyForwardedInfo(&req.Header, ri)
	setConditionalHeaders(func(k, v string) { req.Header.Set(k, v) }, stale)

	// Detached root span: the triggering request is long gone, and its
	// pipeline span already ended. Still attribute-bearing so slow
	// refreshes are filterable in Tempo.
	spanCtx, span := tracing.StartOriginSpan(
		ctx, []byte(ri.GetMethod()), []byte(ri.GetPath()), h.poolName, h.routeName,
	)
	defer span.End()

	res := h.collapsedFetchBg(spanCtx, req, key, ri)
	if res.Err != nil {
		// A failed fetch must not export as a clean span.
		tracing.RecordError(span, res.Err)
		h.scheduleRefreshRetry(key, stale, res.Err)
		return
	}

	// Don't store if the handler is shutting down — the store may
	// close before or during the Put.
	if ctx.Err() != nil {
		return
	}

	if res.StatusCode == fasthttp.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res, ri, time.Now())
		h.detectVaryDrift(ctx, stale, ri, res.StatusCode, refreshed.VaryValue)
		h.storeObject(ctx, key, refreshed, ri, true, staleHits)
		h.refreshMetrics.IncTotal("304")
		return
	}

	resMap := res.Header.ToMap()
	if IsCacheableWithDefault(res.StatusCode, ri.Header, resMap, h.fp.neg, h.fp.defaultTTL) {
		if reason := h.skipStoreReason(resMap, len(res.Body)); reason != "" {
			h.refreshMetrics.IncSkips(reason)
			return
		}
		h.detectVaryDrift(ctx, stale, ri, res.StatusCode, effectiveVary(resMap, h.policy))
		obj := h.buildObject(key, ri, res, resMap, nil, time.Now())
		obj.Hits = 0
		h.storeObject(ctx, key, obj, ri, true, staleHits)
		h.refreshMetrics.IncTotal("200")
		return
	}
	h.refreshMetrics.IncSkips("uncacheable")
}

// scheduleRefreshRetry records the failure metrics and re-schedules the
// refresh with backoff: half the remaining TTL, bounded by refresh_margin
// and floored at 1s. No retry once the object has fully expired.
func (h *Handler) scheduleRefreshRetry(key api.Key, stale *api.Object, refreshErr error) {
	h.refreshMetrics.IncTotal("error")
	h.refreshMetrics.IncErrors(errorType(refreshErr))
	remaining := time.Until(stale.StoredAt.Add(stale.TTL))
	if remaining <= 0 {
		return
	}
	delay := min(h.refreshMargin, remaining/2)
	if delay < time.Second {
		delay = time.Second
	}
	h.scheduler.Schedule(key, time.Now().Add(delay))
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

// logUpstreamPool returns the upstream_pool attribution stamped on
// handler log records: the route's pool name, falling back to
// "_default" for pool-less routes (e.g. static files) — the same
// fallback the metrics middleware applies to the upstream_pool label
// (internal/server/router.go), so log records stay joinable with the
// bouine_requests_total series.
func (h *Handler) logUpstreamPool() string {
	if h.poolName != "" {
		return h.poolName
	}
	return "_default"
}

// KeyPolicy returns the route's compiled cache key policy, or nil when
// the route has none. The engine uses it to rebuild keys from raw URLs
// in the admin plane (purge/refresh/cachecheck): those surfaces must
// compute the key the same way the data plane does, or a route whose
// key differs from the default (e.g. include_host: false) would purge
// and inspect keys that were never stored.
func (h *Handler) KeyPolicy() *KeyPolicy {
	return h.policy
}

// VariantCount returns the number of tracked variant store keys for
// primary key — the live entries reserved for the key's Vary dimension
// (see variantSets). The admin keys-query surface (bouine keys find,
// issue #816) reports it so an operator can see a variant resolver's
// fan-out without enumerating the store.
func (h *Handler) VariantCount(key api.Key) int {
	h.variantMu.Lock()
	defer h.variantMu.Unlock()
	return len(h.variantSets[key])
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
	// ADR-0068: a peer-put VARIANT must land in this handler's variant
	// set under its primary key, or a cluster-wide URL purge would miss
	// it (RFC 9111 §4.4) — the fill ran on another node, so the owner
	// never tracked it. The variant carries its primary as PairedKey.
	if obj.VaryKey != "" && obj.PairedKey != (api.Key{}) && obj.PairedKey != obj.Key {
		h.reserveVariantSlotFromPeer(ctx, obj.PairedKey, obj.Key)
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

// requestBypass reports whether a request-gate knob routes this
// request around the cache: bypass_on_cookie (ADR-0054 — any non-empty
// Cookie header on an opted-in route), bypass_on_cookie_names
// (issue #768 — any listed cookie name, presence trigger plus the
// compiled name scanner), or bypass_on_user_agent (issue #771,
// ADR-0055 — a User-Agent glob match on a route with patterns). The
// UA matcher's purpose is mirroring edge-CDN verified-bot bypass
// rules on inner cache layers (client → edge → bouine), so a crawler
// the edge deliberately sends around its cache is not served
// bouine's stored copy. Zero allocations: one header read per
// configured gate (PeekAll for the cookie gates — fasthttp's Peek
// does NOT join repeated Cookie lines, so a single Peek would miss a
// listed cookie on the second line; one Peek for the UA gate),
// byte-wise matching, no string conversion.
func (h *Handler) requestBypass(ctx *fasthttp.RequestCtx) bool {
	if h.cookieBypassTriggered(&ctx.Request.Header) {
		return true
	}
	return h.uaBypass != nil && h.uaBypass.matchBytes(ctx.Request.Header.Peek(header.UserAgent))
}

// serveCacheBypass dispatches a gated request around the cache. SSE
// intent wins the dispatch: handleSSE already streams live with
// never-cache semantics, which satisfies the same "never stored,
// never shared" contract (the SSE fetch never collapses and never
// buffers into the store). Everything else proxies via handleBypass
// (streamBypass), which does not write the store; the zero cacheKey
// keeps the access-log sampler consistent with the miss/revalidate
// bypass paths.
func (h *Handler) serveCacheBypass(ctx *fasthttp.RequestCtx) {
	if h.sseIntent(ctx) {
		h.handleSSE(ctx)
		return
	}
	ctx.SetUserValue("cacheKey", api.Key{})
	h.handleBypass(ctx)
}

// ServeRequest implements fasthttp.RequestHandler. It dispatches
// cache-invalidating methods (POST/PUT/DELETE) to invalidateAndProxy
// and all others to the cache lookup pipeline.
//
//nolint:gocyclo // 20: the dispatch switch is the RFC 9111 state machine's front door; the header-rewrite hook adds one branch by design
func (h *Handler) ServeRequest(ctx *fasthttp.RequestCtx) {
	if isInvalidatingBytes(ctx.Method()) {
		h.serveInvalidating(ctx)
		return
	}

	// Request-side header rewrite (request.header_set/header_remove).
	// Applied to the ctx before any read — the cache key does not
	// depend on request headers, but the Vary variant computation and
	// every origin-bound copy do, so the rewrite must land before
	// lookup. No-op for routes without directives.
	h.rewriteRequestCtx(ctx)

	// Cookie bypass (ADR-0054, cache.bypass_on_cookie; issue #768,
	// cache.bypass_on_cookie_names) and User-Agent bypass (issue #771,
	// cache.bypass_on_user_agent, ADR-0055): a request gated by any
	// knob never touches the cache — no lookup, no storage, no
	// in-flight sharing. requestBypass reads only already-parsed
	// headers (PeekAll / the compiled matcher), and all flags are
	// resolved at handler build time, so gate-less routes return
	// before any header read (cookie flag off with no listed names is
	// one bool read; UA patterns absent is one nil read — no Peek at
	// all, keeping the zero-alloc hit path unchanged). Flag-on cookie
	// routes pay one PeekAll — fasthttp's Peek does NOT join repeated
	// Cookie lines (peekArgBytes returns the first line until
	// collectCookies runs), so the gate must see every line or a
	// listed cookie on the second line slips into the cache. Placed
	// after rewriteRequestCtx so request.header_set/header_remove can
	// strip or adjust the Cookie / User-Agent headers before the check,
	// and before lookup so no store read is ever performed. SSE intent
	// wins the dispatch inside serveCacheBypass: handleSSE already
	// streams live with never-cache semantics, which satisfies the
	// same "never stored, never shared" contract.
	if h.requestBypass(ctx) {
		h.serveCacheBypass(ctx)
		return
	}

	now := time.Now()
	primaryKey, key, obj, resolver, src := h.lookup(ctx)

	// Fast path: evaluate using direct Peek calls on the request headers,
	// avoiding the headerFromCtx allocation (which builds a header.Map
	// with string conversions for every header on every request).
	// Only construct a full RequestInfo (with header.Map) if we need to
	// leave the hit path (miss, revalidate, bypass, range).
	// SetUserValue is deferred to miss/revalidate/bypass paths to avoid
	// the api.Key→any boxing allocation (1 alloc) on the hit path.
	// Access logging handles a missing cacheKey gracefully (zero-value key).
	disp := evaluateFast(ctx, obj, now)

	// SSE intent (Accept: text/event-stream, WHATWG §9.2.2 client
	// contract) changes how the origin is fetched and the response
	// delivered: live unbuffered stream, never cached, never collapsed
	// (see handleSSE). It never applies to a stored hit — a cached
	// response is complete and is served normally — so the hit path
	// pays nothing.
	if disp.Decision != Hit && disp.Decision != StaleHit && h.sseIntent(ctx) {
		h.handleSSE(ctx)
		return
	}

	switch disp.Decision {
	case Hit, StaleHit:
		cacheRes := cacheHit
		if disp.Decision == StaleHit {
			cacheRes = cacheStale
		}
		if h.tryConditional304Fast(ctx, disp.Object, src, "HIT", cacheRes, now, h.rewriteHook()) {
			// I8 (ADR-0063): a stale-sourced conditional 304 must fire
			// the same background-revalidation trigger the 200 path
			// does, under both freshness modes. Under sync a
			// downstream CDN's revalidations are precisely such
			// conditional requests; without a trigger here they would
			// freeze content for the full SWR window (the stale-304
			// hole also existed in passthrough mode — this fixes it
			// there too). Same triple gate as the 200 path below.
			if disp.Decision == StaleHit && disp.Object.StaleWhileRevalidate > 0 && h.isOwnerOrUnmanaged(key) {
				ri := requestInfoFromCtx(ctx)
				h.triggerBgRevalidate(ri, key, disp.Object)
			}
			return
		}
		// Range requests need the full RequestInfo for ServeRange.
		if hasRangeHeader(ctx) {
			ri := requestInfoFromCtx(ctx)
			if h.serveRange(&ctxResponseWriter{ctx}, ri, disp.Object, disp.Decision == StaleHit, now, src) {
				return
			}
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
		h.handleCacheMiss(ctx, primaryKey, key, obj, resolver, now, src, ri)
	case Revalidate:
		if h.logCacheKeys {
			ctx.SetUserValue("cacheKey", key)
		}
		if !h.isOwnerOrUnmanaged(key) {
			ri := requestInfoFromCtx(ctx)
			h.handleCacheMiss(ctx, primaryKey, key, obj, nil, now, src, ri)
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

// sseIntent reports whether the request announced Server-Sent Events
// intent via Accept: text/event-stream. Zero allocations: Peek returns
// the raw header bytes and the matcher walks them in place. A bare "*/*"
// never matches — every ordinary browser request carries it.
func (h *Handler) sseIntent(ctx *fasthttp.RequestCtx) bool {
	return header.AcceptsEventStream(ctx.Request.Header.Peek(header.Accept))
}

// serveInvalidating handles cache-invalidating methods (POST/PUT/DELETE).
// POST-style SSE (AI streaming APIs) must not take the buffered proxy
// path: an endless body would stall the request until fetch_timeout and
// fail with 502, so hinted requests are streamed live instead.
func (h *Handler) serveInvalidating(ctx *fasthttp.RequestCtx) {
	if h.sseIntent(ctx) {
		h.handleSSE(ctx)
		return
	}
	h.invalidateAndProxy(ctx)
}

// peerVaryAssertion derives the Vary assertion sent with a peer fetch:
// the stale/miss-side object's stored VaryKey when lookup found one
// (variant miss), the primary-key object's otherwise, and "" on a plain
// miss with no stored Vary context.
func peerVaryAssertion(obj *api.Object) string {
	if obj == nil {
		return ""
	}
	return obj.VaryKey
}

// servePeerHit validates and serves a peer-fetched object. Returns true
// when the response was written (the caller must return), false when the
// peer object was rejected or not fresh (the caller falls back to origin).
// RFC 9111 §4.1: a Vary-carrying object belongs to one selecting-header
// set. Recompute the variant dimension the fetched object must carry for
// THIS request and compare against its stored VaryKey: a mismatch means
// the peer answered with another variant's body or with the primary-key
// Vary resolver (whose VaryKey is blank by protocol). Treat it as a miss
// instead of serving cross-variant content (cross-market body served in
// production before this gate).
func (h *Handler) servePeerHit(ctx *fasthttp.RequestCtx, lookupKey api.Key, peerObj *api.Object, now time.Time, ri RequestInfo) bool {
	if peerObj.VaryValue != "" &&
		!varyKeyMatches(peerObj.VaryValue, varyAssertSrc{m: ri.Header}, h.policy, peerObj.VaryKey) {
		if h.onPeerVariantMismatch != nil {
			h.onPeerVariantMismatch()
		}
		h.logger.Debug("peer fetch returned a foreign variant for this request",
			"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
			"key", lookupKey.Hex(), "vary", peerObj.VaryValue,
			"peer_vary_key", peerObj.VaryKey)
		return false
	}
	if d := Evaluate(ri, peerObj, now); d.Decision == Hit || d.Decision == StaleHit {
		cacheRes := cacheHit
		if d.Decision == StaleHit {
			cacheRes = cacheStale
		}
		h.serveObject(ctx, peerObj, now, cacheRes, api.SourcePeer)
		// Do not store or revalidate: the object came from the owner.
		// Caching it on a non-owner would make the fleet cache redundant
		// (issue #509). Revalidation is the owner's responsibility.
		return true
	}
	return false
}

// peerHintsApply reports whether a fast-path peer-branch hint (transferred
// by the h1parser under api.OwnerMissContextKey,
// api.OwnerGateRejectContextKey, or api.OwnerShedContextKey) suppresses
// THIS handler's peer RPC.
// Any hint carries proof that the peer fetch was already attempted or
// shed — so the RPC is skipped and the request goes straight to origin.
// The hint is trusted only when the stored object is nil (otherwise the
// slow path's peer question carries peerVaryAssertion(obj) — a different
// question) and the route has no KeyPolicy (the hint flag does not bind
// to the producing route's policy, so on a policied route the slow path
// cannot prove its peer question identical to the fast path's and keeps
// the retry; conservative, since the per-route fast path runs the route's
// own policy — lifting the restriction needs a route-bound hint).
func (h *Handler) peerHintsApply(obj *api.Object, ctx *fasthttp.RequestCtx) bool {
	if obj != nil || h.policy != nil {
		return false
	}
	return ctx.UserValue(api.OwnerMissContextKey) == true ||
		ctx.UserValue(api.OwnerGateRejectContextKey) == true ||
		ctx.UserValue(api.OwnerShedContextKey) == true
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
// resolver is the primary-key Vary resolver on a variant miss (nil on a
// plain miss); when it carries validators the origin fetch reuses them
// (RFC 9111 §4.3.1) before falling through to the unconditional fill.
//
//nolint:gocyclo // 16: miss/peer-hint/gate branches mirror the fast path's decision tree
func (h *Handler) handleCacheMiss(ctx *fasthttp.RequestCtx, primaryKey api.Key, lookupKey api.Key, obj, resolver *api.Object, now time.Time, src api.Source, ri RequestInfo) {
	// Fast-path peer-branch hints (api.RawRequest.OwnerMiss /
	// OwnerGateReject), transferred by the h1parser under
	// api.OwnerMissContextKey / api.OwnerGateRejectContextKey:
	//   - OwnerMiss: on a plain miss the H1 fast path already asked the
	//     owner and got a definitive miss, so the identical second peer
	//     RPC is skipped and the fetch goes straight to origin.
	//   - OwnerGateReject: the fast path asked and the owner's object
	//     failed the variant gate. On a nil-policy route the two paths'
	//     gates are byte-identical for the same wire bytes (parity
	//     pinned by TestPeerVaryGateHeaderParity), so the retry is
	//     deterministically rejected twice — skip it and go to origin.
	// Both hints require obj == nil AND h.policy == nil: with a stale
	// object or Vary resolver present (obj != nil) the slow path's peer
	// question carries peerVaryAssertion(obj) — a different question —
	// and with a KeyPolicy the hint flag does not bind to the producing
	// route's policy, so the identical-question proof is unavailable
	// and the retry is kept. The owner
	// populating the key in the race window is missed either way;
	// recovery is the same: the origin fetch is singleflight-collapsed
	// and peer-put to the owner, so later requests recover through the
	// peer path.
	if h.peerHintsApply(obj, ctx) {
		// Either hint proves the owner was already asked for exactly
		// this (nil-policy, plain-key) question: skip the duplicate RPC.
	} else if h.ownerFn != nil && h.peerFetch != nil {
		if owner, isLocal := h.ownerFn(lookupKey); !isLocal {
			// ADR-0068: the fast path may have already asked this exact
			// blank question and fetched the metadata-only resolver —
			// reuse it instead of repeating the identical bare ask.
			// transferredResolver verifies the identity proof the
			// boolean hints cannot carry: the object's own Key.
			peerObj := transferredResolver(ctx, obj, lookupKey)
			var err error
			if peerObj == nil {
				peerObj, err = h.peerFetch(ctx, owner, lookupKey, peerVaryAssertion(obj))
			}
			switch {
			case err != nil:
				h.logger.Debug("peer fetch error, falling back to origin",
					"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
					"peer", owner.Addr, "key", lookupKey, "error", err)
			case peerObj != nil && isPeerResolverMetadata(peerObj):
				// ADR-0068: the owner answered the bare
				// primary-key ask with the metadata-only
				// resolver — the Vary list this node could
				// not compute locally. Derive THIS request's
				// variant key and re-ask the variant's owner
				// with a real assertion.
				if h.serveResolverPeerVariant(ctx, primaryKey, peerObj, now, ri) {
					return
				}
			case peerObj != nil:
				if h.servePeerHit(ctx, lookupKey, peerObj, now, ri) {
					return
				}
			}
		}
	}
	// Vary-variant miss with a validator-bearing resolver (RFC 9111
	// §4.3.1): the stored response can never be selected for this
	// request (its Vary values differ), but the cache MAY still combine
	// the stored validators into the forwarded request — the origin
	// then either answers 304 (serveResolver304 serves the stored
	// representation only when the origin proves it is the current one
	// for THIS request) or delivers a full response (the ordinary miss
	// fill, stored under this request's variant). An ambiguous 304 or
	// upstream error falls through to the unconditional fill; the
	// validators are an optimization, never a requirement.
	// Requests carrying Authorization or Cookie are excluded: the
	// resolver can be another requester's variant, and ADR-0052 keeps
	// personal-request traffic from ever being entangled with stored
	// state belonging to a different identity — even for validator
	// metadata, even though the origin makes the content decision.
	if resolver != nil && resolver.ETag != "" && !collapseDenied(ri) && h.isOwnerOrUnmanaged(lookupKey) {
		if h.revalidateResolverMiss(ctx, primaryKey, resolver, now, src, ri) {
			return
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
		h.fetchAndStore(ctx, lookupKey, primaryKey, obj, ri)
	}
}

// revalidateResolverMiss runs the RFC 9111 §4.3.1 validator-reuse
// attempt for a Vary-variant miss: the stored response can never be
// selected for this request (its Vary values differ), but the cache
// MAY combine the stored resolver's entity-tag into the forwarded
// request and let the origin decide. A 304 answer hands off to
// serveResolver304, which serves the stored representation only when
// the origin proves it is the current one for THIS request. Any full
// response takes the ordinary miss fill (writeAndMaybeStore stores it
// under this request's variant via its own Vary declaration).
//
// Returns true when the request was fully handled here. false means
// the caller must proceed with the ordinary unconditional miss fill —
// an upstream error or an ambiguous 304, where the validators were
// an optimization, never a requirement.
//
// Deliberately not singleflight-collapsed: joining the plain-miss
// flight would hand this caller an unconditional result, and letting a
// plain miss join a conditional one would hand the follower a 304
// with no stored response in scope. Variant mismatches are cold-path;
// concurrent ones simply each fetch.
func (h *Handler) revalidateResolverMiss(ctx *fasthttp.RequestCtx, primaryKey api.Key, resolver *api.Object, now time.Time, src api.Source, ri RequestInfo) bool {
	revalReq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(revalReq)
	revalReq.Header.SetMethodBytes(ctx.Method())
	revalReq.SetRequestURIBytes(h.originURI(ctx.RequestURI()))
	revalReq.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		revalReq.Header.AddBytesKV(k, v)
	}
	// Normalize AE to the bucket token so the filled variant matches the
	// key's bucket claim (see rewriteOutboundAE).
	h.rewriteOutboundAE(&revalReq.Header)
	// Client-identity headers (request.forwarded, issue #769) on the
	// outbound copy only — see applyForwardedCtx.
	h.applyForwardedCtx(&revalReq.Header, ctx)
	// RFC 9111 §4.3.1: combine the received If-None-Match list with the
	// stored response's entity-tag — the union replaces the client's own
	// list. When the client sent none, the stored tag alone is sent.
	// A bare "*" is never unioned: it matches every current
	// representation on its own, and mixing it into a list is invalid
	// field syntax (RFC 9110 §13.1.2) that an origin may reject.
	if clientINM := revalReq.Header.Peek(header.IfNoneMatch); len(clientINM) > 0 {
		if bytes.Equal(bytes.TrimSpace(clientINM), []byte("*")) {
			// keep the wildcard as-is
		} else {
			revalReq.Header.Set(header.IfNoneMatch, string(clientINM)+", "+quoteETag(resolver.ETag))
		}
	} else {
		revalReq.Header.Set(header.IfNoneMatch, quoteETag(resolver.ETag))
	}
	// The stored Last-Modified only rides along when the client did not
	// bring its own If-Modified-Since: If-None-Match takes precedence at
	// the origin (RFC 9110 §13.1.2), and overwriting the client's own
	// date would change their condition's meaning.
	if !resolver.LastModified.IsZero() && len(revalReq.Header.Peek(header.IfModifiedSince)) == 0 {
		revalReq.Header.Set(header.IfModifiedSince,
			resolver.LastModified.UTC().Format(httpTimeFormat))
	}

	// Span context stored by the middleware, never the RequestCtx (whose
	// lifetime ends at handler return); ended by the deferred End below.
	revalCtx, revalSpan := tracing.StartOriginSpan(
		tracing.SpanContextFromRequest(ctx),
		ctx.Method(), ctx.Path(), h.poolName, h.routeName,
	)
	defer revalSpan.End()
	res := h.doFetchBg(revalCtx, revalReq)
	if res.Err != nil {
		tracing.RecordError(revalSpan, res.Err)
		return false
	}

	if res.StatusCode == fasthttp.StatusNotModified {
		return h.serveResolver304(ctx, resolver, res, src, ri, now)
	}

	// Full response: the ordinary miss fill. writeAndMaybeStore derives
	// the store key from this response's own Vary declaration, so the
	// newly fetched variant lands under the variant key, not the
	// resolver's primary slot. stale is nil: the resolver was never
	// usable for this request and must not become an error fallback.
	h.writeAndMaybeStore(ctx, res, primaryKey, nil, ri)
	return true
}

// serveResolver304 handles the 304 answer to a vary-miss validator
// reuse. The forwarded If-None-Match carried the union of the client's
// tags and the resolver's, so the 304 validates only SOME member of
// that list (RFC 9111 §4.3.2); the origin's ETag names which
// representation is current for this request. Returns true when the
// request was fully handled here; false directs the caller to the
// ordinary unconditional miss fill.
//
// Cross-requester safety: the stored resolver's body may be served to
// THIS requester only when the 304 identifies the resolver's tag (or
// the resolver's tag was the only condition sent upstream) — the origin
// then explicitly asserted the stored representation is the one this
// request's selecting headers select. A 304 that answered the client's
// own tag, or that names neither's representation, proves nothing
// about the stored body; serving it would hand this requester another
// variant's content.
//
// The refreshed resolver is served to this client only and is never
// re-stored: RFC 9111 §4.3.4 limits 304 freshening to stored responses
// that could have been chosen for the request, and the resolver could
// not (its Vary values differ). Re-storing would extend the freshness
// window of the resolver's own variant based on a validation made with
// a different requester's selecting headers — a window in which the
// origin may have already rotated that variant's representation.
func (h *Handler) serveResolver304(ctx *fasthttp.RequestCtx, resolver *api.Object, res fetchResult, src api.Source, ri RequestInfo, now time.Time) bool {
	tag304 := res.Header.Get(header.ETag)
	clientConditional := len(ctx.Request.Header.Peek(header.IfNoneMatch)) > 0 ||
		len(ctx.Request.Header.Peek(header.IfModifiedSince)) > 0
	if tag304 == "" {
		// A 304 without an ETag cannot say which member of the union
		// matched (RFC 9110 §13.1.2: the origin 304s when ANY listed tag
		// matches). Without a client conditional, the resolver's tag was
		// the only one sent, so it was the match. With one, the client's
		// own tag may be the current representation and nothing about
		// the stored body is proven — refetch unconditionally.
		if clientConditional {
			return false
		}
	} else if tag304 != "*" && !etagMatch(tag304, resolver.ETag) {
		// The 304 names a representation other than the stored
		// resolver's.
		if inm := ctx.Request.Header.Peek(header.IfNoneMatch); len(inm) > 0 && etagMatch(string(inm), tag304) {
			// It is the client's own tag: the client's condition was
			// validated (RFC 9111 §4.3.2's complement of the
			// MUST-generate-200 clause). Answer with the origin's 304,
			// metadata intact; the store stays untouched.
			res.Header.CopyToFastHTTP(&ctx.Response.Header)
			dst := &ctx.Response.Header
			if h.syncCDC {
				// ADR-0063 I3/I4: the origin's CDC-family lines are removed,
				// and the 304 is capped strictly — this branch relays the
				// origin's 304 to the client while the stored resolver keeps
				// its own (stale) clock untouched, so the only honest cap is
				// the strict one: bouine would not serve this content again
				// without revalidation, so a downstream cache must not trust
				// it either.
				removeCDCFamily(dst)
				dst.SetCanonical(header.S2b(header.CDNCacheControl), header.S2b(cdcStrict))
			}
			dst.SetCanonical(header.S2b(header.XCache), header.S2b("REVALIDATED"))
			dst.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(src)))
			ctx.SetStatusCode(fasthttp.StatusNotModified)
			h.applyResponseRewrites(dst)
			return true
		}
		// Neither the resolver's nor the client's representation was
		// named: nothing reusable is proven. Fall through to the
		// ordinary unconditional fill — the validators were an
		// optimization, never a requirement.
		return false
	}
	// The 304 proves the stored resolver's representation is the
	// current one for THIS request. Serve it, merged with the 304's
	// metadata (RFC 9111 §4.3.2: "reusing its corresponding stored
	// response, as updated by the 304 response metadata"); the stored
	// resolver keeps its own clock (see the doc comment above).
	//
	// ADR-0068: the resolver is metadata-only — the bytes it serves are
	// the mirror variant's, fetched here. Legacy resolvers (stored
	// before ADR-0068, e.g. warm-tier reload of a pre-upgrade blob)
	// carry no PairedKey and their own body; serve that directly. If
	// the mirror is gone from both tiers (purged, or hot- and
	// warm-evicted), the 304 proves nothing servable for a bodyless
	// resolver — the validators were an optimization, never a
	// requirement; fall through to the ordinary unconditional fill.
	body, bodySize := resolver.Body, resolver.BodySize
	if resolver.PairedKey != (api.Key{}) {
		mirror, _, mErr := h.store.Get(ctx, resolver.PairedKey)
		if mErr != nil || mirror == nil {
			return false
		}
		body, bodySize = mirror.Body, mirror.BodySize
	}
	refreshed := h.refreshFrom304(resolver, res, ri, now)
	refreshed.Body = body
	refreshed.BodySize = bodySize
	if body != nil {
		// Belt: the resolver's stored Content-Length describes the
		// variant at fill time; serve what the mirror actually holds so
		// the head can never advertise bytes it cannot back.
		var clBuf [16]byte
		refreshed.Header.Set(header.ContentLength, string(strconv.AppendInt(clBuf[:0], int64(len(body)), 10)))
	}
	// Drift check (ADR-0058): a declaration change in the 304 purges
	// the resolver and its variants — they are keyed under the retired
	// declaration and can never be selected again.
	h.detectVaryDrift(ctx, resolver, ri, res.StatusCode, refreshed.VaryValue)
	// RFC 9111 §4.3.1: the MUST-generate-200 clause applies only when
	// the 304's entity-tag is not in the client's own If-None-Match
	// list. When the client's condition was part of what the origin
	// validated, the client gets its 304 — the same complement the
	// plain revalidation path serves.
	if h.tryConditional304Fast(ctx, refreshed, src, "REVALIDATED", cacheRevalidated, now, h.rewriteHook()) {
		return true
	}
	h.serveObject(ctx, refreshed, now, cacheRevalidated, src)
	return true
}

// lookup resolves the cache key and stored object for r, accounting
// for Vary-based secondary keys. Returns the source (hot/warm) from
// the storage tier that served the object. On a variant miss (a
// stored Vary resolver exists but the request's variant is not
// stored), obj is nil and resolver carries the primary-key resolver —
// the caller can still use its validators for an origin revalidation
// attempt (RFC 9111 §4.3.1) even though the response itself can never
// be selected for this request.
func (h *Handler) lookup(ctx *fasthttp.RequestCtx) (primaryKey api.Key, lookupKey api.Key, obj, resolver *api.Object, src api.Source) {
	primaryKey = h.buildKey(ctx)
	obj, src, err := h.store.Get(ctx, primaryKey)
	if err != nil {
		h.logger.Warn("cache lookup error",
			"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
			"key", primaryKey, "error", err)
	}
	if obj == nil || obj.VaryValue == "" {
		return primaryKey, primaryKey, obj, nil, src
	}
	vk := VariantKeyFast(primaryKey, obj.VaryValue, &ctx.Request.Header, h.policy)
	if vk == primaryKey {
		return primaryKey, primaryKey, obj, nil, src
	}
	vobj, vsrc, verr := h.store.Get(ctx, vk)
	if verr == nil && vobj != nil {
		return primaryKey, vk, vobj, nil, vsrc
	}
	return primaryKey, vk, nil, obj, src
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

// remapHeadToGet converts a HEAD method to GET for background origin
// fetches that refresh a stored object (SWR revalidation, shed refill):
// the stored object is the GET representation, and HEAD differs from
// GET only by the omitted body (RFC 9110 §9.3.2), so fetching HEAD
// would refresh the object with an empty body (issue #752). The fast
// path applies the same remap (requestInfoFromRaw).
func remapHeadToGet(method string) string {
	if method == "HEAD" {
		return "GET"
	}
	return method
}

// requestInfoFromRaw builds an owned RequestInfo from a RawRequest.
// The RawRequest's string fields alias the h1parser read buffer, so all
// strings are copied into owned memory — the fast-path equivalent of
// the materialize-before-escape rule the SWR goroutine follows in
// triggerBgRevalidate.
//
// Cookie lines: the h1parser keeps every Cookie header line
// separately, and header.Map.Set overwrites — a per-line Set would keep
// only the LAST line's pairs, dropping a listed cookie and skewing any
// cookie-keying (presence bits, bypass replay) computed downstream.
// The lines are joined into one §4.2 value instead, the same shape
// headerFromCtx produces from fasthttp's collected cookies, so both
// paths' RequestInfos key identically.
func requestInfoFromRaw(req *api.RawRequest) RequestInfo {
	uri := req.Path
	if req.Query != "" {
		uri += "?" + req.Query
	}
	method := remapHeadToGet(req.Method)
	ri := RequestInfo{
		Method: method,
		URI:    uri,
		Host:   req.Host,
		Path:   req.Path,
		TLS:    req.Scheme == "https",
	}
	ri.Header = header.NewMap(req.NHeaders)
	for i := 0; i < req.NHeaders; i++ {
		if api.EqualFold(req.Headers[i].Key, header.Cookie) {
			continue
		}
		ri.Header.Set(req.Headers[i].Key, req.Headers[i].Value)
	}
	if cv := req.CookieValue(); cv != "" {
		ri.Header.Set(header.Cookie, cv)
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
// the source metric label or X-Cache result. BYPASS responses carry
// api.SourceOrigin on every branch that dispatched something toward the
// origin (a fetch or the in-process upstream); the shed 503
// (writeShed503), the no-client-no-upstream 502, and the
// only-if-cached 504 keep the empty source — the origin was never
// reached — which the metrics layer labels "bouine".
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
		// Upstream fallback (issue #598): a cached-static-route handler
		// is wired with Upstream (the staticfile handler) and no
		// FastClient. Run the upstream handler in-process — its response
		// is already in ctx.Response. The upstream is the bare handler,
		// so the origin-bound URI (strip_prefix / path_rewrite) is
		// applied here, in place, exactly once; the bypass path used to
		// rely on rewrite wrappers baked into the upstream chain, which
		// double-applied on every miss fetch.
		if h.upstream != nil {
			if u := h.originURI(ctx.RequestURI()); !bytes.Equal(u, ctx.RequestURI()) {
				ctx.Request.SetRequestURIBytes(u)
			}
			h.upstream(ctx)
			ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("BYPASS"))
			ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
			h.applyResponseRewrites(&ctx.Response.Header)
			return
		}
		// RFC 9111 §5.2.2.2: no fast client configured and no upstream
		// fallback (misconfiguration) — surface 502, not a panic. The
		// error body resets the response, so X-Cache is written after.
		// No X-Cache-Source: no fetch was dispatched, so the metrics
		// layer labels this response "bouine", not "origin".
		ctx.Error("upstream error: no fast client configured", fasthttp.StatusBadGateway)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("BYPASS"))
		h.applyResponseRewrites(&ctx.Response.Header)
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

	// Downstream freshness sync (ADR-0063): under sync the origin's
	// RFC 9213-family and Surrogate-Control lines are replaced (the
	// synthesis is authoritative, invariant I3) and the cap is
	// synthesized from bouine's own clock. Runs before the route's
	// response rewrites so header_set / header_remove remain the
	// operator escape hatch (invariant I7).
	if h.syncCDC {
		removeCDCFamily(dst)
		var cdcBuf [80]byte
		dst.SetCanonical(header.S2b(header.CDNCacheControl),
			appendServeCDCDirectives(cdcBuf[:0], obj, now, result))
	}

	ctx.SetStatusCode(obj.StatusCode)
	if !bytes.Equal(ctx.Method(), []byte("HEAD")) {
		ctx.Response.SetBodyRaw(obj.Body) // #nosec G705 -- obj.Body is an immutable cached origin response
	}
	h.applyResponseRewrites(dst)
}

// collapsedFetch deduplicates concurrent origin fetches for the same key.
// The shared fetchResult is detached per caller: buildObject mutates its
// resMap (attribution headers), so concurrent callers must not share one
// mutable header.Map. Authorized requests skip the dedup entirely
// (collapseDenied, ADR-0052): an authorized response is never shareable
// in-flight, so each authorized caller fetches its own copy. Every other
// request parks under collapseFlightKey: the variant key on a warm
// flight, the dimension-extended key on a cold include_headers route,
// and no shared flight at all on a cold include-free miss (ADR-0057).
// A response carrying client-state headers (Set-Cookie / Set-Login) is
// never handed to a second caller: every shared caller fetches its own
// (ADR-0067).
func (h *Handler) collapsedFetch(ctx *fasthttp.RequestCtx, lookupKey, primaryKey api.Key, obj *api.Object, ri RequestInfo) fetchResult {
	if collapseDenied(ri) {
		res := h.doFetch(ctx)
		res.Header = res.Header.ownedClone()
		return res
	}
	sfKey := collapseFlightKey(h, primaryKey, lookupKey, obj, ri)
	if sfKey == (api.Key{}) {
		res := h.doFetch(ctx)
		res.Header = res.Header.ownedClone()
		return res
	}
	suffix := uint64(0)
	if bytes.Equal(ctx.Method(), []byte("HEAD")) {
		suffix = headRevalKeySuffix
	}
	v, _, shared := h.flight.Do(sfKey.SingleFlightKey(suffix), func() (any, error) {
		res := h.doFetch(ctx)
		return res, nil
	})
	res := v.(fetchResult)
	if shared && res.Err == nil && carriesClientState(res.Header) {
		// ADR-0067: the flight's response carries client-state headers
		// (Set-Cookie / Set-Login) minted for whichever caller fetched
		// it — they must not be served to any other caller. x/sync
		// cannot distinguish the leader from a follower (both see
		// shared=true when the value was handed to others), so every
		// shared caller fetches its own; a solo caller (shared=false)
		// keeps its own response. The cost — one extra origin fetch when
		// a client-state origin meets concurrent anonymous misses —
		// mirrors ADR-0052's refusal to collapse what cannot be proven
		// shareable.
		res = h.doFetch(ctx)
	}
	res.Header = res.Header.ownedClone()
	return res
}

// revalKeySuffix XORs the key with a constant to produce a singleflight
// key that won't collide with regular fetches for the same cache key,
// while still deduplicating concurrent revalidations for that key.
const revalKeySuffix uint64 = 0x726576616c // "reval" in ASCII

// headRevalKeySuffix separates HEAD-led origin-fetch flights from
// GET-led ones in the singleflight space (collapsedRevalidateBg's
// revalidation flights and collapsedFetch's stale-fallback flights). A
// HEAD representation fetch cannot substitute a GET one: sharing one
// flight let a GET follower store a HEAD-led flight's empty body under
// the GET-shared key (the issue #752 poisoning via singleflight), and
// would hand a HEAD follower a GET-led flight's full response, which
// freshenFromHead must not merge over the stored body. HEAD requests
// still collapse with each other; background fetches always remap
// HEAD→GET (remapHeadToGet) and stay in the GET flight space. The cache
// KEY space is untouched — BuildKey still folds HEAD into GET, so the
// stored entry serves both methods; only the in-flight fetch dedup is
// method-aware.
const headRevalKeySuffix uint64 = 0x68656164 // "head" in ASCII

// collapsedFetchBg deduplicates concurrent background origin fetches for
// the same key. Every caller holds a stored object (a refresh or rewrite
// of a known key), so the flight key already encodes the route's declared
// variant dimensions and the collapse is safe without the cold-miss gate.
// Authorized requests still skip the dedup (collapseDenied, ADR-0052).
func (h *Handler) collapsedFetchBg(ctx context.Context, req *fasthttp.Request, key api.Key, ri RequestInfo) fetchResult {
	if collapseDenied(ri) {
		res := h.doFetchBg(ctx, req)
		res.Header = res.Header.ownedClone()
		return res
	}
	v, _, _ := h.flight.Do(key.SingleFlightKey(0), func() (any, error) {
		res := h.doFetchBg(ctx, req)
		return res, nil
	})
	res := v.(fetchResult)
	res.Header = res.Header.ownedClone()
	return res
}

// collapsedRevalidateBg deduplicates concurrent revalidations for the
// same key. Authorized requests skip the dedup entirely (collapseDenied,
// ADR-0052): the conditional request carries the caller's credentials,
// so its response is never shareable in-flight — each authorized caller
// revalidates on its own. Every revalidation holds a stored object, so
// collapseFlightKey resolves to the lookup key and the flight always
// shares; the refusal branch is defense-in-depth only.
func (h *Handler) collapsedRevalidateBg(ctx context.Context, req *fasthttp.Request, lookupKey, primaryKey api.Key, stale *api.Object, ri RequestInfo) fetchResult {
	sfKey := api.Key{}
	if !collapseDenied(ri) {
		sfKey = collapseFlightKey(h, primaryKey, lookupKey, stale, ri)
	}
	if sfKey == (api.Key{}) {
		res := h.doFetchBg(ctx, req)
		res.Header = res.Header.ownedClone()
		return res
	}
	suffix := revalKeySuffix
	if req.Header.IsHead() {
		suffix = headRevalKeySuffix
	}
	v, _, shared := h.flight.Do(sfKey.SingleFlightKey(suffix), func() (any, error) {
		res := h.doFetchBg(ctx, req)
		return res, nil
	})
	res := v.(fetchResult)
	if shared && res.Err == nil && res.StatusCode != fasthttp.StatusNotModified && carriesClientState(res.Header) {
		// ADR-0067: a full 200 revalidation response carrying client
		// state (Set-Cookie / Set-Login) is per-caller; every shared
		// caller re-issues its own conditional fetch. A 304 never leaks
		// client state: MergeHeaders304 excludes it from the merged
		// stored object, and the follower's client response is served
		// from that object.
		res = h.doFetchBg(ctx, req)
	}
	res.Header = res.Header.ownedClone()
	return res
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
		if h.upstream != nil {
			// Upstream fallback (issue #598): a cached-static-route
			// handler is wired with Upstream (the staticfile handler)
			// and no FastClient. The caller (revalidate / background
			// refresh) already built the complete upstream-bound request
			// shape, so replay it verbatim into the scratch ctx.
			return h.fetchViaUpstreamRequest(req)
		}
		return fetchResult{Err: fmt.Errorf("no fast client configured")}
	}
	// The caller already started the bouine.origin span and passes only
	// its context down; the span's owner ends it and records errors from
	// the returned result.
	//
	// Deadline-based timeout: transport.Client.Do maps ctx.Deadline() to
	// fasthttp's kernel-level connection deadlines. The previous
	// context.WithCancel + time.AfterFunc never reached production transports
	// (WithCancel has no deadline, so transport.Client.Do fell back to a
	// fixed 60s DoTimeout) — fetch_timeout was silently ignored in
	// production. Unlike the foreground paths (doFastFetch — no context
	// at all), this background path uses context.WithTimeout instead of
	// DoDeadline because background callers hold a ctx they cancel on
	// handler shutdown, and that cancellation must reach the transport.
	fetchCtx := ctx

	select {
	case h.fetchSem <- struct{}{}:
		defer func() { <-h.fetchSem }()
	default:
	}

	if h.fetchTimeout > 0 {
		var cancel context.CancelFunc
		fetchCtx, cancel = context.WithTimeout(fetchCtx, h.fetchTimeout)
		defer cancel()
	}

	resp := fasthttp.AcquireResponse()

	fetchStart := time.Now()
	if err := h.fastClient.Do(fetchCtx, req, resp); err != nil {
		fasthttp.ReleaseResponse(resp)
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
		Delay:      time.Since(fetchStart),
	}
}

// fillReentryUserKey is the fasthttp user-value key marking a request
// re-entering the pipeline after waiting on a fill flight (ADR-0066):
// its own fetch — taken when the fill stored nothing usable for it —
// must never park on a fill flight again, or the completed leader's
// not-yet-deleted table entry would spin the follower between wake and
// re-lookup. Scoped to internal/cache: set and read only by
// fetchAndStore/serveFillFollower.
const fillReentryUserKey = "bouine.fill_reentry"

// fillWaveUserKey is the fasthttp user-value key carrying the size of the
// fill flight's released wave — the number of followers that parked on the
// flight — from serveFillFollower to the follower's own-fetch site in
// fetchAndStore. Set alongside fillReentryUserKey on every fill-follower
// re-entry; read only by fetchAndStore's re-entry guard. Scoped to
// internal/cache for the same reason as fillReentryUserKey.
const fillWaveUserKey = "bouine.fill_wave"

// fillRefetchStaggerPerFollower and fillRefetchStaggerMax bound the
// stagger window over which a released fill wave re-fetches its own
// copies: each own-fetching follower sleeps a uniform-random slice of a
// window that grows linearly with the wave size, capped at 100ms — the
// same scale as fetchWaitTimeout, the queue bound the sequential world
// already accepts on this path.
const (
	fillRefetchStaggerPerFollower = time.Millisecond
	fillRefetchStaggerMax         = 100 * time.Millisecond
)

// fillRefetchDelay returns the stagger delay for one member of a released
// fill wave of n followers. A lone follower (n == 1) is its own sequential
// arrival and never delays. Wave members spread over a window proportional
// to the wave so the origin observes their own fetches as the staggered
// arrivals a sequential world produces, never as one synchronized burst.
func fillRefetchDelay(n int) time.Duration {
	if n <= 1 {
		return 0
	}
	window := time.Duration(n) * fillRefetchStaggerPerFollower
	if window > fillRefetchStaggerMax {
		window = fillRefetchStaggerMax
	}
	return time.Duration(rand.IntN(int(window))) //nolint:gosec // G404: herd-release spacing, not crypto
}

// waveSize reads the released fill wave's size from the re-entered
// request's user value. Absent (0) means the request is not a fill
// follower; fillRefetchDelay then treats it as a lone sequential
// arrival and never delays.
func waveSize(ctx *fasthttp.RequestCtx) int {
	if n, ok := ctx.UserValue(fillWaveUserKey).(int); ok {
		return n
	}
	return 0
}

// fetchAndStore fills a miss, parking concurrent misses for the same
// key on one leader flight when the collapse rules allow it. Personal
// requests (Authorization, Cookie) and unsafe methods never share a
// flight (ADR-0052/ADR-0054): unchanged. Every other request parks
// under fetchFlightKey (ADR-0057 + ADR-0066): the lookup/dimension key
// on warm and declared-dimension flights, whose followers receive the
// leader's buffered result (the flight key provably encodes the
// origin's declared variation surface); and the fill-flight key on an
// anonymous cold include-free miss, whose followers never receive the
// leader's bytes — they wait for the fill to settle in the store and
// re-enter the request pipeline (serveFillFollower, ADR-0066).
func (h *Handler) fetchAndStore(ctx *fasthttp.RequestCtx, lookupKey, primaryKey api.Key, obj *api.Object, ri RequestInfo) {
	if ctx.UserValue(fillReentryUserKey) == true {
		// A fill follower fetching its own: the pipeline has already
		// decided this request's fetch — never re-park. When this
		// follower is one of a released wave (the fill settled with
		// nothing usable for any of them), the wave must not reach the
		// origin as a synchronized burst: N followers waking on the
		// same settle signal and fetching together complete within
		// microseconds of each other, which clusters their same-status
		// responses adjacently in the pool's passive-health accounting
		// (a healthy origin serving one 5xx-returning path can hit the
		// consecutive-5xx ejection threshold off a single wave —
		// diagnosed on PR #837, residual ejection semantics tracked in
		// #839) and hammers an origin that is already struggling. The
		// stagger restores the arrival spread a sequential world has
		// by construction; store-served followers never pass through
		// here and keep their zero added latency.
		time.Sleep(fillRefetchDelay(waveSize(ctx)))
		h.streamMiss(ctx, primaryKey, ri, newInflightStream())
		return
	}
	sfKey, fillFlight := fetchFlightKey(h, primaryKey, lookupKey, obj, ri)
	if sfKey == (api.Key{}) {
		h.streamMiss(ctx, primaryKey, ri, newInflightStream())
		return
	}
	// Try to become the streaming leader for this key.
	// If another request is already streaming, wait for its buffered result.
	inflight := newInflightStream()
	if actual, loaded := h.inflightStreams.loadOrStore(sfKey, inflight); loaded {
		// Follower: wait for the leader's buffered result.
		existing := actual
		existing.followers.Add(1)
		if fillFlight {
			h.serveFillFollower(ctx, lookupKey, ri, existing)
			return
		}
		<-existing.done
		res := existing.res
		if res.Err != nil {
			if errors.Is(res.Err, ErrStreamUnshareable) {
				// The leader's response is delivered unbuffered (SSE,
				// variant overflow): no body was buffered to share, and
				// the leader released us at header time. Fetch our own
				// response — outside singleflight, since a live stream
				// cannot be replayed to a second client (ADR-0042).
				h.streamMiss(ctx, primaryKey, ri, newInflightStream())
				return
			}
			if errors.Is(res.Err, ErrFetchShed) {
				h.writeShed503(ctx, "MISS")
				// The leader shed without fetching: schedule the bounded
				// background refill so this shed is not a lost re-warm.
				h.triggerShedRefill(ri, lookupKey)
				return
			}
			writeUpstreamError(ctx, res.Err)
			ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
			ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
			h.applyResponseRewrites(&ctx.Response.Header)
			return
		}
		// No re-store: the leader already stored.
		h.writeBufferedResult(ctx, res, primaryKey, ri)
		return
	}
	// Leader: remove from inflight map when done.
	defer h.inflightStreams.delete(sfKey)
	h.streamMiss(ctx, primaryKey, ri, inflight)
}

// dropStaleOwnerHints invalidates the H1 fast path's peer hints
// (api.OwnerMiss / api.OwnerGateReject / api.OwnerShed) before a fill
// follower re-enters ServeRequest. The hints prove the owner was
// already asked exactly this (nil-policy, plain-key) question — BEFORE
// the leader's fill settled. The fill may have changed the owner's
// answer (the leader stored locally or forwarded the object via
// peerPut), so the re-entry must ask again like any freshly-arrived
// request; with a stale hint, a non-owner follower skips the peer
// lookup and fetches its own origin copy instead of serving the fill's
// object from the owner — losing the store-mediated serve ADR-0066
// promises in strong cluster mode.
func dropStaleOwnerHints(ctx *fasthttp.RequestCtx) {
	ctx.SetUserValue(api.OwnerMissContextKey, false)
	ctx.SetUserValue(api.OwnerGateRejectContextKey, false)
	ctx.SetUserValue(api.OwnerShedContextKey, false)
}

// serveFillFollower resolves a fill-flight follower (ADR-0066): a
// concurrent anonymous cold include-free miss that parked on a leader's
// fill. The follower NEVER reads the leader's response bytes. It waits
// for the fill to settle in the store (inflight.stored closes after the
// leader's store attempt, or at the leader's verdict when nothing will
// be stored), then re-enters ServeRequest as a freshly-arrived request:
// every response it can receive passes the same gates a sequential
// request passes — Vary variant selection (a follower whose variant is
// absent from the fill fetches its own), RFC 9111 §3.5 storage (the
// fill only hands the follower a stored object when the origin's
// response was storable for a shared cache), freshness, conditionals —
// pinned byte-for-byte by TestFetchAndStore_FillFollowerSequentialEquivalence.
// Error verdicts follow the same rule: a transport failure re-enters
// to the follower's own fetch — a transient leader failure is not the
// follower's response. Only ErrFetchShed keeps its direct 503: the
// shed signals fetch-semaphore saturation, and a re-fetch would queue
// behind the same saturated semaphore.
//
// The re-entry guard (fillReentryUserKey) bounds the re-entry: the
// follower's own fetch, taken when the fill stored nothing usable for
// it, never parks on a fill flight again.
func (h *Handler) serveFillFollower(ctx *fasthttp.RequestCtx, lookupKey api.Key, ri RequestInfo, existing *inflightStream) {
	// The re-entry re-asks the owner: the pre-fill hints are stale once
	// the leader's fill has (or will) populated the owner.
	dropStaleOwnerHints(ctx)
	<-existing.stored
	// Carry the wave size to the own-fetch site: every follower of this
	// flight that ends up fetching its own (nothing usable was stored for
	// it) staggers over a window proportional to the wave instead of
	// fetching in the synchronized burst the settle signal would
	// otherwise produce.
	ctx.SetUserValue(fillWaveUserKey, int(existing.followers.Load()))
	res := existing.res
	if res.Err != nil {
		if errors.Is(res.Err, ErrFetchShed) {
			h.writeShed503(ctx, "MISS")
			// The leader shed without fetching: schedule the bounded
			// background refill so this shed is not a lost re-warm. The
			// follower does not re-fetch: its own acquire would wait on
			// the same saturated fetch semaphore, so the 503 is the
			// verdict a sequential request receives under the same load.
			h.triggerShedRefill(ri, lookupKey)
			return
		}
		// Every other error verdict — a transport failure, or the
		// leader's response being unshareable (SSE, variant overflow) —
		// means nothing usable was stored for this follower. It
		// fetches its own through the pipeline like a freshly-arrived
		// request (ADR-0066 sequential equivalence; the re-entry guard
		// bounds it to one fetch): inheriting the leader's error would
		// answer a concurrent request with another request's verdict.
		ctx.SetUserValue(fillReentryUserKey, true)
		h.ServeRequest(ctx)
		return
	}
	// The fill settled in the store (or was definitively unstorable).
	// Behave as a freshly-arrived request: identical gates, identical
	// variant selection, identical freshness and conditional handling.
	ctx.SetUserValue(fillReentryUserKey, true)
	h.ServeRequest(ctx)
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
	// Same corrected Age as the flight leader's response (RFC 9111
	// §4.2.3): origin Age + the leader's response delay. Followers of
	// one flight must not report a different initial age than the
	// leader, and res.Delay rides the shared fetchResult.
	setCorrectedAgeHeader(dst, res.Header, res.Delay)
	ctx.SetStatusCode(res.StatusCode)
	if !bytes.Equal(ctx.Method(), []byte("HEAD")) {
		ctx.Response.SetBodyRaw(res.Body)
	}
	h.applyResponseRewrites(dst)
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
	res := h.collapsedFetch(ctx, lookupKey, primaryKey, stale, ri)
	if res.Err != nil {
		if errors.Is(res.Err, ErrFetchShed) {
			// Shed — the fetch queue was full for fetchWaitTimeout. The
			// shed counter (incremented at the shed point) carries the
			// signal; keep this at Debug so a shed storm cannot INFO-spam.
			h.logger.Debug("stayin-alive: fetch wait timeout, serving stale",
				"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
				"key", lookupKey)
			// Stale protects this client, but the shed must not be a lost
			// refill: schedule the bounded background re-warm.
			h.triggerShedRefill(ri, lookupKey)
		} else {
			h.logger.Info("stayin-alive: upstream unreachable, serving stale indefinitely",
				"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
				"error", res.Err, "key", lookupKey)
		}
		h.serveObject(ctx, stale, now, cacheStale, src)
		return
	}
	if res.StatusCode >= 500 {
		h.logger.Info("stayin-alive: upstream 5xx, serving stale indefinitely",
			"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
			"status", res.StatusCode, "key", lookupKey)
		h.serveObject(ctx, stale, now, cacheStale, src)
		return
	}
	// RFC 9111 §4.3.5 (freshening with HEAD): a full origin response to
	// a HEAD on a stale-but-choosable entry (the no-validator Miss
	// decision lands here via revalidateOrMiss) updates the stored
	// entry's headers and freshness instead of being merely served
	// through. A non-freshenable response falls through to
	// writeAndMaybeStore's write-through.
	if bytes.Equal(ctx.Method(), []byte("HEAD")) &&
		h.freshenFromHead(ctx, res, lookupKey, stale, now, src, ri) {
		return
	}
	h.writeAndMaybeStore(ctx, res, primaryKey, stale, ri)
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
	revalReq.SetRequestURIBytes(h.originURI(ctx.RequestURI()))
	revalReq.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		revalReq.Header.AddBytesKV(k, v)
	}
	// Normalize AE to the bucket token so the revalidated variant
	// matches the key's bucket claim (see rewriteOutboundAE).
	h.rewriteOutboundAE(&revalReq.Header)
	// Client-identity headers (request.forwarded, issue #769) on the
	// outbound copy only — see applyForwardedCtx.
	h.applyForwardedCtx(&revalReq.Header, ctx)
	setConditionalHeaders(func(k, v string) { revalReq.Header.Set(k, v) }, stale)

	// Collapse concurrent revalidations for the same key. Each concurrent
	// request that finds the same stale object would otherwise fire its
	// own conditional origin request. The singleflight key is suffixed
	// with a constant to avoid colliding with regular fetch collapsing
	// while still deduplicating revalidations for the same cache key.
	// Start here, not inside doFetchBg, so the span carries this
	// request's method/path/pool/route attributes; each caller owns its
	// span.
	revalCtx, revalSpan := tracing.StartOriginSpan(
		tracing.SpanContextFromRequest(ctx),
		ctx.Method(), ctx.Path(), h.poolName, h.routeName,
	)
	// Join the client trace like the other foreground fetches; the
	// singleflight leader's span context is what gets injected.
	tracing.InjectFastHTTP(revalCtx, revalReq)
	res := h.collapsedRevalidateBg(revalCtx, revalReq, lookupKey, primaryKey, stale, ri)
	if res.Err != nil {
		tracing.RecordError(revalSpan, res.Err)
	}
	// Not deferred: a defer would extend the span over the post-fetch
	// serve work below, which is not the origin fetch.
	revalSpan.End()

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
				"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
				"error", res.Err, "key", stale.Key)
		} else {
			h.logger.Info("stayin-alive: upstream 5xx, serving stale indefinitely",
				"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
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
		writeUpstreamError(ctx, res.Err)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		h.applyResponseRewrites(&ctx.Response.Header)
		return
	}
	if res.StatusCode >= 500 {
		if staleFallbackAllowed(stale) {
			h.serveObject(ctx, stale, now, cacheStale, src)
			return
		}
	}

	if res.StatusCode == fasthttp.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res, ri, now)
		// Drift check before the store (ADR-0058): refreshed.VaryValue
		// carries the merged 304 declaration; purging first removes the
		// old resolver even if the refreshed variant re-lands elsewhere.
		h.detectVaryDrift(ctx, stale, ri, res.StatusCode, refreshed.VaryValue)
		h.storeObject(ctx, lookupKey, refreshed, ri, false, 0)
		// RFC 9111 §4.3.1 (validation model): a 304 from the origin means
		// the stored response's conditionals are satisfied. The client's
		// own conditional is evaluated against the freshened validators —
		// the complement of the spec's rule that a 304 whose ETag is not
		// in the client's If-None-Match list MUST produce a 200 for the
		// client. A matching If-None-Match / If-Modified-Since therefore
		// gets the 304 it asked for instead of a full re-served response.
		if h.tryConditional304Fast(ctx, refreshed, src, "REVALIDATED", cacheRevalidated, now, h.rewriteHook()) {
			return
		}
		h.serveObject(ctx, refreshed, now, cacheRevalidated, src)
		return
	}

	// RFC 9111 §4.3.5 (freshening with HEAD): the origin answered the
	// HEAD with a full response. The client is served the freshened
	// stored entry — retained headers plus the HEAD response's §3.2
	// updates — while the stored GET body stays in place (issue #752).
	// A non-freshenable response (mismatching validators, status other
	// than 200/410) falls through to the ordinary write-through. GET
	// requests always take the full-response handling below.
	if bytes.Equal(ctx.Method(), []byte("HEAD")) &&
		h.freshenFromHead(ctx, res, lookupKey, stale, now, src, ri) {
		return
	}

	// A 200 is a full fresh response: writeAndMaybeStore compares its
	// effectiveVary (the declaration that will be stored) against the
	// stored one, INSIDE the cacheability gate — an uncacheable response
	// must not purge the live surface it will not replace.
	h.writeAndMaybeStore(ctx, res, primaryKey, stale, ri)
}

// refreshFrom304 builds an updated copy of stale after a metadata-only
// validating response (a 304 Not Modified, or — via freshenFromHead — a
// response to a HEAD, RFC 9111 §4.3.5): it merges the validating
// response's headers (RFC 9111 §3.2) and recomputes the stored
// Cache-Control, TTL, OriginAge, ETag, and Last-Modified. The stored
// status and body are preserved; the caller stores the result with its
// own context. Shared by foreground revalidate, background SWR
// revalidation, and HEAD freshening so the recompute logic lives in
// exactly one place.
//
// stale.Header is cloned by CloneForRefresh before mutation: it is shared
// with any other goroutine that looked up the same object, and
// MergeHeaders304's writes would race with their reads.
func (h *Handler) refreshFrom304(stale *api.Object, res fetchResult, ri RequestInfo, now time.Time) *api.Object {
	refreshed := stale.CloneForRefresh()
	refreshed.StoredAt = now
	// Restamp the grace-retention pair from the route's CURRENT config
	// (ADR-0051): the refreshed object replaces the stored one, so the
	// flag must track the route's stayin_alive setting as of now —
	// inheriting the stale object's value would pin (or unpin) retention
	// based on the config at the original fill.
	refreshed.KeepGrace = h.stayinAlive
	refreshed.Pool = h.poolName
	// Reset Hits to 0 for the new TTL window. Object.Hits is a SIEVE
	// eviction signal; the per-window popularity gate uses windowHits
	// from the store, not Object.Hits.
	refreshed.Hits = 0
	MergeHeaders304(refreshed, res.Header.ToMap())
	// Recompute HasDate in case the 304 response added or changed Date.
	refreshed.HasDate = refreshed.Header.Has(header.Date)
	// effectiveVary keeps the stored VaryValue carrying the route's
	// include_headers union across revalidation: a 304 that drops Vary
	// (or is served by an origin that never sent it) must not collapse
	// an include-keyed variant onto the primary key.
	refreshed.VaryValue = effectiveVary(refreshed.Header, h.policy)
	// Recompute VaryKey from the same union: CloneForRefresh copies the
	// stale object's VaryKey verbatim, and a 304 that changes Vary
	// (MergeHeaders304 replaces the stored lines wholesale) would
	// otherwise leave the pair skewed — a mismatch the peer gates
	// (servePeerHit, peerGateMatchesVary) reject on sight, failing
	// every peer fetch of the object until TTL. Skew between the pair
	// is the same bug class buildObject guards against when it hashes
	// obj.VaryValue into obj.VaryKey from one computed value.
	// A union containing "*" anywhere is unkeyable the same way
	// lookup treats it: VariantKey* short-circuits to the primary key
	// on varyContainsStar (token anywhere in the list, not just a
	// pure-"*" value), so a "Vary: *" 304 merged over an include-keyed
	// object ("*, accept-language") must blank VaryKey for the pair to
	// stay consistent with what every lookup site computes.
	if refreshed.VaryValue != "" && !varyContainsStar(refreshed.VaryValue) {
		refreshed.VaryKey = BuildVaryKey(refreshed.VaryValue, ri.Header, h.policy)
	} else {
		refreshed.VaryKey = ""
	}
	// Recompute CacheControl string and parsed TTL from the updated headers.
	refreshed.CacheControl = refreshed.Header.Get(header.CacheControl)
	newCC := ParseCacheControl(refreshed.CacheControl)
	refreshed.RespNoCache = newCC.NoCache
	refreshed.RespMustRevalidate = newCC.MustRevalidate || newCC.ProxyRevalidate
	if ttl, ok := FreshnessLifetime(newCC, refreshed.Header.Get); ok {
		refreshed.TTL = ttl
	}
	// Re-apply route override (success responses only) so a 304 cannot
	// revert bouine's storage lifetime back to the upstream's (potentially
	// shorter) max-age. Error objects keep the ladder TTL from the merged
	// headers (or their previous TTL) — refreshed.StatusCode is the stale
	// object's status (ADR-0059).
	if ttl := h.fp.overrideTTLFor(refreshed.StatusCode); ttl > 0 {
		refreshed.TTL = ttl
	}
	// RFC 9111 §4.2.3: a 304 is an origin response too — its round-trip
	// delay ages the freshened object exactly like a full response's
	// (corrected_age_value = Age + response_delay), floored by apparent
	// age from Date skew. The revalidation fetch measures res.Delay;
	// without it a slow-origin 304 would restore full freshness to an
	// object whose validators were already response_delay seconds old.
	refreshed.OriginAge = correctedInitialAge(
		parseOriginAge(refreshed.Header), res.Delay,
		refreshed.Header.Get(header.Date), now)
	if newETag := res.Header.Get(header.ETag); newETag != "" {
		refreshed.ETag = newETag
	}
	// Recompute the parsed Last-Modified from the merged header line so
	// future revalidations send the merged validator (setConditionalHeaders
	// reads the parsed field, not the header map).
	if lm := refreshed.Header.Get(header.LastModified); lm != "" {
		if t, err := time.Parse(httpTimeFormat, lm); err == nil {
			refreshed.LastModified = t
		}
	}
	return refreshed
}

// freshenFromHead applies RFC 9111 §4.3.5 to a full (non-304) response
// received for a HEAD on a stored GET entry (revalidate path when the
// entry carries validators, stale-fallback path when it does not):
// the stored entry's header fields are updated from the HEAD response
// (§3.2 merge — received fields replace stored ones, absent fields are
// retained) and its freshness is recomputed, while the stored status
// and body are preserved (a HEAD exchange must never store a bodiless
// response, issue #752). The client is answered with the freshened
// entry's headers under the HEAD response's own status code, minus the
// body HEAD never carries.
//
// Returns true when it fully handled the request. false means the
// caller proceeds with the ordinary write-through (writeAndMaybeStore),
// which serves the origin response and never stores for a HEAD client:
//
//   - The §4.3.5 SHOULD gate: a HEAD response whose validators (ETag,
//     Last-Modified) or Content-Length do not match the stored entry
//     means the representations differ, so the stored response is
//     considered stale instead of freshened.
//   - Statuses other than 200 and 410: §4.3.5 specifies the update for
//     a 200, and the cache-tests suite extends it to 410
//     (head-410-update); redirects and other client errors must keep
//     reaching GET clients, not resurrect a stored 200 under them.
//   - A fetch that returned a body: defense in depth on top of the
//     method-separated flights (collapsedRevalidateBg,
//     collapsedFetch) — a bodied fetch is a full GET response, never
//     a HEAD exchange, and freshening from it would merge its headers
//     over the stored body (whose full copy that flight's leader
//     stores itself).
func (h *Handler) freshenFromHead(
	ctx *fasthttp.RequestCtx,
	res fetchResult,
	lookupKey api.Key,
	stale *api.Object,
	now time.Time,
	src api.Source,
	ri RequestInfo,
) bool {
	if res.StatusCode != fasthttp.StatusOK && res.StatusCode != fasthttp.StatusGone {
		return false
	}
	if len(res.Body) > 0 || !headFreshenAllowed(stale, res) {
		return false
	}
	// refreshFrom304 is the shared §3.2 merge machinery: it clones the
	// stale object (keeping status and body), merges the response's
	// headers, and recomputes freshness — identical requirements for a
	// HEAD response, whose headers are metadata-only.
	refreshed := h.refreshFrom304(stale, res, ri, now)
	// Drift check before the store (ADR-0058), mirroring the 304 branch.
	h.detectVaryDrift(ctx, stale, ri, res.StatusCode, refreshed.VaryValue)
	h.storeObject(ctx, lookupKey, refreshed, ri, false, 0)
	// The HEAD client's own conditional is evaluated against the
	// freshened entry, mirroring the 304 branch (§4.3.1).
	if h.tryConditional304Fast(ctx, refreshed, src, "REVALIDATED", cacheRevalidated, now, h.rewriteHook()) {
		return true
	}
	dst := &ctx.Response.Header
	fh := getOrComputeFastHeader(refreshed)
	fh.CopyTo(dst)
	var ageBuf [16]byte
	ageStr := strconv.AppendInt(ageBuf[:0], int64(ComputeAge(refreshed, now).Seconds()), 10)
	dst.SetCanonical(header.S2b(header.Age), ageStr)
	dst.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(src)))
	dst.SetCanonical(header.S2b(header.XCache), header.S2b("REVALIDATED"))
	// Write-through status: the client gets the HEAD response's status
	// (e.g. 410), while the stored entry keeps its own.
	ctx.SetStatusCode(res.StatusCode)
	h.applyResponseRewrites(dst)
	return true
}

// headFreshenAllowed implements the RFC 9111 §4.3.5 validator gate: the
// stored entry may be updated from a HEAD response only when every
// validator received in it matches the stored entry, and a received
// Content-Length matches the stored body's size. Any mismatch (or an
// unparseable value) means the representations differ — the caller
// leaves the stored response stale instead of freshening it.
func headFreshenAllowed(stale *api.Object, res fetchResult) bool {
	if e := res.Header.Get(header.ETag); e != "" && !etagMatch(e, stale.ETag) {
		return false
	}
	if lm := res.Header.Get(header.LastModified); lm != "" {
		lmTime := parseHTTPDate(lm)
		if lmTime.IsZero() || stale.LastModified.IsZero() || !stale.LastModified.Equal(lmTime) {
			return false
		}
	}
	if cl := res.Header.Get(header.ContentLength); cl != "" {
		n, err := strconv.Atoi(cl)
		if err != nil || int64(n) != stale.BodySize {
			return false
		}
	}
	return true
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
	// HEAD is remapped to GET: the background fetch must reproduce the
	// stored GET representation, and HEAD differs from GET only by the
	// omitted body (RFC 9110 §9.3.2) — fetching HEAD would refresh the
	// stored object with an empty body (issue #752). Same remap the fast
	// path applies (requestInfoFromRaw).
	bgReq := RequestInfo{
		Method: remapHeadToGet(ri.GetMethod()),
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
	// ADR-0068: resolvers never register for refresh; a stale resolver
	// here is a pre-upgrade registry entry. Its mirror variant refreshes
	// on its own cycle, and a full-200 refresh of the primary key would
	// re-store a body there — resurrecting the duplicate body #824
	// removed. Resolver identity is the protocol invariant, not
	// PairedKey (variants carry it too).
	if stale.IsVaryResolver() {
		return
	}
	revalReq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(revalReq)
	revalReq.Header.SetMethod(ri.GetMethod())
	revalReq.SetRequestURI(string(h.originURI([]byte(ri.GetURI()))))
	revalReq.Header.SetHost(ri.GetHost())
	ri.Header.Range(func(k, v string) bool {
		revalReq.Header.Set(k, v)
		return true
	})
	// Normalize AE to the bucket token so the revalidated variant
	// matches the key's bucket claim (see rewriteOutboundAE).
	h.rewriteOutboundAE(&revalReq.Header)
	// Client-identity headers (request.forwarded, issue #769) from the
	// captured request info — no XFF append on background fetches (see
	// applyForwardedInfo).
	h.applyForwardedInfo(&revalReq.Header, ri)
	setConditionalHeaders(func(k, v string) { revalReq.Header.Set(k, v) }, stale)

	// The origin span starts here, detached from any client trace: the
	// triggering client's response has already been served by the time
	// this runs, and its pipeline span is ended. Detached-by-design, but
	// still attribute-bearing (method/path/pool/route) so slow SWR
	// fetches are filterable in Tempo.
	spanCtx, span := tracing.StartOriginSpan(
		ctx, []byte(ri.GetMethod()), []byte(ri.GetPath()), h.poolName, h.routeName,
	)
	defer span.End()

	staleHits := h.store.WindowHits(key)

	res := h.collapsedFetchBg(spanCtx, revalReq, key, ri)
	if res.Err != nil {
		// The owner of the span records the failure: a fetch that never
		// succeeded must not export as a clean span (the revalidate
		// caller records its own; errors were silently green before).
		tracing.RecordError(span, res.Err)
		return
	}

	if res.StatusCode == fasthttp.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res, ri, time.Now())
		h.detectVaryDrift(ctx, stale, ri, res.StatusCode, refreshed.VaryValue)
		h.storeObject(ctx, key, refreshed, ri, true, staleHits)
		return
	}

	// Pre-parse Cache-Control/CDN-Cache-Control once instead of up to 6
	// times (IsCacheable parses, isCacheBlocked re-parses for hasCDN,
	// IsCacheableWithDefault re-parses again).
	bgResMap := res.Header.ToMap()
	bgParsed := newParsedResponse(res.StatusCode, ri.Header, bgResMap)

	if bgParsed.isCacheableWithDefault(h.fp.neg, h.fp.defaultTTL) {
		if h.skipStoreReason(bgResMap, len(res.Body)) != "" {
			return
		}
		h.detectVaryDrift(ctx, stale, ri, res.StatusCode, effectiveVary(bgResMap, h.policy))
		obj := h.buildObject(key, ri, res, bgResMap, nil, time.Now())
		h.storeObject(ctx, key, obj, ri, true, staleHits)
	}
}

func (h *Handler) writeAndMaybeStore(
	ctx *fasthttp.RequestCtx,
	res fetchResult,
	primaryKey api.Key,
	stale *api.Object,
	ri RequestInfo,
) {
	dst := &ctx.Response.Header
	res.Header.CopyToFastHTTP(dst)
	// Under sync the origin's CDC-family lines never reach a downstream
	// cache (ADR-0063 I3): the synthesized cap below is authoritative.
	if h.syncCDC {
		removeCDCFamily(dst)
	}
	// Overwrite origin-supplied attribution headers (prevent spoofing).
	dst.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
	dst.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
	// Cache the ToMap() result — used below for header lookups (Age,
	// Set-Cookie, Vary) and passed to buildObject. Each ToMap() call
	// invokes FromFastHTTP which allocates a new header.Map.
	resMap := res.Header.ToMap()
	// RFC 9111 §4.2.3 / §5.1: a cache generating a response computes
	// Age from the corrected initial age — origin Age plus the response
	// delay, floored by apparent age from Date skew. Replaces any origin
	// Age copied above with the corrected value.
	setCorrectedAgeHeader(dst, res.Header, res.Delay)
	ctx.SetStatusCode(res.StatusCode)
	if !bytes.Equal(ctx.Method(), []byte("HEAD")) {
		ctx.Response.SetBodyRaw(res.Body)
	}
	h.applyResponseRewrites(dst)

	// A HEAD exchange must never (re)store an object: the origin's
	// response to the miss carries no body, and HEAD shares the GET
	// cache key — storing would replace a live body with an empty one
	// served to every subsequent GET (issue #752). The revalidation
	// path freshens the stored entry from the HEAD response's headers
	// instead (RFC 9111 §4.3.5, freshenFromHead) — it never reaches this
	// guard. The streaming miss path applies its own guard
	// (streamMissBuffered's !isHEAD). Background fetchers
	// (doBackgroundRevalidate, doShedRefill) never reach this guard
	// either — they remap HEAD→GET when materializing their origin
	// request (remapHeadToGet) so the refresh reproduces the stored GET
	// representation instead.
	if bytes.Equal(ctx.Method(), []byte("HEAD")) {
		return
	}

	// Pre-parse Cache-Control/CDN-Cache-Control once instead of up to 6
	// times (IsCacheable parses, isCacheBlocked re-parses for hasCDN,
	// IsCacheableWithDefault re-parses again).
	parsed := newParsedResponse(res.StatusCode, ri.Header, resMap)

	// Downstream freshness sync (ADR-0063): the fill cap is computed at
	// header-processing time — the same final TTL (fillFinalTTL) the
	// object is stored with below, so the cap advertised downstream and
	// the stored clock never diverge — and applies to every
	// RFC-cacheable fill, stored or not (the operator storage gates
	// below only skip the store). Uncacheable-to-shared fills
	// (private, no-store, Vary:*, unauthorized) never synthesize: no
	// bouine clock exists, and a synthesized public line must never
	// override an origin privacy marker at a shared downstream cache
	// (RFC 9111 §5.2.2.5, §5.2.2.7).
	var fillTTL *time.Duration
	if h.syncCDC && parsed.isCacheableWithDefault(h.fp.neg, h.fp.defaultTTL) {
		ttl := h.fillFinalTTLFor(resMap, res.Delay, parsed, time.Now())
		var cdcBuf [88]byte
		dst.SetCanonical(header.S2b(header.CDNCacheControl),
			appendFillCDCDirectives(cdcBuf[:0], parsed.respCC, originEffectiveMaxAge(parsed.respCC, resMap), ttl))
		fillTTL = &ttl
	}

	if parsed.isCacheableWithDefault(h.fp.neg, h.fp.defaultTTL) {
		if h.skipStoreReason(resMap, len(res.Body)) != "" {
			return
		}
		// Drift check inside the store gate (ADR-0058): only the
		// declaration of a response that WILL be stored may purge the
		// stored one. fetchAndStoreStayinAlive passes stale=nil on the
		// cold-miss path, where detectVaryDrift is a no-op — a cold fill
		// never revalidates a declaration it does not have.
		h.detectVaryDrift(ctx, stale, ri, res.StatusCode, effectiveVary(resMap, h.policy))
		// primaryKey is passed in from lookup() to avoid a redundant
		// buildKey call on the same request.
		storeKey := primaryKey
		if vary := effectiveVary(resMap, h.policy); vary != "" {
			storeKey = VariantKey(primaryKey, vary, ri.Header, h.policy)
		}
		obj := h.buildObject(storeKey, ri, res, resMap, fillTTL, time.Now())
		if storeKey != primaryKey {
			// Enforce MaxVariants cap: skip storage if this primary key already
			// has MaxVariants distinct Vary variants. RFC 9110 §12.5.5 — unbounded
			// variants are a DoS vector.
			if !h.reserveVariantSlot(ctx, primaryKey, storeKey) {
				return
			}
			// ADR-0068: the variant carries its primary key so a node
			// storing it via the write-to-owner RPC can register it in
			// the primary's variant set (cluster-wide purge
			// reachability). Plain objects keep a zero PairedKey.
			obj.PairedKey = primaryKey
		}
		h.storeObject(ctx, storeKey, obj, ri, false, 0)
		// In strong mode, storeObject is a no-op for non-owners. Forward
		// the freshly fetched object to the owner so subsequent peer-fetches
		// from this or other non-owners hit instead of going to origin
		// (issue #509). The owner resolves from obj.Key (storeKey).
		h.forwardToOwnerIfRemote(ctx, obj)
		if storeKey != primaryKey {
			// The primary-key entry is the Vary resolver: metadata-only,
			// pointing at the variant it mirrors (ADR-0068, #824). The old
			// clone shared the body slice with the variant object, but the
			// hot store deep-copies per Put, so the alias stored a second
			// private copy of every page and Vary URLs cost 2x their body
			// in RAM.
			primaryObj := buildResolver(obj, primaryKey)
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
	return h.reserveVariantSlotMode(reqCtx, primaryKey, storeKey, false)
}

// reserveVariantSlotFromPeer registers a variant that landed on this
// handler via the write-to-owner RPC (ADR-0068). The fill ran on another
// node, so this node may not hold the primary-key resolver even though
// the variant set is live — the primary's owner and the variant's owner
// are independent hash-ring assignments. Skip the local primary probe:
// treating the absent primary as a stale incarnation here wipes live
// variants out of the set and makes a cluster-wide URL purge miss them.
// Registration ALWAYS succeeds, including past the MaxVariants cap:
// PeerPutHandler.Handle stores the object BEFORE the onStore callback
// runs, so a refused registration would leave a stored-but-untracked
// variant — servable via the two-step resolution yet invisible to the
// cluster-wide URL purge (RFC 9111 §4.4), the exact reachable-but-
// unkillable shape ADR-0068 exists to close. The set may therefore
// exceed the cap via peer-put registrations; growth is bounded by SIEVE
// evicting the stored variants (the dead-variant sweep prunes them on
// later registrations) and by Purge deleting the set. The cap stays a
// storage refusal on the local-fill path, where the object has not been
// stored yet and refusal is enforceable.
func (h *Handler) reserveVariantSlotFromPeer(reqCtx context.Context, primaryKey, storeKey api.Key) {
	h.reserveVariantSlotMode(reqCtx, primaryKey, storeKey, true)
}

// reconcileStaleIncarnation reconciles the variant set for a primary
// key whose incarnation may have been evicted. Called with variantMu
// held; releases it around the store probes — a lock must never span
// an I/O call — and reacquires before returning the (possibly
// pruned) set for primaryKey.
//
// Single-node (ownerFn nil): the node holds everything, so a primary
// absent from the store means a prior evicted incarnation — its whole
// set is stale and is wiped wholesale. The cheap, sound case.
//
// Cluster mode: the primary legitimately lives on ANOTHER node while
// THIS one stores its variants (the primary's owner and each
// variant's owner are independent ring assignments), so the primary's
// LOCAL absence proves nothing about the tracked variants. ADR-0068:
// a wholesale wipe here deleted LIVE registrations on
// variant-owning nodes — stored, servable, invisible to the
// cluster-wide URL purge, i.e. the exact reachable-but-unkillable
// shape this ADR exists to close (caught by the e2e suite's past-cap
// purge test). Liveness is decided per tracked variant instead: only
// entries gone from the LOCAL store are pruned.
func (h *Handler) reconcileStaleIncarnation(reqCtx context.Context, primaryKey api.Key, set map[api.Key]struct{}) map[api.Key]struct{} {
	h.variantMu.Unlock()
	probeCtx := context.WithoutCancel(reqCtx)
	pkObj, _, _ := h.store.Get(probeCtx, primaryKey)
	if pkObj != nil {
		h.variantMu.Lock()
		return set
	}
	if h.ownerFn == nil {
		h.variantMu.Lock()
		fresh := make(map[api.Key]struct{})
		h.variantSets[primaryKey] = fresh
		return fresh
	}
	keys := make([]api.Key, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return h.pruneDeadVariants(reqCtx, primaryKey, keys)
}

// pruneDeadVariants removes tracked variants that are gone from the
// local store (SIEVE/TTL eviction, explicit Delete, an already-run
// purge) and returns the surviving set for primaryKey. Called WITHOUT
// variantMu held; reacquires it before returning.
func (h *Handler) pruneDeadVariants(reqCtx context.Context, primaryKey api.Key, keys []api.Key) map[api.Key]struct{} {
	probeCtx := context.WithoutCancel(reqCtx)
	dead := make([]api.Key, 0, len(keys))
	for _, k := range keys {
		obj, _, _ := h.store.Get(probeCtx, k)
		if obj == nil {
			dead = append(dead, k)
		}
	}
	h.variantMu.Lock()
	set := h.variantSets[primaryKey]
	if set == nil {
		set = make(map[api.Key]struct{})
		h.variantSets[primaryKey] = set
	}
	for _, k := range dead {
		delete(set, k)
	}
	return set
}

// reserveVariantSlotMode is the shared implementation; fromPeer skips
// the stale-incarnation reconciliation (see reserveVariantSlotFromPeer)
// and registers past the cap — the peer-put object is already stored.
// Reconciliation exists for the local-fill path; in cluster mode it
// prunes per-variant liveness (a node may store variants of a primary
// it does not hold), and wipes wholesale only in single-node mode,
// where an absent primary really does retire the whole set.
func (h *Handler) reserveVariantSlotMode(reqCtx context.Context, primaryKey, storeKey api.Key, fromPeer bool) bool {
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
	// The set may hold entries from a prior incarnation of this
	// primary key (evicted by SIEVE or TTL). Without reconciliation,
	// variantSets grows without bound as primary keys are evicted and
	// stale entries cause incorrect MaxVariants enforcement. What
	// "stale" means depends on the deployment shape — see
	// reconcileStaleIncarnation.
	if len(set) > 0 && !fromPeer {
		set = h.reconcileStaleIncarnation(reqCtx, primaryKey, set)
	}
	if len(set) < h.maxVariants {
		set[storeKey] = struct{}{}
		h.variantMu.Unlock()
		return true
	}
	// Cap reached: snapshot keys, release lock, probe store for
	// liveness, reacquire. pruneDeadVariants locks on return.
	keys := make([]api.Key, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	h.variantMu.Unlock()
	set = h.pruneDeadVariants(reqCtx, primaryKey, keys)
	if len(set) < h.maxVariants || fromPeer {
		// fromPeer tracks past the cap (see reserveVariantSlotFromPeer):
		// the object is already in the store — an untracked variant would
		// be purge-proof. Local fills fall through to the refusal below.
		set[storeKey] = struct{}{}
		h.variantMu.Unlock()
		return true
	}
	h.logger.Warn("vary cap exceeded, skipping variant storage",
		"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
		"primary_key", primaryKey, "cap", h.maxVariants)
	if h.VaryCapHits != nil {
		h.VaryCapHits.Inc()
	}
	h.variantMu.Unlock()
	return false
}

// invalidateAndProxy handles POST/PUT/DELETE by fetching the origin
// response via FastClient, proxying it to the client, and invalidating
// cached entries for the affected resource.
func (h *Handler) invalidateAndProxy(ctx *fasthttp.RequestCtx) {
	if h.fastClient == nil {
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		h.applyResponseRewrites(&ctx.Response.Header)
		ctx.Error("upstream error: no fast client configured", fasthttp.StatusBadGateway)
		return
	}
	// Linked to the client trace: the fetch is part of the triggering
	// request.
	fetchCtx, span := tracing.StartOriginSpan(
		tracing.SpanContextFromRequest(ctx),
		ctx.Method(), ctx.Path(), h.poolName, h.routeName,
	)
	defer span.End()

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethodBytes(ctx.Method())
	req.SetRequestURIBytes(h.originURI(ctx.RequestURI()))
	req.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		req.Header.AddBytesKV(k, v)
	}
	// Client-identity headers (request.forwarded, issue #769) on the
	// outbound copy only — see applyForwardedCtx.
	h.applyForwardedCtx(&req.Header, ctx)
	if ctx.Request.Body() != nil {
		body := ctx.Request.Body()
		req.SetBodyRaw(body)
	}
	tracing.InjectFastHTTP(fetchCtx, req)

	if err := h.doFastFetch(req, resp); err != nil {
		tracing.RecordError(span, err)
		// writeUpstreamError runs first: ctx.Error resets the response
		// (fasthttp RequestCtx.Error), so the attribution headers must
		// be written after it — the same order as the sibling 50x
		// branches (revalidate, streamBypass, handleSSE).
		writeUpstreamError(ctx, err)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		h.applyResponseRewrites(&ctx.Response.Header)
		return
	}

	if h.maxResponseBytes > 0 && int64(len(resp.Body())) > h.maxResponseBytes {
		h.logger.Warn("upstream response exceeded max_response_bytes, aborting",
			"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
			"key", h.buildKey(ctx), "limit", h.maxResponseBytes)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		ctx.Error("upstream response too large", fasthttp.StatusBadGateway)
		return
	}

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
	h.applyResponseRewrites(dst)
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
	h.purgeAndBroadcast(ctx, key)

	// Evict Content-Location and Location URLs (RFC 9111 §4.4).
	for _, hdr := range []string{header.ContentLocation, header.Location} {
		if loc := string(resp.Header.Peek(hdr)); loc != "" {
			locKey := h.buildLocationKey(ctx, loc)
			if !locKey.IsZero() {
				h.purgeAndBroadcast(ctx, locKey)
			}
		}
	}

	// RFC 7234 §3: a POST response may be stored when it carries explicit
	// freshness and its Content-Location matches the request URI. RFC 9111
	// dropped this rule (no equivalent section), but the behavior is kept
	// intentionally for Varnish parity.
	h.maybeStorePostResponseFast(ctx, getRI, key, resp)
}

// purgeAndBroadcast invalidates key locally, then fans the purge out
// to cluster peers (issue #753). The broadcast fires regardless of the
// local purge's owned result: in strong mode the key's owner holds the
// object while this node may be a non-owner with an empty local store,
// and in eventual mode every node caches independently. No-op wiring
// (single-node) reduces to the plain local purge.
func (h *Handler) purgeAndBroadcast(ctx *fasthttp.RequestCtx, key api.Key) {
	_, _ = h.Purge(ctx, key)
	if h.purgeBroadcast != nil {
		h.purgeBroadcast(key)
	}
}

// maybeStorePostResponseFast stores a successful POST response under
// the GET key when it has explicit freshness and a matching
// Content-Location (RFC 7234 §3; dropped by RFC 9111, kept by design).
func (h *Handler) maybeStorePostResponseFast(ctx *fasthttp.RequestCtx, getRI RequestInfo, key api.Key, resp *fasthttp.Response) {
	// A 304 is never storable (RFC 9110 §15.4.5) — see IsCacheable's
	// guard; it carries no body and satisfies no subsequent request.
	if resp.StatusCode() == fasthttp.StatusNotModified {
		return
	}
	// Responses with client-state headers (Set-Cookie, Set-Login) are
	// per-user and never stored from a POST response.
	if clientStateInFastHTTP(&resp.Header) {
		return
	}
	loc := string(resp.Header.Peek(header.ContentLocation)) // RFC 7234 §3
	if loc == "" {
		return
	}
	body := make([]byte, len(resp.Body()))
	copy(body, resp.Body())
	hdr := header.FromFastHTTP(&resp.Header)
	res := fetchResult{
		StatusCode: resp.StatusCode(),
		Header:     fromHeaderMap(hdr),
		Body:       body,
	}
	now := time.Now()
	obj := h.buildObject(key, getRI, res, hdr, nil, now)
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
	if h.shouldRegisterRefresh(obj, ri) {
		// An error status covered by the negative-caching policy skips
		// proactive refresh: re-fetching an origin already returning
		// errors amplifies the outage.
		if h.fp.neg.Cacheable(obj.StatusCode) {
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
		h.refreshRegistry.Register(key, ri, varyHeader, h.refreshPersistCycles, h.policy)
		h.scheduler.Schedule(key, obj.StoredAt.Add(obj.TTL-h.refreshMargin))
	}
}

// triggerShedRefill schedules a bounded background origin fetch that
// stores the object WITHOUT serving any client: the caller already got
// its 503 + Retry-After. This is the post-ban re-warm allowance — a shed
// used to be a lost refill, so after a surrogate-key purge ban the miss
// storm pinned the hit ratio at the shed equilibrium until the ban
// expired. The refill runs on its own bounded pool (rewarmSem) instead
// of the foreground fetchSem it was just shed from: sharing that budget
// would keep the re-warm starved by the very demand it recovers from.
//
// Deduplication is the same inflight-stream collapse the foreground miss
// path uses: concurrent sheds for one key share one refill, and a refill
// that lands while a foreground fetch of the same key is still queued
// collapses into it. Bounded by defaultRewarmConcurrency; a full pool
// drops the refill (the next request re-triggers).
func (h *Handler) triggerShedRefill(ri RequestInfo, key api.Key) {
	// Bail out early if the handler is already shutting down.
	select {
	case <-h.done:
		return
	default:
	}
	if h.fastClient == nil {
		return
	}
	select {
	case h.rewarmSem <- struct{}{}:
	default:
		return // allowance full — the next request re-triggers
	}
	// Materialize the request fields now: ri's []byte fields alias the
	// *fasthttp.RequestCtx's internal buffers (requestInfoFromCtx),
	// which are reused by the next keep-alive request the moment the
	// handler returns. The background goroutine must not read them after
	// that. ri.Header is an owned header.Map (headerFromCtx copies) —
	// safe as is. HEAD is remapped to GET for the same reason as the SWR
	// revalidation goroutine: the refill must reproduce the stored GET
	// representation, never an empty HEAD body (issue #752).
	bgReq := RequestInfo{
		Method: remapHeadToGet(ri.GetMethod()),
		URI:    ri.GetURI(),
		Host:   ri.GetHost(),
		Path:   ri.GetPath(),
		Header: ri.Header,
		TLS:    ri.TLS,
	}
	bgCtx, bgCancel := context.WithCancel(context.Background())
	h.revalWg.Add(1)
	go func() {
		defer func() {
			h.revalWg.Done()
			<-h.rewarmSem
		}()
		defer bgCancel()
		// Cancel the refill if the handler is shutting down so we do not
		// call store.Put on a closed store.
		go func() {
			select {
			case <-h.done:
				bgCancel()
			case <-bgCtx.Done():
			}
		}()
		h.doShedRefill(bgCtx, bgReq, key)
	}()
}

// doShedRefill fetches the object from origin and stores it, serving no
// one. It collapses behind any concurrent foreground or background fetch
// for the same key via the shared singleflight group (same key space as
// collapsedFetch), so a refill landing while a foreground miss is queued
// costs zero extra origin load. Deliberately unslotted on fetchSem: the
// allowance IS this pool.
//
// The request URI is the client's own request line (path/query only,
// stripped of the route prefix); the dialed host is the operator-
// configured pool target selected by PoolFastClient.dispatch, never a
// request parameter. Same data flow as doFetchFast/doFetchBg (suppressed
// go/request-forgery alerts); see docs/security/threat-model.md §A10.
func (h *Handler) doShedRefill(ctx context.Context, ri RequestInfo, key api.Key) {
	// Rebuild the origin request from the materialized fields.
	//nolint:gosec // G106/CWE-918: origin target is operator-configured; see above
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(ri.GetMethod())
	// lgtm[go/request-forgery] — origin target is the configured pool; see docs/security/threat-model.md §A10
	req.SetRequestURI(string(h.originURI([]byte(ri.GetURI()))))
	req.Header.SetHost(ri.GetHost())
	ri.Header.Range(func(k, v string) bool {
		req.Header.Set(k, v)
		return true
	})
	// Normalize AE to the bucket token so the refilled variant matches
	// the key's bucket claim (see rewriteOutboundAE).
	h.rewriteOutboundAE(&req.Header)
	// Client-identity headers (request.forwarded, issue #769) from the
	// captured request info — no XFF append on background fetches (see
	// applyForwardedInfo).
	h.applyForwardedInfo(&req.Header, ri)

	// Deadline-based timeout, mirroring doFetchBg's transport deadline.
	// Detached root span: no client trace to join, but attribute-bearing
	// so slow refills are filterable in Tempo.
	spanCtx, span := tracing.StartOriginSpan(
		ctx, []byte(ri.GetMethod()), []byte(ri.GetPath()), h.poolName, h.routeName,
	)
	defer span.End()
	fetchCtx := spanCtx
	if h.fetchTimeout > 0 {
		var cancel context.CancelFunc
		fetchCtx, cancel = context.WithTimeout(spanCtx, h.fetchTimeout)
		defer cancel()
	}

	// Collapse with concurrent foreground misses for this key. The
	// foreground leader runs the slotted doFetch and stores on success;
	// if it sheds, our leader slot re-runs the fetch here — unslotted,
	// which is the point of the allowance. An authorized triggering
	// request skips the shared flight (collapseDenied, ADR-0052): the
	// refill carries its credentials, and an authorized response is
	// never shareable in-flight.
	var res fetchResult
	if collapseDenied(ri) {
		res = h.doRefillFetch(fetchCtx, req)
	} else {
		v, _, _ := h.flight.Do(key.SingleFlightKey(0), func() (any, error) {
			return h.doRefillFetch(fetchCtx, req), nil
		})
		res = v.(fetchResult)
	}
	// Detach the header.Map before buildObject mutates it (attribution
	// headers, Set-Cookie strip): the flight group shares one result
	// across all concurrent callers, and concurrent SetEntryRaw/Del on a
	// shared Map corrupt the entries/values pairing (index-out-of-range).
	// Same contract as collapsedFetch's ownedClone.
	res.Header = res.Header.ownedClone()
	if res.Err != nil {
		return
	}

	// Cacheability gate before storage, mirroring the buffered streaming
	// path's store gate (no client response is written here).
	resMap := res.Header.ToMap()
	parsed := newParsedResponse(res.StatusCode, ri.Header, resMap)
	if !parsed.isCacheableWithDefault(h.fp.neg, h.fp.defaultTTL) ||
		h.skipStoreReason(resMap, len(res.Body)) != "" {
		return
	}
	obj := h.buildObject(key, ri, res, resMap, nil, time.Now())
	h.storeObject(ctx, key, obj, ri, true, 0)
	h.forwardToOwnerIfRemote(ctx, obj)
}

// doRefillFetch performs one unslotted origin fetch with the same
// response handling as doFetchBg: bounded body copy, owned header.Map —
// the result must be safe to share with singleflight callers.
func (h *Handler) doRefillFetch(ctx context.Context, req *fasthttp.Request) (res fetchResult) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && errors.Is(err, ErrAbortHandler) {
				res = fetchResult{Err: ErrAbortHandler}
				return
			}
			panic(r) //nolint:forbidigo // re-panic for real (non-abort) panics
		}
	}()
	resp := fasthttp.AcquireResponse()
	if err := h.fastClient.Do(ctx, req, resp); err != nil {
		fasthttp.ReleaseResponse(resp)
		return fetchResult{Err: fmt.Errorf("origin fetch: %w", err)}
	}
	if h.maxResponseBytes > 0 && int64(len(resp.Body())) > h.maxResponseBytes {
		fasthttp.ReleaseResponse(resp)
		return fetchResult{Err: fmt.Errorf("upstream response exceeds %d bytes", h.maxResponseBytes)}
	}
	hdrMap := header.FromFastHTTP(&resp.Header)
	statusCode := resp.StatusCode()
	// Exact-size copy: the body may be stored in the cache, and the hot
	// tier pins slice slack for the object's lifetime.
	bodyCopy := make([]byte, len(resp.Body()))
	copy(bodyCopy, resp.Body())
	fasthttp.ReleaseResponse(resp)
	return fetchResult{
		StatusCode: statusCode,
		Header:     fromHeaderMap(hdrMap),
		Body:       bodyCopy,
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
		if h.upstream != nil {
			// Upstream fallback (issue #598): a cached-static-route
			// handler is wired with Upstream (the staticfile handler)
			// and no FastClient. Materialize the upstream response
			// through a scratch RequestCtx, then convert it to a
			// fetchResult. Miss-path only: the interface check stays
			// off the hit path.
			return h.fetchViaUpstream(ctx)
		}
		return fetchResult{Err: fmt.Errorf("no fast client configured")}
	}
	// Span context stored by the middleware, never the RequestCtx (whose
	// lifetime ends at handler return).
	fetchCtx, span := tracing.StartOriginSpan(
		tracing.SpanContextFromRequest(ctx),
		ctx.Method(), ctx.Path(), h.poolName, h.routeName,
	)
	defer span.End()

	if err := h.acquireFetchSlot(); err != nil {
		return fetchResult{Err: err}
	}
	defer func() { <-h.fetchSem }()

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	// resp is released inside doFetchFast after the headers are detached
	// into an owned header.Map and the body copied: the result is shared
	// with singleflight followers, so it must not alias pooled state.

	// Populate the request from *fasthttp.RequestCtx.
	// Use *Bytes variants to avoid string([]byte) conversions.
	req.Header.SetMethodBytes(ctx.Method())
	req.SetRequestURIBytes(h.originURI(ctx.RequestURI()))
	req.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		req.Header.AddBytesKV(k, v)
	}
	// Normalize AE to the bucket token so the stored variant matches
	// the key's bucket claim (see rewriteOutboundAE).
	h.rewriteOutboundAE(&req.Header)
	// Client-identity headers (request.forwarded, issue #769): applied
	// to the outbound copy only — never the ctx, so the cache key, the
	// Vary variant key, and the stored RequestInfo stay untouched.
	h.applyForwardedCtx(&req.Header, ctx)
	// Inject W3C TraceContext.
	tracing.InjectFastHTTP(fetchCtx, req)

	fetchStart := time.Now()
	if err := h.doFastFetch(req, resp); err != nil {
		fasthttp.ReleaseResponse(resp)
		tracing.RecordError(span, err)
		return fetchResult{Err: fmt.Errorf("origin fetch: %w", err)}
	}

	if h.maxResponseBytes > 0 && int64(len(resp.Body())) > h.maxResponseBytes {
		fasthttp.ReleaseResponse(resp)
		return fetchResult{Err: fmt.Errorf("upstream response exceeds %d bytes", h.maxResponseBytes)}
	}

	statusCode := resp.StatusCode()
	bodyCopy := make([]byte, len(resp.Body()))
	copy(bodyCopy, resp.Body())

	// Detach into an owned header.Map and release the pooled response
	// before returning. collapsedFetch shares this result with every
	// singleflight caller; keeping fastResp alive here meant each caller's
	// releaseFetchResult re-released the SAME pooled response, corrupting
	// fasthttp's response pool (the same *fasthttp.Response handed to two
	// requests — one parsing origin headers into it while the other
	// iterated All() in FromFastHTTP, panicking with "index out of range"
	// on the miss path). The conversion cost is miss-path only.
	hdrMap := header.FromFastHTTP(&resp.Header)
	fasthttp.ReleaseResponse(resp)

	return fetchResult{
		StatusCode: statusCode,
		Header:     fromHeaderMap(hdrMap),
		Body:       bodyCopy,
		Delay:      time.Since(fetchStart),
	}
}

// fetchViaUpstream materializes a fetchResult by running the configured
// Upstream fasthttp.RequestHandler in-process (issue #598). A
// cached-static-route handler is wired with Upstream (the staticfile
// handler) and no FastClient, so when the cache cannot answer (MISS,
// revalidation, BYPASS, SSE) the upstream handler is the origin. The
// request is replayed into a scratch RequestCtx carrying the same
// method/URI/host/headers/body as the client's, and the upstream's
// response is converted to the shared fetchResult shape.
func (h *Handler) fetchViaUpstream(ctx *fasthttp.RequestCtx) (res fetchResult) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && errors.Is(err, ErrAbortHandler) {
				res = fetchResult{Err: ErrAbortHandler}
				return
			}
			panic(r) //nolint:forbidigo // re-panic for real (non-abort) panics
		}
	}()
	if h.fetchTimeout > 0 {
		deadline := time.NewTimer(h.fetchTimeout)
		defer deadline.Stop()
		done := make(chan struct{}, 1)
		var upstreamCtx *fasthttp.RequestCtx
		var panicVal any
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicVal = r
				}
				done <- struct{}{}
			}()
			upstreamCtx = &fasthttp.RequestCtx{}
			h.runUpstream(ctx, upstreamCtx)
			done <- struct{}{}
		}()
		select {
		case <-done:
			if panicVal != nil {
				panic(panicVal) //nolint:forbidigo // re-panic: the recover at the top of fetchViaUpstream maps ErrAbortHandler; the same convention as doFetchFast
			}
			defer upstreamCtx.Response.Reset()
			return upstreamFetchResult(upstreamCtx)
		case <-deadline.C:
			return fetchResult{Err: fmt.Errorf("origin fetch: timeout after %s", h.fetchTimeout)}
		}
	}
	upstreamCtx := &fasthttp.RequestCtx{}
	defer upstreamCtx.Response.Reset()
	h.runUpstream(ctx, upstreamCtx)
	return upstreamFetchResult(upstreamCtx)
}

// runUpstream replays the client request into a scratch RequestCtx and
// invokes the upstream handler. The request is replayed via fasthttp's
// Request.Copy (which owns its buffers), because the scratch ctx outlives
// the client's conn-owned buffer on streaming paths. No dialing happens
// here: the scratch request only feeds the wired in-process handler,
// never a network client — the same data flow the FastClient paths
// already carry (suppressed go/request-forgery alerts on doFetchFast/
// doFetchBg share it). See docs/security/threat-model.md §A10: origin
// targets are operator-configured, never derived from request parameters.
func (h *Handler) runUpstream(ctx *fasthttp.RequestCtx, upstreamCtx *fasthttp.RequestCtx) {
	upstreamCtx.Request.Reset()
	ctx.Request.CopyTo(&upstreamCtx.Request)
	upstreamCtx.Request.SetRequestURIBytes(h.originURI(ctx.Request.URI().RequestURI()))
	h.upstream(upstreamCtx)
}

// upstreamFetchResult converts a scratch RequestCtx produced by an
// in-process upstream handler into the shared fetchResult shape.
// Exact-size body copy: the result is shared with singleflight
// followers and may be stored, so it must not alias the scratch ctx.
func upstreamFetchResult(ctx *fasthttp.RequestCtx) fetchResult {
	hdrMap := header.FromFastHTTP(&ctx.Response.Header)
	statusCode := ctx.Response.StatusCode()
	bodyCopy := make([]byte, len(ctx.Response.Body()))
	copy(bodyCopy, ctx.Response.Body())
	return fetchResult{
		StatusCode: statusCode,
		Header:     fromHeaderMap(hdrMap),
		Body:       bodyCopy,
	}
}

// fetchViaUpstreamRequest is the request-shaped variant of fetchViaUpstream:
// the caller (revalidate / background refresh) already built a complete
// upstream-bound request (method, stripped URI, host, headers, body,
// conditional headers), so it is replayed verbatim into a scratch
// RequestCtx and handed to the in-process upstream handler. No dialing
// happens here: the scratch request only feeds the wired handler, never a
// network client — the same data flow the FastClient paths already have
// (suppressed alerts on doFetchFast/doFetchBg share it). See
// docs/security/threat-model.md §A10: origin targets are
// operator-configured, never derived from request parameters.
func (h *Handler) fetchViaUpstreamRequest(req *fasthttp.Request) (res fetchResult) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && errors.Is(err, ErrAbortHandler) {
				res = fetchResult{Err: ErrAbortHandler}
				return
			}
			panic(r) //nolint:forbidigo // re-panic for real (non-abort) panics
		}
	}()
	upstreamCtx := &fasthttp.RequestCtx{}
	defer upstreamCtx.Response.Reset()
	req.CopyTo(&upstreamCtx.Request)
	if h.fetchTimeout > 0 {
		deadline := time.NewTimer(h.fetchTimeout)
		defer deadline.Stop()
		done := make(chan struct{}, 1)
		var panicVal any
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicVal = r
				}
				done <- struct{}{}
			}()
			h.upstream(upstreamCtx)
			done <- struct{}{}
		}()
		select {
		case <-done:
			if panicVal != nil {
				panic(panicVal) //nolint:forbidigo // re-panic: the recover at the top of fetchViaUpstreamRequest maps ErrAbortHandler; the same convention as doFetchFast
			}
			return upstreamFetchResult(upstreamCtx)
		case <-deadline.C:
			return fetchResult{Err: fmt.Errorf("origin fetch: timeout after %s", h.fetchTimeout)}
		}
	}
	h.upstream(upstreamCtx)
	return upstreamFetchResult(upstreamCtx)
}

// skipStoreReason reports why a cacheable fill response must not be
// stored, or "" when it may be: client-state headers (Set-Cookie,
// Set-Login) under the default gate, then the admission size cap. The
// pair was repeated verbatim at every fill gate; the returned string is
// also the refresh path's skip metric label.
func (h *Handler) skipStoreReason(resMap header.Map, bodyLen int) string {
	if !h.allowSetCookie && clientStateInMap(resMap) {
		return "set_cookie"
	}
	if h.maxObjectSize > 0 && int64(bodyLen) > h.maxObjectSize {
		return "too_large"
	}
	return ""
}

// buildObject assembles the stored api.Object for a fetched response.
// Route knobs never vary per call: the freshness ladder lives on
// h.fp (fillPolicy), the identity stamps (key policy, stayin-alive,
// upstream pool) on the Handler itself. Keeping both off the parameter
// list removes the adjacent time.Duration run whose transposition
// compiled and silently served wrong grace windows.
//
//nolint:gocyclo // 16: TTL/freshness conditionals are inherently branchy
func (h *Handler) buildObject(key api.Key, ri RequestInfo, res fetchResult, resMap header.Map, presetTTL *time.Duration, now time.Time) *api.Object {
	// Parse Cache-Control (may be multiple headers — merge first).
	// CDN-Cache-Control overrides Cache-Control for shared caches (RFC 9213):
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
		ccHeader = resMap.GetAll(header.CDNCacheControl)
	} else {
		respCC = ParseCacheControl(ccHeader)
	}
	// RFC 9111 §4.2.3: corrected_initial_age = max(apparent_age,
	// corrected_age_value) where corrected_age_value = Age +
	// response_delay. The response delay (res.Delay) makes a
	// slow-origin response already aged on arrival; apparent age is
	// derived from the Date header and guards against clock skew
	// between cache and origin.
	dateStr := resMap.Get(header.Date)
	hasDate := dateStr != ""
	originAge := correctedInitialAge(parseOriginAge(resMap), res.Delay, dateStr, now)
	// computeTTL consolidates heuristic, fallback, negative, jitter, and Age subtraction.
	// presetTTL (non-nil) is a caller-computed final TTL: the ADR-0063
	// sync-mode fill cap is computed at header-processing time and must
	// advertise the exact clock the object is stored with, so the caller
	// computes fillFinalTTL once and passes it in (the streaming fill
	// paths flush the head before the store runs, so the value must
	// exist before the body streams). Nil = compute here, as before.
	var ttl time.Duration
	if presetTTL != nil {
		ttl = *presetTTL
	} else {
		ttl = h.fp.fillFinalTTL(resMap, res.StatusCode, respCC, originAge, now)
	}

	// res.Body is already an independently-owned copy: all production
	// callers (doFetchFast, streamMissBuffered, streamMissTee,
	// doFetchBg, maybeStorePostResponseFast) copy the body from the
	// pooled fasthttp.Response buffer before calling buildObject.
	// Using res.Body directly avoids a redundant make+copy per miss.
	obj := &api.Object{
		Key:          key,
		StatusCode:   res.StatusCode,
		Header:       resMap,
		Body:         res.Body,
		BodySize:     int64(len(res.Body)),
		StoredAt:     now,
		TTL:          ttl,
		ETag:         resMap.Get(header.ETag),
		CacheControl: ccHeader,  // Lead 1: pre-stored, avoids re-parsing on every hit
		OriginAge:    originAge, // Lead 3: pre-stored, avoids re-parsing on the read path
		HasDate:      hasDate,
		// effectiveVary, not joinedVary/Get: Vary is list-based, so field
		// lines split across multiple headers combine per RFC 9110 §5.2, and
		// cache.key.include_headers fields are unioned in. The union is
		// computed once so VaryValue and VaryKey below see the identical
		// value — skew between them breaks the peer variant gates
		// (handler.go handleCacheMiss, fastpath.go peerGateMatchesVary).
		VaryValue:          effectiveVary(resMap, h.policy),
		RespNoCache:        respCC.NoCache,
		RespMustRevalidate: respCC.MustRevalidate || respCC.ProxyRevalidate,
		// P1b grace retention stamp (ADR-0051): the object carries the
		// route's stayin_alive intent so the storage layer can gate
		// time-based reaping on origin-pool health without any route
		// knowledge. Two field assignments on the miss path; zero hit
		// path cost.
		KeepGrace: h.stayinAlive,
		Pool:      h.poolName,
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
	// Client-state headers are always stripped from cached objects:
	// joining multiple Set-Cookie values with ", " is non-conformant per
	// RFC 9110 §5.2, and serving another client's session state
	// (Set-Cookie, Set-Login) is a security risk. AllowSetCookie controls
	// whether responses carrying them are cached at all (gated above);
	// it does not preserve them in the cache (ADR-0012, ADR-0067).
	obj.Header.Del(header.SetCookie)
	obj.Header.Del(header.SetLogin)
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
	} else if h.fp.defaultSWR > 0 {
		obj.StaleWhileRevalidate = h.fp.defaultSWR
	}
	if respCC.StaleIfErrorSet {
		obj.StaleIfError = respCC.StaleIfError
	} else if h.fp.defaultSIE > 0 {
		obj.StaleIfError = h.fp.defaultSIE
	}
	if lm := obj.Header.Get(header.LastModified); lm != "" {
		if t, err := time.Parse(httpTimeFormat, lm); err == nil {
			obj.LastModified = t
		}
	}
	obj.VaryKey = BuildVaryKey(obj.VaryValue, ri.Header, h.policy)

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
// headers (X-Bouine-*), no-cache fields stripped per RFC 9111 §5.2.2.4,
// and fields listed in the stored response's Connection header
// (RFC 9110 §7.6.1 — the fast path must match the slow path's
// stripConnectionListedHeaders).
// Under downstream_freshness: sync (skipCDC, ADR-0063 I3) the origin's
// RFC 9213-family and Surrogate-Control lines are excluded: the
// synthesized CDN-Cache-Control (appendDynamicHeaders) is authoritative.
func serializeHead(obj *api.Object, skipCDC bool) []byte {
	noCacheFields := parseNoCacheFieldNames(obj.CacheControl)
	var connListed map[string]bool
	if obj.HasConnectionList {
		connListed = parseConnectionListedHeaders(obj)
	}
	buf := make([]byte, 0, 512)
	n := obj.Header.Len()
	for i := 0; i < n; i++ {
		key, value := obj.Header.At(i)
		if skipStaticHeader(key, noCacheFields, connListed, skipCDC) {
			continue
		}
		buf = append(buf, key...)
		buf = append(buf, ": "...)
		buf = append(buf, value...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

// fillPolicy owns the route's freshness knobs applied on every fill
// path — the negative-caching table, the default/override TTLs, the
// SWR/SIE floors stamped onto stored objects, and the TTL jitter.
// buildObject, the ADR-0063 sync-mode fill cap, and the 304-refresh
// override all consume it, so the stored clock, the advertised cap,
// and the revalidated lifetime can never diverge. The knobs previously
// rode call sites as adjacent time.Duration parameters — a transposed
// pair compiled and silently served wrong grace windows.
type fillPolicy struct {
	neg         *StatusTTL
	defaultTTL  time.Duration // operator fallback when origin sends no freshness
	overrideTTL time.Duration // wins over origin freshness for content responses (status < 400); error statuses keep the negative-caching ladder (ADR-0059)
	defaultSWR  time.Duration // operator-level stale-while-revalidate floor
	defaultSIE  time.Duration // operator-level stale-if-error floor
	jitterPct   int
}

// fillFinalTTL computes bouine's final TTL for a fill response: the
// computeTTL ladder, then the route override for content statuses.
// Single definition — buildObject and the ADR-0063 sync-mode fill cap
// both call it, so the cap advertised downstream and the stored clock
// can never diverge.
func (fp *fillPolicy) fillFinalTTL(resMap header.Map, status int, respCC Directives,
	originAge time.Duration, now time.Time) time.Duration {
	ttl := fp.computeTTL(resMap, status, respCC, originAge, now)
	if ov := fp.overrideTTLFor(status); ov > 0 {
		ttl = ov
	}
	return ttl
}

// overrideTTLFor returns the jittered route override for a content
// status, or 0 when the route sets none. The route-level override wins
// over the upstream's freshness directives for content responses
// (status < 400); error responses keep the negative-caching TTL ladder
// (ADR-0059). Applied after the ladder so jitter is seeded from the
// override value, not the origin's max-age.
func (fp *fillPolicy) overrideTTLFor(status int) time.Duration {
	if fp.overrideTTL <= 0 || status >= 400 {
		return 0
	}
	return JitterTTL(fp.overrideTTL, fp.jitterPct)
}

// computeTTL derives the freshness lifetime for a response. Precedence
// when the origin sends no explicit freshness (max-age/s-maxage/valid
// Expires): per-status negative TTL (negative_ttl) for the statuses it
// covers > heuristic TTL (Last-Modified) > operator default TTL. The
// per-status policy wins first for covered statuses: caching a 5xx for
// hours because the origin echoes Last-Modified, or for ttl_default's
// duration instead of the operator's 10s, is exactly what the policy
// exists to prevent. Uncovered statuses fall through to the heuristic
// and the operator default, in that order. Jitter and the origin Age
// adjustment apply last.
func (fp *fillPolicy) computeTTL(hdr header.Map, status int, respCC Directives,
	originAge time.Duration, now time.Time) time.Duration {
	ttl, explicit := FreshnessLifetimeH(respCC, hdr)
	if !explicit {
		// Per-status negative caching outranks every other implicit
		// source (Cloudflare's "Cache TTL by status code" semantics),
		// but only for the statuses it covers.
		if negTTL := fp.neg.TTL(status); negTTL > 0 {
			ttl = negTTL
		} else {
			ttl = HeuristicTTL(hdr, now)
			if ttl == 0 && fp.defaultTTL > 0 {
				ttl = fp.defaultTTL
			}
		}
	}
	ttl = JitterTTL(ttl, fp.jitterPct)
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

// rewriteHook returns the response-rewrite applier when the route
// configures any response rewrite directive; nil otherwise. Used by
// package-level response writers (write304Fast) that have no handler
// reference.
func (h *Handler) rewriteHook() func(*fasthttp.ResponseHeader) {
	if h.respHeaderRemove == nil && h.respHeaderSet == nil {
		return nil
	}
	return h.applyResponseRewrites
}
