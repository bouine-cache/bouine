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
	"bytes"
	"context"
	"log/slog"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/thylong/bouine/internal/observability/tracing"
	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/api"
)

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
)

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
	if uint(secs) < uint(len(ageHeaderCache)) {
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

// recorderPool reuses responseRecorder instances on the miss and
// invalidation paths. Both paths copy the captured body and header out of
// the recorder before returning it to the pool, so the recorder's buffer
// and header map are reused across requests rather than reallocated per
// fetch. The hit path never allocates a recorder.
var recorderPool = sync.Pool{
	New: func() any {
		return &responseRecorder{
			header: make(http.Header, 8),
			body:   &bytes.Buffer{},
		}
	},
}

func acquireRecorder() *responseRecorder {
	rec := recorderPool.Get().(*responseRecorder)
	rec.statusCode = 0
	rec.body.Reset()
	for k := range rec.header {
		delete(rec.header, k)
	}
	return rec
}

func releaseRecorder(rec *responseRecorder) {
	recorderPool.Put(rec)
}

// Handler is the caching HTTP handler. It wraps an upstream
// http.Handler (the origin pool) and a storage.Store.
type Handler struct {
	upstream         http.Handler
	store            storage.Store
	flight           singleflight.Group
	logger           *slog.Logger
	negativeTTL      time.Duration
	jitterPercent    int
	stayinAlive      bool
	defaultTTL       time.Duration   // operator fallback when origin sends no freshness
	overrideTTL      time.Duration   // operator override; wins over origin max-age/Expires when > 0
	defaultSWR       time.Duration   // operator-level stale-while-revalidate floor
	defaultSIE       time.Duration   // operator-level stale-if-error floor
	allowSetCookie   bool            // when false (default), Set-Cookie blocks caching
	maxObjectSize    int64           // skip storage for responses larger than this; 0 = no limit
	stripQueryParams map[string]bool // query params to exclude from cache key; nil = none
	excludeHeaders   map[string]bool // headers to exclude from Vary variant key; nil = none
	// variantCounts tracks stored Vary variants per primary key to
	// enforce MaxVariants cap. Protected by variantMu.
	variantMu     sync.Mutex
	variantCounts map[api.Key]int
	// VaryCapHits is incremented when a variant is rejected; nil-safe.
	VaryCapHits interface{ Inc() }
	// ownerFn returns the peer that owns a cache key and whether the key
	// is local to this node. Nil in single-node mode.
	ownerFn func(key api.Key) (owner api.PeerInfo, isLocal bool)
	// peerFetch asks a peer for a cached object. Returns nil, nil on
	// peer miss; errors fall through to origin. Nil in single-node mode.
	peerFetch func(ctx context.Context, peer api.PeerInfo, key api.Key) (*api.Object, error)
	// replicateFn, if non-nil, is called after a cacheable response is
	// stored locally. Used in full cluster mode to broadcast the object
	// to all peers via gossip. Nil in strong and eventual modes.
	replicateFn func(ctx context.Context, obj *api.Object)
}

// HandlerConfig configures a cache Handler.
type HandlerConfig struct {
	Upstream http.Handler
	Store    storage.Store
	Logger   *slog.Logger
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
	// ReplicateFn, if non-nil, is called after a cacheable response is
	// stored locally. Used in full cluster mode to broadcast the object
	// to all peers via gossip. Nil in strong and eventual modes.
	ReplicateFn func(ctx context.Context, obj *api.Object)
}

// NewHandler creates a caching handler.
func NewHandler(cfg HandlerConfig) *Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	h := &Handler{
		upstream:         cfg.Upstream,
		store:            cfg.Store,
		logger:           cfg.Logger,
		negativeTTL:      cfg.NegativeTTL,
		jitterPercent:    cfg.JitterPercent,
		stayinAlive:      cfg.StayinAlive,
		defaultTTL:       cfg.DefaultTTL,
		overrideTTL:      cfg.OverrideTTL,
		defaultSWR:       cfg.DefaultSWR,
		defaultSIE:       cfg.DefaultSIE,
		variantCounts:    make(map[api.Key]int),
		VaryCapHits:      cfg.VaryCapHits,
		ownerFn:          cfg.OwnerFn,
		peerFetch:        cfg.PeerFetch,
		replicateFn:      cfg.ReplicateFn,
		allowSetCookie:   cfg.AllowSetCookie,
		maxObjectSize:    cfg.MaxObjectSize,
		stripQueryParams: cfg.StripQueryParams,
		excludeHeaders:   cfg.ExcludeHeaders,
	}
	return h
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

	// Fast-path: skip key computation and store lookup entirely for
	// requests whose Cache-Control: no-store directive guarantees we
	// will never serve from — or store into — the cache.
	if reqHasNoStore(r) {
		h.handleBypass(w, r)
		return
	}

	// L4 span: cache engine layer.
	ctx, span := tracing.StartSpan(r.Context(), "bouine.cache")
	defer span.End()
	r = r.WithContext(ctx)

	// Take a single timestamp; thread it through Evaluate and serve
	// functions to avoid a second time.Now() syscall per hit.
	now := time.Now()
	key, obj := h.lookup(r)
	disp := Evaluate(r, obj, now)

	switch disp.Decision {
	case Hit, StaleHit:
		if h.tryConditional304(w, r, disp.Object) {
			return
		}
		if ServeRange(w, r, disp.Object) {
			return
		}
		h.serveObject(w, r, disp.Object, now, cacheHit)
		if disp.Decision == StaleHit && disp.Object.StaleWhileRevalidate > 0 {
			h.triggerBgRevalidate(r, key, disp.Object)
		}
	case Miss:
		h.handleCacheMiss(w, r, key, obj, now)
	case Revalidate:
		h.revalidate(w, r, key, disp.Object)
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
func (h *Handler) handleCacheMiss(w http.ResponseWriter, r *http.Request, key api.Key, obj *api.Object, now time.Time) {
	if h.ownerFn != nil && h.peerFetch != nil {
		if owner, isLocal := h.ownerFn(key); !isLocal {
			if peerObj, err := h.peerFetch(r.Context(), owner, key); err == nil && peerObj != nil {
				// Re-evaluate: the peer may have returned a stale object.
				if d2 := Evaluate(r, peerObj, now); d2.Decision == Hit || d2.Decision == StaleHit {
					h.serveObject(w, r, peerObj, now, cacheHit)
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
		h.fetchAndStoreStayinAlive(w, r, key, obj)
	} else {
		h.fetchAndStore(w, r, key)
	}
}

// reqHasNoStore returns true when the request's Cache-Control header
// contains the no-store directive (RFC 9111 §5.2.1.5), meaning neither
// a cache lookup nor storage is applicable. Uses a token scan to avoid
// a full ParseCacheControl allocation.
func reqHasNoStore(r *http.Request) bool {
	cc := r.Header.Get("Cache-Control")
	if cc == "" {
		return false
	}
	for cc != "" {
		var tok string
		if i := strings.IndexByte(cc, ','); i >= 0 {
			tok, cc = cc[:i], cc[i+1:]
		} else {
			tok, cc = cc, ""
		}
		tok = strings.TrimSpace(tok)
		if strings.EqualFold(tok, "no-store") {
			return true
		}
	}
	return false
}

// lookup resolves the cache key and stored object for r, accounting
// for Vary-based secondary keys.
func (h *Handler) lookup(r *http.Request) (api.Key, *api.Object) {
	key := h.buildKey(r)
	obj, err := h.store.Get(r.Context(), key)
	if err != nil {
		h.logger.Warn("cache lookup error", "error", err)
	}
	if obj == nil || obj.Header.Get("Vary") == "" {
		return key, obj
	}
	vk := VariantKey(key, obj.Header.Get("Vary"), r.Header, h.excludeHeaders)
	if vk == key {
		return key, obj
	}
	vobj, verr := h.store.Get(r.Context(), vk)
	if verr == nil && vobj != nil {
		return vk, vobj
	}
	return vk, nil
}

// tryConditional304 returns true and writes a 304 if the client's
// conditional headers match the cached object. Used for both hit and
// revalidate paths.
func (h *Handler) tryConditional304(w http.ResponseWriter, r *http.Request, obj *api.Object) bool {
	if !ClientConditionalMatch(r, obj) {
		return false
	}
	if obj.ETag != "" {
		w.Header().Set("ETag", obj.ETag)
	}
	w.Header().Set("X-Cache", "HIT")
	w.WriteHeader(http.StatusNotModified)
	return true
}

func (h *Handler) handleBypass(w http.ResponseWriter, r *http.Request) {
	// only-if-cached with no cached response → 504 Gateway Timeout
	// (RFC 9111 §5.2.1.7).
	reqCC := ParseCacheControl(mergeHeaderValues(r.Header, "Cache-Control"))
	if reqCC.OnlyIfCached {
		http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		return
	}
	w.Header()["X-Cache"] = headerBYPASS
	h.upstream.ServeHTTP(w, r)
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
// RFC 7234 §5.5.3.
func (h *Handler) serveObject(w http.ResponseWriter, r *http.Request, obj *api.Object, now time.Time, result cacheResult) {
	dst := w.Header()
	maps.Copy(dst, obj.Header)
	dst["Age"] = ageHeader(ComputeAge(obj, now))
	switch result {
	case cacheHit:
		dst["X-Cache"] = headerHIT
	case cacheStale:
		dst["X-Cache"] = headerSTALE
		dst["Warning"] = []string{`110 - "Response is Stale"`}
	case cacheRevalidated:
		dst["X-Cache"] = headerREVALIDATED
	}
	stripNoCacheFields(dst, obj.CacheControl)
	w.WriteHeader(obj.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = w.Write(obj.Body)
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

func (h *Handler) fetchAndStore(w http.ResponseWriter, r *http.Request, key api.Key) {
	res := h.collapsedFetch(r, key)
	if res.Err != nil {
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	h.writeAndMaybeStore(w, r, key, res)
}

// fetchAndStoreStayinAlive is like fetchAndStore but falls back to
// serving the super-stale obj if the upstream is unavailable.
func (h *Handler) fetchAndStoreStayinAlive(w http.ResponseWriter, r *http.Request, key api.Key, stale *api.Object) {
	res := h.collapsedFetch(r, key)
	if res.Err != nil {
		h.logger.Warn("stayin-alive: upstream unreachable, serving stale indefinitely",
			"error", res.Err, "key", key)
		h.serveObject(w, r, stale, time.Now(), cacheStale)
		return
	}
	if res.StatusCode >= 500 {
		h.logger.Warn("stayin-alive: upstream 5xx, serving stale indefinitely",
			"status", res.StatusCode, "key", key)
		h.serveObject(w, r, stale, time.Now(), cacheStale)
		return
	}
	h.writeAndMaybeStore(w, r, key, res)
}

func (h *Handler) revalidate(w http.ResponseWriter, r *http.Request, key api.Key, stale *api.Object) {
	revalReq := r.Clone(r.Context())
	ConditionalHeaders(revalReq, stale)

	res := h.doFetch(revalReq)
	if res.Err != nil {
		h.serveObject(w, r, stale, time.Now(), cacheStale)
		return
	}

	// stale-if-error (RFC 5861 §4) and general stale-on-error: if origin
	// returns 5xx, serve stale unless must-revalidate/proxy-revalidate
	// explicitly forbids it (RFC 9111 §5.2.2.2).
	if res.StatusCode >= 500 {
		if h.stayinAlive {
			h.logger.Warn("stayin-alive: upstream 5xx, serving stale indefinitely",
				"status", res.StatusCode, "key", stale.Key)
			h.serveObject(w, r, stale, time.Now(), cacheStale)
			return
		}
		// Serve stale unless the stored response demands revalidation.
		cc := objDirectives(stale)
		if !cc.MustRevalidate && !cc.ProxyRevalidate {
			h.serveObject(w, r, stale, time.Now(), cacheStale)
			return
		}
	}

	if res.StatusCode == http.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res)
		_ = h.store.Put(r.Context(), key, refreshed)
		h.serveObject(w, r, refreshed, time.Now(), cacheRevalidated)
		return
	}

	h.writeAndMaybeStore(w, r, key, res)
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
	MergeHeaders304(&refreshed, res.Header)
	// Recompute CacheControl string and parsed TTL from the updated headers.
	refreshed.CacheControl = mergeHeaderValues(refreshed.Header, "Cache-Control")
	if ttl, ok := FreshnessLifetime(ParseCacheControl(refreshed.CacheControl), refreshed.Header.Get); ok {
		refreshed.TTL = ttl
	}
	// Re-apply route override so a 304 cannot revert bouine's storage lifetime
	// back to the upstream's (potentially shorter) max-age.
	if h.overrideTTL > 0 {
		refreshed.TTL = JitterTTL(h.overrideTTL, h.jitterPercent)
	}
	refreshed.OriginAge = parseOriginAge(refreshed.Header)
	if newETag := res.Header.Get("ETag"); newETag != "" {
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

	res := h.collapsedFetch(revalReq, key)
	if res.Err != nil {
		return
	}

	if res.StatusCode == http.StatusNotModified {
		refreshed := h.refreshFrom304(stale, res)
		_ = h.store.Put(ctx, key, refreshed)
		return
	}

	if IsCacheableWithDefault(res.StatusCode, r.Header, res.Header, h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && res.Header.Get("Set-Cookie") != "" {
			return
		}
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			return
		}
		obj := buildObject(key, r, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.excludeHeaders)
		obj.Header.Del("Set-Cookie")
		_ = h.store.Put(ctx, key, obj)
	}
}

func (h *Handler) writeAndMaybeStore(
	w http.ResponseWriter,
	r *http.Request,
	_ api.Key,
	res fetchResult,
) {
	dst := w.Header()
	for k, vals := range res.Header {
		dst[k] = vals
	}
	dst["X-Cache"] = headerMISS
	// A proxy SHOULD add an Age header to responses it forwards,
	// even on first fetch (Age: 0 + any origin Age).
	if res.Header.Get("Age") == "" {
		dst["Age"] = []string{"0"}
	}
	w.WriteHeader(res.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = w.Write(res.Body)
	}

	if IsCacheableWithDefault(res.StatusCode, r.Header, res.Header, h.negativeTTL, h.defaultTTL) {
		if !h.allowSetCookie && res.Header.Get("Set-Cookie") != "" {
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
		if vary := res.Header.Get("Vary"); vary != "" {
			storeKey = VariantKey(primaryKey, vary, r.Header, h.excludeHeaders)
		}
		// Enforce MaxVariants cap: skip storage if this primary key already
		// has MaxVariants distinct Vary variants. RFC 9110 §12.5.5 — unbounded
		// variants are a DoS vector.
		if storeKey != primaryKey {
			h.variantMu.Lock()
			n := h.variantCounts[primaryKey]
			if n >= MaxVariants {
				h.variantMu.Unlock()
				h.logger.Warn("vary cap exceeded, skipping variant storage",
					"primary_key", primaryKey, "cap", MaxVariants)
				if h.VaryCapHits != nil {
					h.VaryCapHits.Inc()
				}
				return
			}
			h.variantCounts[primaryKey] = n + 1
			h.variantMu.Unlock()
		}
		obj := buildObject(storeKey, r, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.excludeHeaders)
		obj.Header.Del("Set-Cookie")
		_ = h.store.Put(r.Context(), storeKey, obj)
		// Also store a "primary" entry so Vary-aware lookup finds
		// the Vary header on the first lookup.
		if storeKey != primaryKey {
			primaryObj := buildObject(primaryKey, r, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.excludeHeaders)
			primaryObj.Header.Del("Set-Cookie")
			_ = h.store.Put(r.Context(), primaryKey, primaryObj)
		}
		// Replication hook: in full cluster mode, broadcast the newly
		// cached object to all peers via gossip. No-op in strong and
		// eventual modes where replicateFn is nil.
		if h.replicateFn != nil {
			h.replicateFn(r.Context(), obj)
		}
	}
}

func (h *Handler) invalidateAndProxy(w http.ResponseWriter, r *http.Request) {
	// Capture the upstream response first — only invalidate on success
	// (RFC 9111 §4.4: invalidation MUST NOT happen if the response
	// indicates a server error).
	rec := acquireRecorder()
	defer releaseRecorder(rec)
	h.upstream.ServeHTTP(rec, r)

	// Only invalidate on 2xx/3xx success.
	if rec.statusCode >= 200 && rec.statusCode < 400 {
		getReq := r.Clone(r.Context())
		getReq.Method = http.MethodGet
		key := h.buildKey(getReq)
		_ = h.store.Delete(r.Context(), key)

		// Also evict Content-Location and Location URLs.
		for _, hdr := range []string{"Content-Location", "Location"} {
			if loc := rec.header.Get(hdr); loc != "" {
				locReq := r.Clone(r.Context())
				locReq.Method = http.MethodGet
				locReq.URL.Path = loc
				locKey := h.buildKey(locReq)
				_ = h.store.Delete(r.Context(), locKey)
			}
		}

		// RFC 9111 §4.3.1: if the POST response has explicit freshness
		// and a Content-Location matching the request URI, store the
		// response under the GET key so subsequent GETs can reuse it.
		if r.Method == http.MethodPost && rec.statusCode >= 200 && rec.statusCode < 300 &&
			IsCacheable(rec.statusCode, r.Header, rec.header) &&
			(h.allowSetCookie || rec.header.Get("Set-Cookie") == "") &&
			(h.maxObjectSize <= 0 || int64(rec.body.Len()) <= h.maxObjectSize) {
			// Copy body and header to avoid aliasing the pooled recorder buffer.
			bodyCopy := make([]byte, rec.body.Len())
			copy(bodyCopy, rec.body.Bytes())
			res := fetchResult{
				StatusCode: rec.statusCode,
				Header:     rec.header.Clone(),
				Body:       bodyCopy,
			}
			obj := buildObject(key, getReq, res, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.excludeHeaders)
			obj.Header.Del("Set-Cookie")
			_ = h.store.Put(r.Context(), key, obj)
		}
	}

	// Write the captured response to the client.
	dst := w.Header()
	for k, vals := range rec.header {
		dst[k] = vals
	}
	w.WriteHeader(rec.statusCode)
	_, _ = w.Write(rec.body.Bytes())
}

func (h *Handler) doFetch(r *http.Request) fetchResult {
	// L5 span: upstream origin pool layer.
	ctx, span := tracing.StartSpan(r.Context(), "bouine.origin")
	defer span.End()
	// Propagate W3C TraceContext into the upstream request so the origin
	// can participate in the distributed trace.
	outReq := r.WithContext(ctx)
	tracing.InjectHTTP(ctx, outReq)
	rec := acquireRecorder()
	defer releaseRecorder(rec)
	h.upstream.ServeHTTP(rec, outReq)

	// Copy body and header out so the fetchResult owns them before the
	// recorder returns to the pool. The body copy is also right-sized:
	// bytes.Buffer over-allocates, and stored objects are long-lived.
	body := make([]byte, rec.body.Len())
	copy(body, rec.body.Bytes())

	return fetchResult{
		StatusCode: rec.statusCode,
		Header:     rec.header.Clone(),
		Body:       body,
	}
}

func buildObject(key api.Key, r *http.Request, res fetchResult, negativeTTL, defaultTTL, overrideTTL, defaultSWR, defaultSIE time.Duration, jitterPct int, excludeHeaders map[string]bool) *api.Object {
	now := time.Now()
	// Parse Cache-Control (may be multiple headers — merge first).
	// CDN-Cache-Control overrides Cache-Control for shared caches (RFC 9211):
	// use it as the authoritative directive source when present.
	ccHeader := mergeHeaderValues(res.Header, "Cache-Control")
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
	if dateStr := res.Header.Get("Date"); dateStr != "" {
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
		Header:       res.Header.Clone(),
		Body:         res.Body,
		BodySize:     int64(len(res.Body)),
		StoredAt:     now,
		TTL:          ttl,
		ETag:         res.Header.Get("ETag"),
		CacheControl: ccHeader,  // Lead 1: pre-stored, avoids re-parsing on every hit
		OriginAge:    originAge, // Lead 3: pre-stored, avoids re-parsing on the read path
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
	if lm := res.Header.Get("Last-Modified"); lm != "" {
		if t, err := time.Parse(http.TimeFormat, lm); err == nil {
			obj.LastModified = t
		}
	}
	obj.VaryKey = BuildVaryKey(res.Header.Get("Vary"), r.Header, excludeHeaders)

	obj.SurrogateKeys = parseSurrogateKeys(res.Header)

	return obj
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
	for _, hdr := range []string{"Surrogate-Key", "Cache-Tag", "X-Cache-Tags"} {
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
}

func (r *responseRecorder) Header() http.Header { return r.header }

func (r *responseRecorder) WriteHeader(code int) { r.statusCode = code }

func (r *responseRecorder) Write(b []byte) (int, error) {
	if r.statusCode == 0 {
		r.statusCode = 200
	}
	return r.body.Write(b)
}

// Flush implements http.Flusher for streaming compatibility.
func (r *responseRecorder) Flush() {}

// ensure interface compliance.
var _ http.ResponseWriter = (*responseRecorder)(nil)
var _ http.Flusher = (*responseRecorder)(nil)
