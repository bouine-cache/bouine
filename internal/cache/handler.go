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
	"log/slog"
	"maps"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/thylong/bouine/internal/pipeline/collapse"
	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/api"
)

// Pre-allocated header values to avoid per-request slice literals.
var (
	headerHIT         = []string{"HIT"}
	headerMISS        = []string{"MISS"}
	headerSTALE       = []string{"STALE"}
	headerBYPASS      = []string{"BYPASS"}
	headerREVALIDATED = []string{"REVALIDATED"}
)

// recorderPool reuses responseRecorder instances on the miss/invalidation
// paths to reduce allocations. The hit path never allocates a recorder.
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
	upstream      http.Handler
	store         storage.Store
	collapse      *collapse.Group
	logger        *slog.Logger
	negativeTTL   time.Duration
	jitterPercent int
	stayinAlive   bool
	// variantCounts tracks stored Vary variants per primary key to
	// enforce MaxVariants cap. Protected by variantMu.
	variantMu     sync.Mutex
	variantCounts map[api.Key]int
	// VaryCapHits is incremented when a variant is rejected; nil-safe.
	VaryCapHits interface{ Inc() }
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
	// VaryCapHits, if non-nil, is incremented when a variant is rejected
	// because MaxVariants is exceeded.
	VaryCapHits interface{ Inc() }
}

// NewHandler creates a caching handler.
func NewHandler(cfg HandlerConfig) *Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Handler{
		upstream:      cfg.Upstream,
		store:         cfg.Store,
		collapse:      collapse.NewGroup(),
		logger:        cfg.Logger,
		negativeTTL:   cfg.NegativeTTL,
		jitterPercent: cfg.JitterPercent,
		stayinAlive:   cfg.StayinAlive,
		variantCounts: make(map[api.Key]int),
		VaryCapHits:   cfg.VaryCapHits,
	}
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isInvalidating(r.Method) {
		h.invalidateAndProxy(w, r)
		return
	}

	key, obj := h.lookup(r)
	disp := Evaluate(r, obj, time.Now())

	switch disp.Decision {
	case Hit, StaleHit:
		if h.tryConditional304(w, r, disp.Object) {
			return
		}
		if ServeRange(w, r, disp.Object) {
			return
		}
		h.serveFromCache(w, r, disp.Object)
	case Miss:
		// StayinAlive: if we have a super-stale object and origin fails,
		// serve it rather than returning 502.
		if h.stayinAlive && obj != nil {
			h.fetchAndStoreStayinAlive(w, r, key, obj)
		} else {
			h.fetchAndStore(w, r, key)
		}
	case Revalidate:
		h.revalidate(w, r, key, disp.Object)
	case Bypass:
		h.handleBypass(w, r)
	}
}

// lookup resolves the cache key and stored object for r, accounting
// for Vary-based secondary keys.
func (h *Handler) lookup(r *http.Request) (api.Key, *api.Object) {
	key := BuildKey(r)
	obj, err := h.store.Get(r.Context(), key)
	if err != nil {
		h.logger.Warn("cache lookup error", "error", err)
	}
	if obj == nil || obj.Header.Get("Vary") == "" {
		return key, obj
	}
	vk := VariantKey(key, obj.Header.Get("Vary"), r.Header)
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

func (h *Handler) serveFromCache(w http.ResponseWriter, r *http.Request, obj *api.Object) {
	dst := w.Header()
	maps.Copy(dst, obj.Header)
	age := ComputeAge(obj, time.Now())
	dst["Age"] = []string{strconv.Itoa(int(age.Seconds()))}
	dst["X-Cache"] = headerHIT
	w.WriteHeader(obj.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = w.Write(obj.Body)
	}
}

// serveStale writes a cached object with X-Cache: STALE (SWR / SIE path).
func (h *Handler) serveStale(w http.ResponseWriter, r *http.Request, obj *api.Object) {
	dst := w.Header()
	maps.Copy(dst, obj.Header)
	age := ComputeAge(obj, time.Now())
	dst["Age"] = []string{strconv.Itoa(int(age.Seconds()))}
	dst["X-Cache"] = headerSTALE
	w.WriteHeader(obj.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = w.Write(obj.Body)
	}
}

// serveRevalidated writes a refreshed cached object with X-Cache: REVALIDATED
// (origin confirmed freshness via 304).
func (h *Handler) serveRevalidated(w http.ResponseWriter, r *http.Request, obj *api.Object) {
	dst := w.Header()
	maps.Copy(dst, obj.Header)
	age := ComputeAge(obj, time.Now())
	dst["Age"] = []string{strconv.Itoa(int(age.Seconds()))}
	dst["X-Cache"] = headerREVALIDATED
	w.WriteHeader(obj.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = w.Write(obj.Body)
	}
}

func (h *Handler) fetchAndStore(w http.ResponseWriter, r *http.Request, key api.Key) {
	res, _ := h.collapse.Do(key, func() collapse.Result {
		return h.doFetch(r)
	})
	if res.Err != nil {
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	h.writeAndMaybeStore(w, r, key, res)
}

// fetchAndStoreStayinAlive is like fetchAndStore but falls back to
// serving the super-stale obj if the upstream is unavailable.
func (h *Handler) fetchAndStoreStayinAlive(w http.ResponseWriter, r *http.Request, key api.Key, stale *api.Object) {
	res, _ := h.collapse.Do(key, func() collapse.Result {
		return h.doFetch(r)
	})
	if res.Err != nil {
		h.logger.Warn("stayin-alive: upstream unreachable, serving stale indefinitely",
			"error", res.Err, "key", key)
		h.serveStale(w, r, stale)
		return
	}
	if res.StatusCode >= 500 {
		h.logger.Warn("stayin-alive: upstream 5xx, serving stale indefinitely",
			"status", res.StatusCode, "key", key)
		h.serveStale(w, r, stale)
		return
	}
	h.writeAndMaybeStore(w, r, key, res)
}

func (h *Handler) revalidate(w http.ResponseWriter, r *http.Request, key api.Key, stale *api.Object) {
	revalReq := r.Clone(r.Context())
	ConditionalHeaders(revalReq, stale)

	res := h.doFetch(revalReq)
	if res.Err != nil {
		// Origin error during revalidation — serve stale if available.
		h.serveFromCache(w, r, stale)
		return
	}

	// stale-if-error (RFC 5861 §4): if origin returns 5xx and the
	// stale object has SIE configured, serve stale instead.
	if res.StatusCode >= 500 {
		if h.stayinAlive || (stale.StaleIfError > 0 && stale.StaleButServable(time.Now())) {
			if h.stayinAlive {
				h.logger.Warn("stayin-alive: upstream 5xx, serving stale indefinitely",
					"status", res.StatusCode, "key", stale.Key)
			}
			h.serveStale(w, r, stale)
			return
		}
	}

	if res.StatusCode == http.StatusNotModified {
		// Merge 304 headers into stored object (RFC 9111 §3.2).
		// Clone the header map before mutating: stale.Header is shared with
		// any other goroutine that looked up the same object, and
		// maps.Copy/range iteration on it would race with our writes.
		refreshed := *stale
		refreshed.Header = stale.Header.Clone()
		refreshed.StoredAt = time.Now()
		MergeHeaders304(&refreshed, res.Header)
		// Recompute TTL from the (possibly updated) headers.
		newCC := ParseCacheControl(refreshed.Header.Get("Cache-Control"))
		if ttl, ok := FreshnessLifetime(newCC, refreshed.Header.Get); ok {
			refreshed.TTL = ttl
		}
		// Update ETag if the 304 provides a new one.
		if newETag := res.Header.Get("ETag"); newETag != "" {
			refreshed.ETag = newETag
		}
		_ = h.store.Put(r.Context(), key, &refreshed)
		h.serveRevalidated(w, r, &refreshed)
		return
	}

	h.writeAndMaybeStore(w, r, key, res)
}

func (h *Handler) writeAndMaybeStore(
	w http.ResponseWriter,
	r *http.Request,
	key api.Key,
	res collapse.Result,
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

	if IsCacheable(res.StatusCode, r.Header, res.Header, h.negativeTTL) {
		// Always compute from the canonical URL so the cap is enforced
		// against the primary key regardless of whether lookup() already
		// resolved the variant key.
		primaryKey := BuildKey(r)
		storeKey := primaryKey
		if vary := res.Header.Get("Vary"); vary != "" {
			storeKey = VariantKey(primaryKey, vary, r.Header)
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
		obj := buildObject(storeKey, r, res, h.negativeTTL, h.jitterPercent)
		_ = h.store.Put(r.Context(), storeKey, obj)
		// Also store a "primary" entry so Vary-aware lookup finds
		// the Vary header on the first lookup.
		if storeKey != primaryKey {
			primaryObj := buildObject(primaryKey, r, res, h.negativeTTL, h.jitterPercent)
			_ = h.store.Put(r.Context(), primaryKey, primaryObj)
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
		key := BuildKey(getReq)
		_ = h.store.Delete(r.Context(), key)

		// Also evict Content-Location and Location URLs.
		for _, hdr := range []string{"Content-Location", "Location"} {
			if loc := rec.header.Get(hdr); loc != "" {
				locReq := r.Clone(r.Context())
				locReq.Method = http.MethodGet
				locReq.URL.Path = loc
				locKey := BuildKey(locReq)
				_ = h.store.Delete(r.Context(), locKey)
			}
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

func (h *Handler) doFetch(r *http.Request) collapse.Result {
	rec := acquireRecorder()
	h.upstream.ServeHTTP(rec, r)

	return collapse.Result{
		StatusCode: rec.statusCode,
		Header:     rec.header,
		Body:       rec.body.Bytes(),
	}
}

func buildObject(key api.Key, r *http.Request, res collapse.Result, negativeTTL time.Duration, jitterPct int) *api.Object {
	now := time.Now()
	// Parse all Cache-Control headers (may be multiple).
	ccHeader := mergeHeaderValues(res.Header, "Cache-Control")
	respCC := ParseCacheControl(ccHeader)
	ttl, explicit := FreshnessLifetimeH(respCC, res.Header)

	// Heuristic freshness: if no explicit TTL, use 10% of age since
	// Last-Modified (RFC 9111 §4.2.2).
	if !explicit {
		ttl = HeuristicTTL(res.Header, now)
	}

	// Negative caching: assign configured TTL for error statuses.
	if !explicit && ttl == 0 && negativeTTL > 0 && IsNegativeCacheable(res.StatusCode) {
		ttl = negativeTTL
	}

	// Jitter: randomize TTL to prevent synchronized expiry stampedes.
	ttl = JitterTTL(ttl, jitterPct)

	// Subtract the origin's Age header from TTL so that objects that
	// arrive already partially aged are correctly marked as stale
	// (RFC 9111 §4.2.3). For example, if max-age=60 and Age=50, the
	// remaining freshness is 10s, not 60s.
	originAge := parseOriginAge(res.Header)
	ttl -= originAge
	if ttl < 0 {
		ttl = 0
	}

	obj := &api.Object{
		Key:        key,
		StatusCode: res.StatusCode,
		Header:     res.Header.Clone(),
		Body:       res.Body,
		BodySize:   int64(len(res.Body)),
		StoredAt:   now,
		TTL:        ttl,
		ETag:       res.Header.Get("ETag"),
	}
	if respCC.StaleWhileRevalidSet {
		obj.StaleWhileRevalidate = respCC.StaleWhileRevalid
	}
	if respCC.StaleIfErrorSet {
		obj.StaleIfError = respCC.StaleIfError
	}
	if lm := res.Header.Get("Last-Modified"); lm != "" {
		if t, err := time.Parse(http.TimeFormat, lm); err == nil {
			obj.LastModified = t
		}
	}
	obj.VaryKey = BuildVaryKey(res.Header.Get("Vary"), r.Header)
	return obj
}

// isInvalidating returns true for any unsafe method that should
// trigger cache invalidation (RFC 9111 §4.4). Only GET, HEAD, and
// OPTIONS are safe; everything else invalidates.
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
