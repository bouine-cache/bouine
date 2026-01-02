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
	"time"

	"github.com/thylong/bouine/internal/pipeline/collapse"
	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/api"
)

// Pre-allocated header values to avoid per-request slice literals.
var (
	headerHIT  = []string{"HIT"}
	headerMISS = []string{"MISS"}
)

// Handler is the caching HTTP handler. It wraps an upstream
// http.Handler (the origin pool) and a storage.Store.
type Handler struct {
	upstream http.Handler
	store    storage.Store
	collapse *collapse.Group
	logger   *slog.Logger
}

// HandlerConfig configures a cache Handler.
type HandlerConfig struct {
	Upstream http.Handler
	Store    storage.Store
	Logger   *slog.Logger
}

// NewHandler creates a caching handler.
func NewHandler(cfg HandlerConfig) *Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Handler{
		upstream: cfg.Upstream,
		store:    cfg.Store,
		collapse: collapse.NewGroup(),
		logger:   cfg.Logger,
	}
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isInvalidating(r.Method) {
		h.invalidateAndProxy(w, r)
		return
	}

	key := BuildKey(r)

	// Try Vary-aware lookup: if we have a stored object for this primary
	// key, use its Vary header to compute the variant key.
	obj, err := h.store.Get(r.Context(), key)
	if err != nil {
		h.logger.Warn("cache lookup error", "error", err)
	}
	if obj != nil && obj.Header.Get("Vary") != "" {
		vk := VariantKey(key, obj.Header.Get("Vary"), r.Header)
		if vk != key {
			vobj, verr := h.store.Get(r.Context(), vk)
			if verr == nil && vobj != nil {
				obj = vobj
			} else {
				obj = nil
			}
			key = vk
		}
	}

	disp := Evaluate(r, obj, time.Now())

	switch disp.Decision {
	case Hit, StaleHit:
		// Client-side conditional: return 304 if client's validators match.
		if ClientConditionalMatch(r, disp.Object) {
			// 304 MUST include ETag if the full response would have (RFC 9110 §15.4.5).
			if disp.Object.ETag != "" {
				w.Header().Set("ETag", disp.Object.ETag)
			}
			w.Header().Set("X-Cache", "HIT")
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if ServeRange(w, r, disp.Object) {
			return
		}
		h.serveFromCache(w, r, disp.Object)
	case Miss:
		h.fetchAndStore(w, r, key)
	case Revalidate:
		h.revalidate(w, r, key, disp.Object)
	case Bypass:
		h.upstream.ServeHTTP(w, r)
	}
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

func (h *Handler) revalidate(w http.ResponseWriter, r *http.Request, key api.Key, stale *api.Object) {
	revalReq := r.Clone(r.Context())
	ConditionalHeaders(revalReq, stale)

	res := h.doFetch(revalReq)
	if res.Err != nil {
		// Origin error during revalidation — serve stale if available.
		h.serveFromCache(w, r, stale)
		return
	}

	if res.StatusCode == http.StatusNotModified {
		// Merge 304 headers into stored object (RFC 9111 §3.2).
		refreshed := *stale
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
		h.serveFromCache(w, r, &refreshed)
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
	w.WriteHeader(res.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = w.Write(res.Body)
	}

	if IsCacheable(res.StatusCode, r.Header, res.Header) {
		storeKey := key
		if vary := res.Header.Get("Vary"); vary != "" {
			storeKey = VariantKey(BuildKey(r), vary, r.Header)
		}
		obj := buildObject(storeKey, r, res)
		_ = h.store.Put(r.Context(), storeKey, obj)
		// Also store a "primary" entry so Vary-aware lookup finds
		// the Vary header on the first lookup.
		if storeKey != key {
			primaryObj := buildObject(key, r, res)
			_ = h.store.Put(r.Context(), key, primaryObj)
		}
	}
}

func (h *Handler) invalidateAndProxy(w http.ResponseWriter, r *http.Request) {
	// Invalidation targets the GET key for this URL (RFC 9111 §4.4).
	getReq := r.Clone(r.Context())
	getReq.Method = http.MethodGet
	key := BuildKey(getReq)
	_ = h.store.Delete(r.Context(), key)

	// Capture the upstream response to check for Content-Location and
	// Location headers for related-URL invalidation (RFC 9111 §4.4).
	rec := &responseRecorder{
		header: make(http.Header),
		body:   &bytes.Buffer{},
	}
	h.upstream.ServeHTTP(rec, r)

	// Only invalidate related URLs on success (2xx).
	if rec.statusCode >= 200 && rec.statusCode < 400 {
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
	rec := &responseRecorder{
		header: make(http.Header),
		body:   &bytes.Buffer{},
	}
	h.upstream.ServeHTTP(rec, r)

	return collapse.Result{
		StatusCode: rec.statusCode,
		Header:     rec.header,
		Body:       rec.body.Bytes(),
	}
}

func buildObject(key api.Key, r *http.Request, res collapse.Result) *api.Object {
	now := time.Now()
	// Parse all Cache-Control headers (may be multiple).
	ccHeader := mergeHeaderValues(res.Header, "Cache-Control")
	respCC := ParseCacheControl(ccHeader)
	ttl, explicit := FreshnessLifetime(respCC, res.Header.Get)

	// Heuristic freshness: if no explicit TTL, use 10% of age since
	// Last-Modified (RFC 9111 §4.2.2).
	if !explicit {
		ttl = HeuristicTTL(res.Header, now)
	}

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

func isInvalidating(method string) bool {
	return method == http.MethodPost ||
		method == http.MethodPut ||
		method == http.MethodDelete ||
		method == http.MethodPatch
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
