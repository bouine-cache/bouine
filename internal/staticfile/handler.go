package staticfile

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bouine-cache/xxhash/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/header"
)

// httpTimeFormat is the HTTP date format per RFC 9110 §6.6.1.
const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

// defaultMaxFileSize is the per-file size cap when MaxFileSize is zero.
const defaultMaxFileSize int64 = 10 << 20 // 10 MiB

// bufPool provides 64 KiB buffers for streaming file bodies, avoiding
// per-request allocation. Buffers are reset on return.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64<<10)
		return &b
	},
}

// Metrics holds the Prometheus counters for a static file Handler.
type Metrics struct {
	RequestsTotal *prometheus.CounterVec
	BytesTotal    *prometheus.CounterVec
}

// NewMetrics registers and returns static-file metrics on the given
// registry. Labels: route, result. Cardinality is bounded by the number
// of routes × 5 result values.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "staticfile_requests_total",
			Help:      "Total requests to static file routes.",
		}, []string{"route", "result"}),
		BytesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "staticfile_bytes_total",
			Help:      "Total bytes served from static file routes.",
		}, []string{"route"}),
	}
	reg.MustRegister(m.RequestsTotal, m.BytesTotal)
	return m
}

// result values for the staticfile_requests_total metric.
const (
	resultServed           = "served"
	resultNotFound         = "not_found"
	resultTooLarge         = "too_large"
	resultTraversalBlocked = "traversal_blocked"
	resultMethodNotAllowed = "method_not_allowed"
)

// etagEntry caches a computed ETag keyed by file path + mtime.
type etagEntry struct {
	mtime time.Time
	etag  string
}

// Handler serves static files from a filesystem rooted at a fixed
// directory. It does NOT support directory listing. Path traversal is
// prevented by path.Clean + filepath.Rel containment check. Symlinks
// in the root are resolved once at startup; per-request symlink
// evaluation is not performed.
//
// Only GET and HEAD methods are accepted; all others return 405.
type Handler struct {
	logger     observability.Logger
	metrics    *Metrics
	etagCache  sync.Map // map[string]etagEntry
	root       string
	routeLabel string
	indexFiles []string
	maxBytes   int64
}

// Config configures a static file Handler.
type Config struct {
	// Logger is the structured logger. Nil defaults to NoopLogger.
	Logger observability.Logger
	// Metrics holds the Prometheus counters. May be nil — the handler
	// checks before incrementing.
	Metrics *Metrics
	// Root is the absolute path to the directory from which files are
	// served. Symlinks are resolved once at construction time.
	Root string
	// RouteLabel is the route name used in metric labels.
	RouteLabel string
	// IndexFiles are tried (in order) when the request path maps to a
	// directory. If none match, bouine returns 404.
	IndexFiles []string
	// MaxBytes is the per-file size cap. Files larger than this are
	// rejected with 413. Zero applies a 10 MiB default.
	MaxBytes int64
}

// New opens the root directory, resolves symlinks, and returns a Handler.
// Returns an error if root does not exist, is not a directory, or contains
// a symlink that escapes its parent.
func New(cfg Config) (*Handler, error) {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)

	resolved, err := filepath.EvalSymlinks(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("staticfile: resolve root %q: %w", cfg.Root, err)
	}

	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("staticfile: stat root %q: %w", resolved, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("staticfile: root %q is not a directory", resolved)
	}

	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxFileSize
	}

	return &Handler{
		root:       resolved,
		indexFiles: cfg.IndexFiles,
		maxBytes:   maxBytes,
		logger:     cfg.Logger,
		metrics:    cfg.Metrics,
		routeLabel: cfg.RouteLabel,
	}, nil
}

// ServeRequest is the fasthttp-native handler for serving static files.
func (h *Handler) ServeRequest(ctx *fasthttp.RequestCtx) {
	method := string(ctx.Method())
	if method != "GET" && method != "HEAD" {
		h.recordResult(resultMethodNotAllowed)
		ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
		return
	}

	cleaned := path.Clean("/" + string(ctx.Path()))
	if !h.isPathContained(cleaned) {
		h.recordResult(resultTraversalBlocked)
		ctx.Error("forbidden", fasthttp.StatusForbidden)
		return
	}

	f, stat, servedPath, ok := h.resolveFile(cleaned)
	if !ok {
		h.recordResult(resultNotFound)
		ctx.Error("not found", fasthttp.StatusNotFound)
		return
	}
	defer func() { _ = f.Close() }()

	if stat.Size() > h.maxBytes {
		h.recordResult(resultTooLarge)
		ctx.Error("file too large", fasthttp.StatusRequestEntityTooLarge)
		return
	}

	h.setFastHeaders(ctx, f, servedPath, stat)

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		h.logger.Warn("staticfile: seek to start failed", "path", servedPath, "error", err)
		ctx.Error("internal error", fasthttp.StatusInternalServerError)
		return
	}

	if h.handleFastConditional(ctx, stat) {
		return
	}

	if rng := string(ctx.Request.Header.Peek(header.Range)); rng != "" {
		if h.handleFastRange(ctx, f, rng, stat.Size(), method) {
			return
		}
	}

	if method == "HEAD" {
		ctx.SetStatusCode(fasthttp.StatusOK)
		h.recordResult(resultServed)
		return
	}

	h.streamFastFile(ctx, f, servedPath)
}

// isPathContained checks whether the cleaned path, when joined with
// root, stays within the root directory.
func (h *Handler) isPathContained(cleaned string) bool {
	full := filepath.Join(h.root, filepath.FromSlash(cleaned))
	rel, err := filepath.Rel(h.root, full)
	return err == nil && !strings.HasPrefix(rel, "..")
}

// resolveFile opens the file at cleanedPath. If it's a directory, it
// tries index files. Returns the opened file, its stat info, the
// effective served path (may differ from cleanedPath if an index file
// was used), and whether the file was found.
func (h *Handler) resolveFile(cleanedPath string) (*os.File, os.FileInfo, string, bool) {
	f, err := os.Open(filepath.Join(h.root, filepath.FromSlash(cleanedPath))) //nolint:gosec // path sanitized by isPathContained
	if err != nil {
		h.logger.Warn("staticfile: open error", "path", cleanedPath, "error", err)
		return nil, nil, "", false
	}

	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		h.logger.Warn("staticfile: stat error", "path", cleanedPath, "error", err)
		return nil, nil, "", false
	}

	if !stat.IsDir() {
		return f, stat, cleanedPath, true
	}

	_ = f.Close()
	return h.resolveIndex(cleanedPath)
}

// resolveIndex tries each index file in the directory at cleanedPath.
func (h *Handler) resolveIndex(cleanedPath string) (*os.File, os.FileInfo, string, bool) {
	for _, idx := range h.indexFiles {
		idxPath := path.Join(cleanedPath, idx)
		if !h.isPathContained(idxPath) {
			continue
		}
		fullPath := filepath.Join(h.root, filepath.FromSlash(idxPath))
		idxFile, err := os.Open(fullPath) //nolint:gosec // path sanitized by isPathContained
		if err != nil {
			continue
		}
		idxStat, err := idxFile.Stat()
		_ = idxFile.Close()
		if err != nil || idxStat.IsDir() {
			continue
		}
		f, err := os.Open(fullPath) //nolint:gosec // path sanitized by isPathContained
		if err != nil {
			continue
		}
		stat, err := f.Stat()
		if err != nil {
			_ = f.Close()
			continue
		}
		return f, stat, idxPath, true
	}
	return nil, nil, "", false
}

// setHeaders sets Content-Length, Content-Type, Last-Modified, and ETag
// headers on the response. The already-opened file f is used for ETag
// computation to avoid a redundant open syscall.

// setFastHeaders sets Content-Length, Content-Type, Last-Modified, and ETag
// headers on the fasthttp response.
func (h *Handler) setFastHeaders(ctx *fasthttp.RequestCtx, f *os.File, cleanedPath string, stat os.FileInfo) {
	ctx.Response.Header.Set(header.ContentLength, strconv.FormatInt(stat.Size(), 10))

	ext := filepath.Ext(cleanedPath)
	ctx.Response.Header.Set(header.ContentType, contentTypeByExtension(ext))

	lastMod := stat.ModTime().UTC()
	ctx.Response.Header.Set(header.LastModified, lastMod.Format(httpTimeFormat))

	if etag := h.computeETag(f, cleanedPath, stat); etag != "" {
		ctx.Response.Header.Set(header.ETag, etag)
	}
}

// handleConditional checks If-None-Match and If-Modified-Since headers
// and writes a 304 response if the conditions are met. Returns true if
// the response was written.

// handleFastConditional checks If-None-Match and If-Modified-Since headers
// for the fasthttp handler and writes a 304 response if conditions are met.
func (h *Handler) handleFastConditional(ctx *fasthttp.RequestCtx, stat os.FileInfo) bool {
	etag := string(ctx.Response.Header.Peek(header.ETag))
	if match := string(ctx.Request.Header.Peek(header.IfNoneMatch)); match != "" {
		if isETagMatch(match, etag) {
			ctx.SetStatusCode(fasthttp.StatusNotModified)
			h.recordResult(resultServed)
			return true
		}
		return false
	}
	if ims := string(ctx.Request.Header.Peek(header.IfModifiedSince)); ims != "" {
		lastMod := stat.ModTime().UTC()
		if isModifiedSinceMatch(ims, lastMod) {
			ctx.SetStatusCode(fasthttp.StatusNotModified)
			h.recordResult(resultServed)
			return true
		}
	}
	return false
}

// streamFile copies the file body to the response writer using a pooled
// buffer. Records the bytes served metric on success.

// streamFastFile copies the file body to the fasthttp response using a
// pooled buffer.
func (h *Handler) streamFastFile(ctx *fasthttp.RequestCtx, f *os.File, cleanedPath string) {
	bufPtr := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufPtr)
	buf := *bufPtr

	written, err := io.CopyBuffer(ctx.Response.BodyWriter(), f, buf)
	if err != nil {
		h.logger.Warn("staticfile: stream error", "path", cleanedPath, "error", err)
		return
	}

	h.recordServed(written)
}

// computeETag returns a strong ETag for the file. It caches the result
// keyed by path + mtime so unchanged files are only hashed once. The
// ETag is the xxhash64 of the file content, formatted as a quoted hex
// string.
//
// The already-opened file f is used for hashing to avoid a redundant
// open syscall. The file offset is advanced to EOF after hashing;
// ServeRequest resets it to 0 before any body streaming.
//
// Returns an empty string if the content hash cannot be computed
// (seek/read error). Per ADR-0017 §7, a missing ETag is strictly safer
// than a wrong mtime-based one: clients fall back to If-Modified-Since
// validation, which is correct.
func (h *Handler) computeETag(f *os.File, cleanedPath string, stat os.FileInfo) string {
	mtime := stat.ModTime()
	if cached, ok := h.etagCache.Load(cleanedPath); ok {
		if e, ok := cached.(etagEntry); ok && e.mtime.Equal(mtime) {
			return e.etag
		}
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		h.logger.Warn("staticfile: etag seek error", "path", cleanedPath, "error", err)
		return ""
	}

	hasher := xxhash.New()
	if _, err := io.Copy(hasher, f); err != nil {
		h.logger.Warn("staticfile: etag hash error", "path", cleanedPath, "error", err)
		return ""
	}

	etag := fmt.Sprintf(`"%x"`, hasher.Sum64())
	h.etagCache.Store(cleanedPath, etagEntry{mtime: mtime, etag: etag})
	return etag
}

// isETagMatch checks if the If-None-Match header value matches the ETag.
// Handles comma-separated lists and the wildcard "*".
func isETagMatch(ifNoneMatch, etag string) bool {
	if ifNoneMatch == "*" {
		return true
	}
	parts := strings.Split(ifNoneMatch, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.TrimPrefix(p, "W/")
		if p == etag {
			return true
		}
	}
	return false
}

// isModifiedSinceMatch returns true if the file has NOT been modified
// since the If-Modified-Since time (i.e., a 304 should be sent).
func isModifiedSinceMatch(ims string, lastMod time.Time) bool {
	imsTime, err := time.Parse(httpTimeFormat, ims)
	if err != nil {
		return false
	}
	return !lastMod.Truncate(time.Second).After(imsTime)
}

// recordResult increments the requests_total metric for the given result.
func (h *Handler) recordResult(result string) {
	if h.metrics != nil {
		h.metrics.RequestsTotal.WithLabelValues(h.routeLabel, result).Inc()
	}
}

// recordServed increments the requests_total and bytes_total metrics.
func (h *Handler) recordServed(bytes int64) {
	if h.metrics != nil {
		h.metrics.RequestsTotal.WithLabelValues(h.routeLabel, resultServed).Inc()
		h.metrics.BytesTotal.WithLabelValues(h.routeLabel).Add(float64(bytes))
	}
}

// rangeStatus indicates the result of parsing a range spec.
type rangeStatus int

const (
	rangeValid         rangeStatus = iota // start/end are valid
	rangeInvalid                          // malformed — fall through to 200
	rangeUnsatisfiable                    // start >= size — write 416
)

// handleRange processes a Range header. Returns true if the response was
// written (either 206 or 416). Returns false if the range is invalid and
// the caller should fall through to a full 200 response.
//
// Per RFC 9110 §14.3.2, a server MAY collapse a multipart range request
// into a single 206 response. We serve the first range only.
//
// The already-opened file f is reused from resolveFile — no second
// open syscall. The file offset must be at 0 when handleFastRange is
// called (guaranteed by ServeRequest's Seek(0) after setHeaders, or by
// resolveFile opening a fresh handle on cache hit). When handleFastRange
// returns false (rangeInvalid), it must NOT have called Seek on f, so
// the caller can safely fall through to streamFile which reads from
// offset 0.

// handleFastRange processes a Range header for the fasthttp handler.
func (h *Handler) handleFastRange(ctx *fasthttp.RequestCtx, f *os.File, rangeHeader string, size int64, method string) bool {
	start, end, status := parseRangeSpec(rangeHeader, size)
	switch status {
	case rangeInvalid:
		return false
	case rangeUnsatisfiable:
		ctx.Response.Header.Set(header.ContentRange, fmt.Sprintf("bytes */%d", size))
		ctx.Error("range not satisfiable", fasthttp.StatusRequestedRangeNotSatisfiable)
		h.recordResult(resultServed)
		return true
	}

	length := end - start + 1
	ctx.Response.Header.Set(header.ContentRange, fmt.Sprintf("bytes %d-%d/%d", start, end, size))
	ctx.Response.Header.Set(header.ContentLength, strconv.FormatInt(length, 10))

	if start > 0 {
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			h.logger.Warn("staticfile: range seek error", "error", err)
			ctx.Response.Header.Del(header.ContentRange)
			ctx.Response.Header.Del(header.ContentLength)
			ctx.Error("range seek error", fasthttp.StatusInternalServerError)
			return true
		}
	}

	ctx.SetStatusCode(fasthttp.StatusPartialContent)
	if method == "HEAD" {
		h.recordResult(resultServed)
		return true
	}

	bufPtr := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufPtr)
	buf := *bufPtr

	written, err := io.CopyBuffer(ctx.Response.BodyWriter(), io.LimitReader(f, length), buf)
	if err != nil {
		h.logger.Warn("staticfile: range stream error", "error", err)
		return true
	}
	h.recordServed(written)
	return true
}

// parseRangeSpec parses a Range header value and returns the start and
// end byte offsets plus a status indicating whether the range is valid,
// invalid (malformed — caller should fall through to 200), or
// unsatisfiable (start >= size — caller should write 416).
//
//nolint:funlen // range parsing is inherently branchy
func parseRangeSpec(rangeHeader string, size int64) (start, end int64, status rangeStatus) {
	const prefix = "bytes="
	if !strings.HasPrefix(rangeHeader, prefix) {
		return 0, 0, rangeInvalid
	}
	spec := strings.TrimPrefix(rangeHeader, prefix)
	if idx := strings.Index(spec, ","); idx >= 0 {
		spec = strings.TrimSpace(spec[:idx])
	}

	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, rangeInvalid
	}

	if parts[0] == "" {
		n, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, rangeInvalid
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, rangeValid
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, rangeUnsatisfiable
	}

	if parts[1] == "" {
		return start, size - 1, rangeValid
	}

	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || end < start {
		return 0, 0, rangeInvalid
	}
	if end >= size {
		end = size - 1
	}
	return start, end, rangeValid
}
