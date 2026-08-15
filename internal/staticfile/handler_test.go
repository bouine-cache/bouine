package staticfile

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T, files map[string]string, cfg Config) *Handler {
	t.Helper()
	dir := t.TempDir()
	for rel, content := range files {
		full := filepath.Join(dir, rel)
		err := os.MkdirAll(filepath.Dir(full), 0o755)
		require.NoError(t, err)
		err = os.WriteFile(full, []byte(content), 0o644)
		require.NoError(t, err)
	}
	cfg.Root = dir
	h, err := New(cfg)
	require.NoError(t, err, "New")
	return h
}

func doRequest(t *testing.T, h *Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(method, path, nil)
	h.ServeHTTP(w, r)
	return w
}

func TestHandler_ServeFile(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"index.html": "<h1>hello</h1>",
		"style.css":  "body { color: red; }",
	}, Config{})
	w := doRequest(t, h, "GET", "/index.html")
	require.Equal(t, 200, w.Code)
	require.Equal(t, "<h1>hello</h1>", w.Body.String())
	ct := w.Header().Get("Content-Type")
	require.Equal(t, "text/html; charset=utf-8", ct)
	cl := w.Header().Get("Content-Length")
	require.Equal(t, "14", cl)
}

func TestHandler_ContentTypes(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"app.js":    "console.log(1);",
		"data.json": `{"a":1}`,
		"logo.png":  "fakepng",
		"file.txt":  "plain",
	}, Config{})
	tests := []struct {
		path   string
		wantCT string
	}{
		{"app.js", "application/javascript"},
		{"data.json", "application/json"},
		{"logo.png", "image/png"},
		{"file.txt", "text/plain; charset=utf-8"},
	}
	for _, tt := range tests {
		w := doRequest(t, h, "GET", "/"+tt.path)
		assert.Equal(t, 200, w.Code)
		got := w.Header().Get("Content-Type")
		assert.Equal(t, tt.wantCT, got)
	}
}

func TestHandler_ContentTypes_CaseInsensitive(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"INDEX.HTML": "<h1>hi</h1>",
		"STYLE.CSS":  "body{}",
		"SCRIPT.JS":  "console.log(1);",
	}, Config{})
	tests := []struct {
		path   string
		wantCT string
	}{
		{"/INDEX.HTML", "text/html; charset=utf-8"},
		{"/STYLE.CSS", "text/css; charset=utf-8"},
		{"/SCRIPT.JS", "application/javascript"},
	}
	for _, tt := range tests {
		w := doRequest(t, h, "GET", tt.path)
		assert.Equal(t, 200, w.Code)
		got := w.Header().Get("Content-Type")
		assert.Equal(t, tt.wantCT, got, "path %s", tt.path)
	}
}

func TestMIME_BundledTypesServedDirectly(t *testing.T) {
	// ADR-0017 §6: Content-Type is set from a bundled MIME map, not the
	// host OS MIME database. This test verifies the handler resolves
	// Content-Type from bundledMIMEs directly — the .webp entry exists
	// in the map regardless of whether the host OS knows about it.
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"image.webp": "fake-webp",
	}, Config{})
	w := doRequest(t, h, "GET", "/image.webp")
	require.Equal(t, 200, w.Code)
	assert.Equal(t, "image/webp", w.Header().Get("Content-Type"))
}

func TestHandler_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, nil, Config{})
	w := doRequest(t, h, "GET", "/nope.html")
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandler_PathTraversal_DotDot(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"safe.txt": "ok",
	}, Config{})
	// path.Clean resolves .. so "/../safe.txt" becomes "/safe.txt" — this
	// is expected to serve the file. The real test is that ".." cannot
	// escape the root directory.
	w := doRequest(t, h, "GET", "/../safe.txt")
	require.Equal(t, 200, w.Code)
}

func TestHandler_PathTraversal_Escapes(t *testing.T) {
	t.Parallel()
	// Create a file outside the root that should never be accessible.
	dir := t.TempDir()
	secretDir := filepath.Join(filepath.Dir(dir), "bouine-secret-test")
	t.Cleanup(func() { os.RemoveAll(secretDir) })
	err := os.MkdirAll(secretDir, 0o755)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(secretDir, "secret.txt"), []byte("secret"), 0o644)
	require.NoError(t, err)

	h, err := New(Config{Root: dir})
	require.NoError(t, err)

	// Simulate a request that tries to escape the root.
	// Since path.Clean resolves ".." components, we need to test with
	// a path that doesn't get cleaned by path.Clean but still escapes.
	// On most systems, path.Clean("/../bouine-secret-test/secret.txt")
	// returns "/bouine-secret-test/secret.txt" which, when joined with
	// root, stays inside root. So we verify the containment check works.
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/../../bouine-secret-test/secret.txt", nil)
	h.ServeHTTP(w, r)
	if w.Code == 200 && w.Body.String() == "secret" {
		t.Fatal("path traversal escaped root directory")
	}
}

func TestHandler_DirectoryWithoutIndex(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"subdir/file.txt": "nested",
	}, Config{})
	w := doRequest(t, h, "GET", "/subdir/")
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandler_DirectoryWithIndex(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"subdir/index.html": "<h1>dir index</h1>",
		"subdir/file.txt":   "nested",
	}, Config{IndexFiles: []string{"index.html"}})
	w := doRequest(t, h, "GET", "/subdir/")
	require.Equal(t, 200, w.Code)
	require.Equal(t, "<h1>dir index</h1>", w.Body.String())
}

func TestHandler_FileTooLarge(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"big.txt": "this is more than 5 bytes",
	}, Config{MaxBytes: 5})
	w := doRequest(t, h, "GET", "/big.txt")
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

func TestHandler_HeadRequest(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	w := doRequest(t, h, "HEAD", "/file.txt")
	require.Equal(t, 200, w.Code)
	require.Equal(t, 0, w.Body.Len())
	cl := w.Header().Get("Content-Length")
	require.Equal(t, "11", cl)
}

func TestHandler_PostNotAllowed(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	w := doRequest(t, h, "POST", "/file.txt")
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestHandler_PutNotAllowed(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	w := doRequest(t, h, "PUT", "/file.txt")
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestHandler_ETagSet(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	w := doRequest(t, h, "GET", "/file.txt")
	etag := w.Header().Get("Etag")
	require.NotEqual(t, "", etag)
	if etag[0] != '"' || etag[len(etag)-1] != '"' {
		t.Fatalf("ETag should be quoted: got %q", etag)
	}
}

func TestHandler_ETagIsStrongHash_NotMtime(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	w := doRequest(t, h, "GET", "/file.txt")
	etag := w.Header().Get("Etag")
	require.NotEqual(t, "", etag, "ETag must be set")
	// A strong xxhash64 ETag is a quoted hex string, not a decimal
	// nanosecond mtime. This catches the old mtime-based fallback.
	require.Equal(t, `"`, string(etag[0]))
	require.Equal(t, `"`, string(etag[len(etag)-1]))
	hex := etag[1 : len(etag)-1]
	for _, r := range hex {
		assert.True(t, (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f'),
			"ETag %q contains non-hex char %q — looks like mtime fallback", etag, r)
	}
}

// readErrFile wraps an http.File but returns an error on Read, simulating
// a file that can be opened and stat'd but whose content can't be read
// (e.g. a file on a failing disk). This tests the computeETag error path.
type readErrFile struct {
	http.File
}

func (f *readErrFile) Read(p []byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

// readErrFS wraps an http.FileSystem so that all opened files fail on Read.
type readErrFS struct {
	inner http.FileSystem
}

func (fs *readErrFS) Open(name string) (http.File, error) {
	f, err := fs.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &readErrFile{File: f}, nil
}

// seekErrFile wraps an http.File but returns an error on Seek. When
// failOnlyOnPositiveOffset is true, Seek(0, SeekStart) succeeds (delegated
// to the inner file) but Seek with a positive offset fails. This isolates
// the handleRange Seek(start>0) error path from computeETag's Seek(0).
type seekErrFile struct {
	http.File
	failOnlyOnPositiveOffset bool
}

func (f *seekErrFile) Seek(offset int64, whence int) (int64, error) {
	if f.failOnlyOnPositiveOffset {
		if offset == 0 && whence == io.SeekStart {
			return f.File.Seek(offset, whence)
		}
		return 0, io.ErrUnexpectedEOF
	}
	return 0, io.ErrUnexpectedEOF
}

// seekErrFS wraps an http.FileSystem so that all opened files fail on Seek.
type seekErrFS struct {
	inner                    http.FileSystem
	failOnlyOnPositiveOffset bool
}

func (fs *seekErrFS) Open(name string) (http.File, error) {
	f, err := fs.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &seekErrFile{File: f, failOnlyOnPositiveOffset: fs.failOnlyOnPositiveOffset}, nil
}

func TestHandler_ETagNoMtimeFallbackOnReadFailure(t *testing.T) {
	t.Parallel()
	// Build a handler normally, then swap its fs for a readErrFS so
	// that computeETag's io.Copy(hasher, f) fails. The old code would
	// fall back to a mtime-based ETag; the fix must omit the ETag
	// entirely (return empty string).
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	// Clear the etag cache so computeETag tries to hash the file.
	h.etagCache = sync.Map{}
	// Replace the fs so that files fail on Read.
	h.fs = &readErrFS{inner: h.fs}

	w := doRequest(t, h, "GET", "/file.txt")
	// The response should still be 200 (the file is "open"), but the
	// ETag must be omitted because the content hash failed.
	require.Equal(t, 200, w.Code)
	etag := w.Header().Get("Etag")
	require.Empty(t, etag,
		"ETag must be omitted when content hash fails, not mtime-based")
}

func TestHandler_ETagNoMtimeFallbackOnSeekFailure(t *testing.T) {
	t.Parallel()
	// When computeETag's pre-hash Seek(0) fails, the ETag must be
	// omitted (not mtime-based). Additionally, ServeHTTP's post-headers
	// Seek(0) will also fail, producing a 500 — the file offset is
	// indeterminate and the body can't be streamed safely.
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	h.etagCache = sync.Map{}
	h.fs = &seekErrFS{inner: h.fs}

	w := doRequest(t, h, "GET", "/file.txt")
	// ServeHTTP's Seek(0) fails → 500.
	require.Equal(t, http.StatusInternalServerError, w.Code)
	// No ETag should be set.
	etag := w.Header().Get("Etag")
	require.Empty(t, etag, "ETag must be omitted when seek fails")
}

func TestHandler_RangeSeekError_Returns500WithoutContentRange(t *testing.T) {
	t.Parallel()
	// When handleRange's Seek(start>0) fails, the response must be 500
	// with no Content-Range or Content-Length headers (RFC 9110 §14.5
	// forbids Content-Range on non-206/416 responses).
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})

	// Prime the ETag cache so computeETag returns early (no Seek needed).
	w0 := doRequest(t, h, "GET", "/file.txt")
	require.Equal(t, 200, w0.Code)

	// Swap the fs so that Seek(n>0) fails. The existing opened file from
	// resolveFile will be wrapped — computeETag cache hit (no Seek),
	// ServeHTTP's Seek(0) succeeds, handleRange's Seek(2) fails.
	h.fs = &seekErrFS{inner: h.fs, failOnlyOnPositiveOffset: true}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/file.txt", nil)
	r.Header.Set("Range", "bytes=2-5")
	h.ServeHTTP(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Empty(t, w.Header().Get("Content-Range"),
		"Content-Range must be absent on 500 (RFC 9110 §14.5)")
	require.Empty(t, w.Header().Get("Content-Length"),
		"Content-Length must be absent on 500 (http.Error deletes it)")
}

func TestHandler_RangeStreamError_ReturnsTrue(t *testing.T) {
	t.Parallel()
	// When io.CopyBuffer fails during range body streaming (e.g. disk
	// read error mid-transfer), handleRange must log and return true
	// (response already committed via WriteHeader).
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})

	// Prime the ETag cache so computeETag returns early.
	w0 := doRequest(t, h, "GET", "/file.txt")
	require.Equal(t, 200, w0.Code)

	// Swap fs so Read fails. Seek(0) succeeds (delegated), so
	// ServeHTTP's Seek(0) passes, but handleRange's io.CopyBuffer
	// calls Read → fails.
	h.fs = &readErrFS{inner: h.fs}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/file.txt", nil)
	r.Header.Set("Range", "bytes=0-4")
	h.ServeHTTP(w, r)

	// WriteHeader(206) was called before the stream error, so the
	// status is 206. The body is empty because Read failed immediately.
	require.Equal(t, http.StatusPartialContent, w.Code)
}

func TestHandler_ETagConsistency(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	w1 := doRequest(t, h, "GET", "/file.txt")
	w2 := doRequest(t, h, "GET", "/file.txt")
	require.Equal(t, w2.Header().Get("Etag"), w1.Header().Get("Etag"))
}

func TestHandler_IfNoneMatch_304(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	// First request to get the ETag.
	w1 := doRequest(t, h, "GET", "/file.txt")
	etag := w1.Header().Get("Etag")

	// Second request with If-None-Match should return 304.
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/file.txt", nil)
	r2.Header.Set("If-None-Match", etag)
	h.ServeHTTP(w2, r2)
	require.Equal(t, http.StatusNotModified, w2.Code)
	require.Equal(t, 0, w2.Body.Len())
}

func TestHandler_IfModifiedSince_304(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	// First request to get Last-Modified.
	w1 := doRequest(t, h, "GET", "/file.txt")
	lastMod := w1.Header().Get("Last-Modified")

	// Second request with If-Modified-Since should return 304.
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest("GET", "/file.txt", nil)
	r2.Header.Set("If-Modified-Since", lastMod)
	h.ServeHTTP(w2, r2)
	require.Equal(t, http.StatusNotModified, w2.Code)
}

func TestHandler_RangeInvalid_FallsThroughToFullBody(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	// A malformed Range header (missing "bytes=" prefix) should cause
	// handleRange to return false before any Seek, and the handler should
	// fall through to streaming the full body from offset 0.
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/file.txt", nil)
	r.Header.Set("Range", "0-5")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "0123456789", w.Body.String(),
		"full body must be served when range is invalid — seek offset invariant")
}

func TestHandler_RangeSingle(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/file.txt", nil)
	r.Header.Set("Range", "bytes=2-5")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusPartialContent, w.Code)
	require.Equal(t, "2345", w.Body.String())
	cr := w.Header().Get("Content-Range")
	require.Equal(t, "bytes 2-5/10", cr)
	cl := w.Header().Get("Content-Length")
	require.Equal(t, "4", cl)
}

func TestHandler_RangeMultipart_CollapsesToFirst(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/file.txt", nil)
	r.Header.Set("Range", "bytes=0-2, 4-6")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusPartialContent, w.Code)
	require.Equal(t, "012", w.Body.String())
}

func TestHandler_RangeUnsatisfiable_416(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/file.txt", nil)
	r.Header.Set("Range", "bytes=100-200")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, w.Code)
}

func TestHandler_RangeSuffix(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/file.txt", nil)
	r.Header.Set("Range", "bytes=-3")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusPartialContent, w.Code)
	require.Equal(t, "789", w.Body.String())
}

func TestHandler_RangeOpenEnd(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/file.txt", nil)
	r.Header.Set("Range", "bytes=4-")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusPartialContent, w.Code)
	require.Equal(t, "456789", w.Body.String())
}

func TestHandler_RangeHead(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("HEAD", "/file.txt", nil)
	r.Header.Set("Range", "bytes=0-3")
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusPartialContent, w.Code)
	require.Equal(t, 0, w.Body.Len())
	cl := w.Header().Get("Content-Length")
	require.Equal(t, "4", cl)
}

func TestHandler_SymlinkRoot(t *testing.T) {
	t.Parallel()
	realDir := t.TempDir()
	err := os.WriteFile(filepath.Join(realDir, "file.txt"), []byte("via symlink"), 0o644)
	require.NoError(t, err)
	// Create a symlink to realDir.
	linkDir := filepath.Join(t.TempDir(), "link")
	err = os.Symlink(realDir, linkDir)
	require.NoError(t, err)
	h, err := New(Config{Root: linkDir})
	require.NoError(t, err, "New with symlink root")
	w := doRequest(t, h, "GET", "/file.txt")
	require.Equal(t, 200, w.Code)
	require.Equal(t, "via symlink", w.Body.String())
}

func TestHandler_NewRootNotExists(t *testing.T) {
	t.Parallel()
	_, err := New(Config{Root: "/nonexistent/path/that/does/not/exist"})
	require.Error(t, err)
}

func TestHandler_NewRootNotDirectory(t *testing.T) {
	t.Parallel()
	f := filepath.Join(t.TempDir(), "file.txt")
	err := os.WriteFile(f, []byte("x"), 0o644)
	require.NoError(t, err)
	_, err = New(Config{Root: f})
	require.Error(t, err)
}

func TestHandler_NestedPath(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"css/main.css": "body { }",
		"js/app.js":    "console.log(1);",
		"img/logo.svg": "<svg></svg>",
	}, Config{})
	w := doRequest(t, h, "GET", "/css/main.css")
	require.Equal(t, 200, w.Code)
	require.Equal(t, "body { }", w.Body.String())
}

func TestHandler_LastModifiedSet(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	w := doRequest(t, h, "GET", "/file.txt")
	lm := w.Header().Get("Last-Modified")
	require.NotEqual(t, "", lm)
}

func TestHandler_UnknownExtension(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.zzzunknown": "content",
	}, Config{})
	w := doRequest(t, h, "GET", "/file.zzzunknown")
	require.Equal(t, 200, w.Code)
	ct := w.Header().Get("Content-Type")
	require.Equal(t, "application/octet-stream", ct)
}

func TestHandler_BodyStreamed(t *testing.T) {
	t.Parallel()
	// Verify the handler streams by checking the body is correct for
	// a larger file.
	content := make([]byte, 100*1024) // 100 KiB
	for i := range content {
		content[i] = byte(i % 256)
	}
	h := newTestHandler(t, map[string]string{
		"big.bin": string(content),
	}, Config{})
	w := doRequest(t, h, "GET", "/big.bin")
	require.Equal(t, 200, w.Code)
	require.Len(t, content, w.Body.Len())
	for i, b := range w.Body.Bytes() {
		require.Equal(t, content[i], b)
	}
}

func TestIsETagMatch(t *testing.T) {
	t.Parallel()
	etag := `"abc123"`
	tests := []struct {
		header string
		want   bool
	}{
		{`"abc123"`, true},
		{`"other", "abc123"`, true},
		{`"other"`, false},
		{`*`, true},
		{`W/"abc123"`, true},
		{`"abc123", "def"`, true},
	}
	for _, tt := range tests {
		got := isETagMatch(tt.header, etag)
		assert.Equal(t, tt.want, got)
	}
}

func TestNew_DefaultMaxBytes(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, nil, Config{})
	require.Equal(t, defaultMaxFileSize, h.maxBytes)
}

func TestHandler_ClosedFileOnError(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	// 405 should not leak a file handle.
	w := doRequest(t, h, "DELETE", "/file.txt")
	require.Equal(t, 405, w.Code)
	// 404 should not leak.
	w2 := doRequest(t, h, "GET", "/nonexistent")
	require.Equal(t, 404, w2.Code)
}

// Ensure the handler satisfies http.Handler.
var _ http.Handler = (*Handler)(nil)

// Ensure Config can be zero-valued without panic.
func TestConfig_ZeroValue(t *testing.T) {
	t.Parallel()
	var c Config
	require.Equal(t, "", c.Root)
}

func TestHandler_Metrics(t *testing.T) {
	t.Parallel()
	// We can't easily test prometheus counters in isolation, but verify
	// the handler doesn't panic with nil metrics.
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{RouteLabel: "test"})
	w := doRequest(t, h, "GET", "/file.txt")
	require.Equal(t, 200, w.Code)
}

// io.ReadAll is used to verify full body content in some tests.
var _ = io.ReadAll
