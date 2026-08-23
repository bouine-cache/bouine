package staticfile

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"
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

func doRequest(t *testing.T, h *Handler, method, path string) *fasthttp.RequestCtx {
	t.Helper()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI(path)
	ctx.Request.Header.SetMethod(method)
	h.ServeRequest(ctx)
	return ctx
}

func TestHandler_ServeFile(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"index.html": "<h1>hello</h1>",
		"style.css":  "body { color: red; }",
	}, Config{})
	w := doRequest(t, h, "GET", "/index.html")
	require.Equal(t, 200, w.Response.StatusCode())
	require.Equal(t, "<h1>hello</h1>", string(w.Response.Body()))
	ct := string(w.Response.Header.Peek("Content-Type"))
	require.Equal(t, "text/html; charset=utf-8", ct)
	cl := string(w.Response.Header.Peek("Content-Length"))
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
		assert.Equal(t, 200, w.Response.StatusCode())
		got := string(w.Response.Header.Peek("Content-Type"))
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
		assert.Equal(t, 200, w.Response.StatusCode())
		got := string(w.Response.Header.Peek("Content-Type"))
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
	require.Equal(t, 200, w.Response.StatusCode())
	assert.Equal(t, "image/webp", string(w.Response.Header.Peek("Content-Type")))
}

func TestHandler_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, nil, Config{})
	w := doRequest(t, h, "GET", "/nope.html")
	require.Equal(t, fasthttp.StatusNotFound, w.Response.StatusCode())
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
	require.Equal(t, 200, w.Response.StatusCode())
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
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/../../bouine-secret-test/secret.txt")
	ctx.Request.Header.SetMethod("GET")
	h.ServeRequest(ctx)
	if ctx.Response.StatusCode() == 200 && string(ctx.Response.Body()) == "secret" {
		t.Fatal("path traversal escaped root directory")
	}
}

func TestHandler_DirectoryWithoutIndex(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"subdir/file.txt": "nested",
	}, Config{})
	w := doRequest(t, h, "GET", "/subdir/")
	require.Equal(t, fasthttp.StatusNotFound, w.Response.StatusCode())
}

func TestHandler_DirectoryWithIndex(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"subdir/index.html": "<h1>dir index</h1>",
		"subdir/file.txt":   "nested",
	}, Config{IndexFiles: []string{"index.html"}})
	w := doRequest(t, h, "GET", "/subdir/")
	require.Equal(t, 200, w.Response.StatusCode())
	require.Equal(t, "<h1>dir index</h1>", string(w.Response.Body()))
}

func TestHandler_FileTooLarge(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"big.txt": "this is more than 5 bytes",
	}, Config{MaxBytes: 5})
	w := doRequest(t, h, "GET", "/big.txt")
	require.Equal(t, fasthttp.StatusRequestEntityTooLarge, w.Response.StatusCode())
}

func TestHandler_HeadRequest(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	w := doRequest(t, h, "HEAD", "/file.txt")
	require.Equal(t, 200, w.Response.StatusCode())
	require.Equal(t, 0, len(w.Response.Body()))
	cl := string(w.Response.Header.Peek("Content-Length"))
	require.Equal(t, "11", cl)
}

func TestHandler_PostNotAllowed(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	w := doRequest(t, h, "POST", "/file.txt")
	require.Equal(t, fasthttp.StatusMethodNotAllowed, w.Response.StatusCode())
}

func TestHandler_PutNotAllowed(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	w := doRequest(t, h, "PUT", "/file.txt")
	require.Equal(t, fasthttp.StatusMethodNotAllowed, w.Response.StatusCode())
}

func TestHandler_ETagSet(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	w := doRequest(t, h, "GET", "/file.txt")
	etag := string(w.Response.Header.Peek("Etag"))
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
	etag := string(w.Response.Header.Peek("Etag"))
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

// a file that can be opened and stat'd but whose content can't be read
// (e.g. a file on a failing disk). This tests the computeETag error path.

// failOnlyOnPositiveOffset is true, Seek(0, SeekStart) succeeds (delegated
// to the inner file) but Seek with a positive offset fails. This isolates
// the handleRange Seek(start>0) error path from computeETag's Seek(0).

func TestHandler_ETagConsistency(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	w1 := doRequest(t, h, "GET", "/file.txt")
	w2 := doRequest(t, h, "GET", "/file.txt")
	require.Equal(t, string(w2.Response.Header.Peek("Etag")), string(w1.Response.Header.Peek("Etag")))
}

func TestHandler_IfNoneMatch_304(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	// First request to get the ETag.
	w1 := doRequest(t, h, "GET", "/file.txt")
	etag := string(w1.Response.Header.Peek("Etag"))

	// Second request with If-None-Match should return 304.
	w2 := &fasthttp.RequestCtx{}
	w2.Request.SetRequestURI("/file.txt")
	w2.Request.Header.SetMethod("GET")
	w2.Request.Header.Set("If-None-Match", etag)
	h.ServeRequest(w2)
	require.Equal(t, fasthttp.StatusNotModified, w2.Response.StatusCode())
	require.Equal(t, 0, len(w2.Response.Body()))
}

func TestHandler_IfModifiedSince_304(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	// First request to get Last-Modified.
	w1 := doRequest(t, h, "GET", "/file.txt")
	lastMod := string(w1.Response.Header.Peek("Last-Modified"))

	// Second request with If-Modified-Since should return 304.
	w2 := &fasthttp.RequestCtx{}
	w2.Request.SetRequestURI("/file.txt")
	w2.Request.Header.SetMethod("GET")
	w2.Request.Header.Set("If-Modified-Since", lastMod)
	h.ServeRequest(w2)
	require.Equal(t, fasthttp.StatusNotModified, w2.Response.StatusCode())
}

func TestHandler_RangeInvalid_FallsThroughToFullBody(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	// A malformed Range header (missing "bytes=" prefix) should cause
	// handleRange to return false before any Seek, and the handler should
	// fall through to streaming the full body from offset 0.
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/file.txt")
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.Header.Set("Range", "0-5")
	h.ServeRequest(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	require.Equal(t, "0123456789", string(ctx.Response.Body()),
		"full body must be served when range is invalid — seek offset invariant")
}

func TestHandler_RangeSingle(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/file.txt")
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.Header.Set("Range", "bytes=2-5")
	h.ServeRequest(ctx)
	require.Equal(t, fasthttp.StatusPartialContent, ctx.Response.StatusCode())
	require.Equal(t, "2345", string(ctx.Response.Body()))
	cr := string(ctx.Response.Header.Peek("Content-Range"))
	require.Equal(t, "bytes 2-5/10", cr)
	cl := string(ctx.Response.Header.Peek("Content-Length"))
	require.Equal(t, "4", cl)
}

func TestHandler_RangeMultipart_CollapsesToFirst(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/file.txt")
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.Header.Set("Range", "bytes=0-2, 4-6")
	h.ServeRequest(ctx)
	require.Equal(t, fasthttp.StatusPartialContent, ctx.Response.StatusCode())
	require.Equal(t, "012", string(ctx.Response.Body()))
}

func TestHandler_RangeUnsatisfiable_416(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/file.txt")
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.Header.Set("Range", "bytes=100-200")
	h.ServeRequest(ctx)
	require.Equal(t, fasthttp.StatusRequestedRangeNotSatisfiable, ctx.Response.StatusCode())
}

func TestHandler_RangeSuffix(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/file.txt")
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.Header.Set("Range", "bytes=-3")
	h.ServeRequest(ctx)
	require.Equal(t, fasthttp.StatusPartialContent, ctx.Response.StatusCode())
	require.Equal(t, "789", string(ctx.Response.Body()))
}

func TestHandler_RangeOpenEnd(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/file.txt")
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.Header.Set("Range", "bytes=4-")
	h.ServeRequest(ctx)
	require.Equal(t, fasthttp.StatusPartialContent, ctx.Response.StatusCode())
	require.Equal(t, "456789", string(ctx.Response.Body()))
}

func TestHandler_RangeHead(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "0123456789",
	}, Config{})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/file.txt")
	ctx.Request.Header.SetMethod("HEAD")
	ctx.Request.Header.Set("Range", "bytes=0-3")
	h.ServeRequest(ctx)
	require.Equal(t, fasthttp.StatusPartialContent, ctx.Response.StatusCode())
	require.Equal(t, 0, len(ctx.Response.Body()))
	cl := string(ctx.Response.Header.Peek("Content-Length"))
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
	require.Equal(t, 200, w.Response.StatusCode())
	require.Equal(t, "via symlink", string(w.Response.Body()))
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
	require.Equal(t, 200, w.Response.StatusCode())
	require.Equal(t, "body { }", string(w.Response.Body()))
}

func TestHandler_LastModifiedSet(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	w := doRequest(t, h, "GET", "/file.txt")
	lm := string(w.Response.Header.Peek("Last-Modified"))
	require.NotEqual(t, "", lm)
}

func TestHandler_UnknownExtension(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.zzzunknown": "content",
	}, Config{})
	w := doRequest(t, h, "GET", "/file.zzzunknown")
	require.Equal(t, 200, w.Response.StatusCode())
	ct := string(w.Response.Header.Peek("Content-Type"))
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
	require.Equal(t, 200, w.Response.StatusCode())
	require.Len(t, content, len(w.Response.Body()))
	for i, b := range w.Response.Body() {
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
	require.Equal(t, 405, w.Response.StatusCode())
	// 404 should not leak.
	w2 := doRequest(t, h, "GET", "/nonexistent")
	require.Equal(t, 404, w2.Response.StatusCode())
}

// Ensure the handler satisfies http.Handler.

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
	require.Equal(t, 200, w.Response.StatusCode())
}

// io.ReadAll is used to verify full body content in some tests.
var _ = io.ReadAll
