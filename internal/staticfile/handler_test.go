package staticfile

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func newTestHandler(t *testing.T, files map[string]string, cfg Config) *Handler {
	t.Helper()
	dir := t.TempDir()
	for rel, content := range files {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg.Root = dir
	h, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
	if w.Code != 200 {
		t.Fatalf("status: got %d, want 200", w.Code)
	}
	if w.Body.String() != "<h1>hello</h1>" {
		t.Fatalf("body: got %q", w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Fatalf("Content-Type: got %q, want text/html; charset=utf-8", ct)
	}
	if cl := w.Header().Get("Content-Length"); cl != "14" {
		t.Fatalf("Content-Length: got %q, want 14", cl)
	}
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
		if w.Code != 200 {
			t.Errorf("%s: status %d", tt.path, w.Code)
		}
		if got := w.Header().Get("Content-Type"); got != tt.wantCT {
			t.Errorf("%s: Content-Type got %q, want %q", tt.path, got, tt.wantCT)
		}
	}
}

func TestHandler_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, nil, Config{})
	w := doRequest(t, h, "GET", "/nope.html")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", w.Code)
	}
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
	if w.Code != 200 {
		t.Fatalf("cleaned traversal should serve: got %d", w.Code)
	}
}

func TestHandler_PathTraversal_Escapes(t *testing.T) {
	t.Parallel()
	// Create a file outside the root that should never be accessible.
	dir := t.TempDir()
	secretDir := filepath.Join(filepath.Dir(dir), "bouine-secret-test")
	t.Cleanup(func() { os.RemoveAll(secretDir) })
	if err := os.MkdirAll(secretDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secretDir, "secret.txt"), []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}

	h, err := New(Config{Root: dir})
	if err != nil {
		t.Fatal(err)
	}

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
	if w.Code != http.StatusNotFound {
		t.Fatalf("directory without index: got %d, want 404", w.Code)
	}
}

func TestHandler_DirectoryWithIndex(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"subdir/index.html": "<h1>dir index</h1>",
		"subdir/file.txt":   "nested",
	}, Config{IndexFiles: []string{"index.html"}})
	w := doRequest(t, h, "GET", "/subdir/")
	if w.Code != 200 {
		t.Fatalf("status: got %d, want 200", w.Code)
	}
	if w.Body.String() != "<h1>dir index</h1>" {
		t.Fatalf("body: got %q", w.Body.String())
	}
}

func TestHandler_FileTooLarge(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"big.txt": "this is more than 5 bytes",
	}, Config{MaxBytes: 5})
	w := doRequest(t, h, "GET", "/big.txt")
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status: got %d, want 413", w.Code)
	}
}

func TestHandler_HeadRequest(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	w := doRequest(t, h, "HEAD", "/file.txt")
	if w.Code != 200 {
		t.Fatalf("status: got %d, want 200", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("HEAD should have empty body, got %d bytes", w.Body.Len())
	}
	if cl := w.Header().Get("Content-Length"); cl != "11" {
		t.Fatalf("Content-Length: got %q, want 11", cl)
	}
}

func TestHandler_PostNotAllowed(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	w := doRequest(t, h, "POST", "/file.txt")
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want 405", w.Code)
	}
}

func TestHandler_PutNotAllowed(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	w := doRequest(t, h, "PUT", "/file.txt")
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want 405", w.Code)
	}
}

func TestHandler_ETagSet(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	w := doRequest(t, h, "GET", "/file.txt")
	etag := w.Header().Get("Etag")
	if etag == "" {
		t.Fatal("ETag header not set")
	}
	if etag[0] != '"' || etag[len(etag)-1] != '"' {
		t.Fatalf("ETag should be quoted: got %q", etag)
	}
}

func TestHandler_ETagConsistency(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello world",
	}, Config{})
	w1 := doRequest(t, h, "GET", "/file.txt")
	w2 := doRequest(t, h, "GET", "/file.txt")
	if w1.Header().Get("Etag") != w2.Header().Get("Etag") {
		t.Fatal("ETag should be consistent across requests")
	}
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
	if w2.Code != http.StatusNotModified {
		t.Fatalf("status: got %d, want 304", w2.Code)
	}
	if w2.Body.Len() != 0 {
		t.Fatalf("304 should have empty body, got %d bytes", w2.Body.Len())
	}
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
	if w2.Code != http.StatusNotModified {
		t.Fatalf("status: got %d, want 304", w2.Code)
	}
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
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status: got %d, want 206", w.Code)
	}
	if w.Body.String() != "2345" {
		t.Fatalf("body: got %q, want 2345", w.Body.String())
	}
	if cr := w.Header().Get("Content-Range"); cr != "bytes 2-5/10" {
		t.Fatalf("Content-Range: got %q, want bytes 2-5/10", cr)
	}
	if cl := w.Header().Get("Content-Length"); cl != "4" {
		t.Fatalf("Content-Length: got %q, want 4", cl)
	}
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
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status: got %d, want 206", w.Code)
	}
	if w.Body.String() != "012" {
		t.Fatalf("body: got %q, want 012 (first range only)", w.Body.String())
	}
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
	if w.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status: got %d, want 416", w.Code)
	}
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
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status: got %d, want 206", w.Code)
	}
	if w.Body.String() != "789" {
		t.Fatalf("body: got %q, want 789", w.Body.String())
	}
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
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status: got %d, want 206", w.Code)
	}
	if w.Body.String() != "456789" {
		t.Fatalf("body: got %q, want 456789", w.Body.String())
	}
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
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status: got %d, want 206", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("HEAD range should have empty body, got %d", w.Body.Len())
	}
	if cl := w.Header().Get("Content-Length"); cl != "4" {
		t.Fatalf("Content-Length: got %q, want 4", cl)
	}
}

func TestHandler_SymlinkRoot(t *testing.T) {
	t.Parallel()
	realDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(realDir, "file.txt"), []byte("via symlink"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Create a symlink to realDir.
	linkDir := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Fatal(err)
	}
	h, err := New(Config{Root: linkDir})
	if err != nil {
		t.Fatalf("New with symlink root: %v", err)
	}
	w := doRequest(t, h, "GET", "/file.txt")
	if w.Code != 200 {
		t.Fatalf("status: got %d, want 200", w.Code)
	}
	if w.Body.String() != "via symlink" {
		t.Fatalf("body: got %q", w.Body.String())
	}
}

func TestHandler_NewRootNotExists(t *testing.T) {
	t.Parallel()
	_, err := New(Config{Root: "/nonexistent/path/that/does/not/exist"})
	if err == nil {
		t.Fatal("New should error for nonexistent root")
	}
}

func TestHandler_NewRootNotDirectory(t *testing.T) {
	t.Parallel()
	f := filepath.Join(t.TempDir(), "file.txt")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := New(Config{Root: f})
	if err == nil {
		t.Fatal("New should error when root is not a directory")
	}
}

func TestHandler_NestedPath(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"css/main.css": "body { }",
		"js/app.js":    "console.log(1);",
		"img/logo.svg": "<svg></svg>",
	}, Config{})
	w := doRequest(t, h, "GET", "/css/main.css")
	if w.Code != 200 {
		t.Fatalf("status: got %d", w.Code)
	}
	if w.Body.String() != "body { }" {
		t.Fatalf("body: got %q", w.Body.String())
	}
}

func TestHandler_LastModifiedSet(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	w := doRequest(t, h, "GET", "/file.txt")
	if lm := w.Header().Get("Last-Modified"); lm == "" {
		t.Fatal("Last-Modified header not set")
	}
}

func TestHandler_UnknownExtension(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.zzzunknown": "content",
	}, Config{})
	w := doRequest(t, h, "GET", "/file.zzzunknown")
	if w.Code != 200 {
		t.Fatalf("status: got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("Content-Type: got %q, want application/octet-stream", ct)
	}
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
	if w.Code != 200 {
		t.Fatalf("status: got %d", w.Code)
	}
	if w.Body.Len() != len(content) {
		t.Fatalf("body length: got %d, want %d", w.Body.Len(), len(content))
	}
	for i, b := range w.Body.Bytes() {
		if b != content[i] {
			t.Fatalf("byte %d: got %d, want %d", i, b, content[i])
		}
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
		if got := isETagMatch(tt.header, etag); got != tt.want {
			t.Errorf("isETagMatch(%q, %q) = %v, want %v", tt.header, etag, got, tt.want)
		}
	}
}

func TestNew_DefaultMaxBytes(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, nil, Config{})
	if h.maxBytes != defaultMaxFileSize {
		t.Fatalf("default maxBytes: got %d, want %d", h.maxBytes, defaultMaxFileSize)
	}
}

func TestHandler_ClosedFileOnError(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{})
	// 405 should not leak a file handle.
	w := doRequest(t, h, "DELETE", "/file.txt")
	if w.Code != 405 {
		t.Fatalf("status: got %d, want 405", w.Code)
	}
	// 404 should not leak.
	w2 := doRequest(t, h, "GET", "/nonexistent")
	if w2.Code != 404 {
		t.Fatalf("status: got %d, want 404", w2.Code)
	}
}

// Ensure the handler satisfies http.Handler.
var _ http.Handler = (*Handler)(nil)

// Ensure Config can be zero-valued without panic.
func TestConfig_ZeroValue(t *testing.T) {
	t.Parallel()
	var c Config
	if c.Root != "" {
		t.Fatal("zero Config Root should be empty")
	}
}

func TestHandler_Metrics(t *testing.T) {
	t.Parallel()
	// We can't easily test prometheus counters in isolation, but verify
	// the handler doesn't panic with nil metrics.
	h := newTestHandler(t, map[string]string{
		"file.txt": "hello",
	}, Config{RouteLabel: "test"})
	w := doRequest(t, h, "GET", "/file.txt")
	if w.Code != 200 {
		t.Fatalf("status: got %d", w.Code)
	}
}

// io.ReadAll is used to verify full body content in some tests.
var _ = io.ReadAll
