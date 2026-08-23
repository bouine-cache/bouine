package staticfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/valyala/fasthttp"
)

// FuzzPathTraversal fuzzes the path cleaning and containment check.
// The handler must never serve a file outside the root directory,
// regardless of the input URL path.
func FuzzPathTraversal(f *testing.F) {
	dir := f.TempDir()
	secretDir := filepath.Join(filepath.Dir(dir), "bouine-fuzz-secret")
	if err := os.MkdirAll(secretDir, 0o755); err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { os.RemoveAll(secretDir) })
	if err := os.WriteFile(filepath.Join(secretDir, "secret.txt"), []byte("secret"), 0o644); err != nil {
		f.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "safe.txt"), []byte("safe"), 0o644); err != nil {
		f.Fatal(err)
	}

	h, err := New(Config{Root: dir})
	if err != nil {
		f.Fatal(err)
	}

	// Seed corpus with known traversal attempts.
	f.Add("../safe.txt")
	f.Add("/../safe.txt")
	f.Add("/../../bouine-fuzz-secret/secret.txt")
	f.Add("..%2f..%2fbouine-fuzz-secret/secret.txt")
	f.Add("/..%5c..%5c/bouine-fuzz-secret/secret.txt")
	f.Add("/./safe.txt")
	f.Add("//safe.txt")
	f.Add("/safe.txt")
	f.Add("")
	f.Add("/")
	f.Add("/../../../../../etc/passwd")
	f.Add("/%2e%2e/%2e%2e/bouine-fuzz-secret/secret.txt")

	f.Fuzz(func(t *testing.T, urlPath string) {
		// Skip paths with control characters or non-printable bytes.
		for _, r := range urlPath {
			if r < 0x20 || r == 0x7f {
				t.Skip()
			}
		}
		// Skip paths that would produce an invalid HTTP request line
		// (e.g. urlPath starting with a space creates " HTTP/1.0").
		if len(urlPath) > 0 && (urlPath[0] == ' ' || urlPath[0] == '\t') {
			t.Skip()
		}

		ctx := &fasthttp.RequestCtx{}
		ctx.Request.SetRequestURI("/" + urlPath)
		ctx.Request.Header.SetMethod("GET")
		h.ServeRequest(ctx)

		// The handler must NEVER return the secret file content.
		if strings.Contains(string(ctx.Response.Body()), "secret") {
			t.Fatalf("path traversal: urlPath=%q returned secret content", urlPath)
		}

		// If the response is 200, the body must be "safe" (the only
		// legitimate file in the root).
		if ctx.Response.StatusCode() == fasthttp.StatusOK && string(ctx.Response.Body()) != "safe" {
			t.Fatalf("unexpected 200 body: urlPath=%q body=%q", urlPath, string(ctx.Response.Body()))
		}
	})
}
