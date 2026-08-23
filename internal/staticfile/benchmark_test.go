// Package staticfile_test benchmarks the static file handler.
//
// Run with:
//
//	go test -bench=. -benchmem ./internal/staticfile/
package staticfile_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/staticfile"
)

func setupBenchDir(b *testing.B, size int) string {
	b.Helper()
	dir := b.TempDir()
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 256)
	}
	if err := os.WriteFile(filepath.Join(dir, "file.bin"), content, 0o644); err != nil {
		b.Fatal(err)
	}
	return dir
}

func BenchmarkStaticFile_SmallFile(b *testing.B) {
	dir := setupBenchDir(b, 1024) // 1 KiB
	h, err := staticfile.New(staticfile.Config{Root: dir})
	if err != nil {
		b.Fatal(err)
	}
	r := &fasthttp.RequestCtx{}
	r.Request.SetRequestURI("/file.bin")
	r.Request.Header.SetMethod("GET")
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := &fasthttp.RequestCtx{}
		h.ServeRequest(w)
		if w.Response.StatusCode() != 200 {
			b.Fatalf("status: %d", w.Response.StatusCode())
		}
	}
}

func BenchmarkStaticFile_LargeFile(b *testing.B) {
	dir := setupBenchDir(b, 1024*1024) // 1 MiB
	h, err := staticfile.New(staticfile.Config{Root: dir})
	if err != nil {
		b.Fatal(err)
	}
	r := &fasthttp.RequestCtx{}
	r.Request.SetRequestURI("/file.bin")
	r.Request.Header.SetMethod("GET")
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := &fasthttp.RequestCtx{}
		h.ServeRequest(w)
		if w.Response.StatusCode() != 200 {
			b.Fatalf("status: %d", w.Response.StatusCode())
		}
	}
}

func BenchmarkStaticFile_ConditionalGET_304(b *testing.B) {
	dir := setupBenchDir(b, 1024)
	h, err := staticfile.New(staticfile.Config{Root: dir})
	if err != nil {
		b.Fatal(err)
	}
	// Get the ETag first.
	w := &fasthttp.RequestCtx{}
	r := &fasthttp.RequestCtx{}
	r.Request.SetRequestURI("/file.bin")
	r.Request.Header.SetMethod("GET")
	h.ServeRequest(w)
	etag := string(w.Response.Header.Peek("Etag"))

	r2 := &fasthttp.RequestCtx{}
	r2.Request.SetRequestURI("/file.bin")
	r2.Request.Header.SetMethod("GET")
	r2.Request.Header.Set("If-None-Match", etag)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := &fasthttp.RequestCtx{}
		h.ServeRequest(w)
		if w.Response.StatusCode() != 304 {
			b.Fatalf("status: %d", w.Response.StatusCode())
		}
	}
}

func BenchmarkStaticFile_Range(b *testing.B) {
	dir := setupBenchDir(b, 1024*1024) // 1 MiB
	h, err := staticfile.New(staticfile.Config{Root: dir})
	if err != nil {
		b.Fatal(err)
	}
	r := &fasthttp.RequestCtx{}
	r.Request.SetRequestURI("/file.bin")
	r.Request.Header.SetMethod("GET")
	r.Request.Header.Set("Range", "bytes=0-1023")
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := &fasthttp.RequestCtx{}
		h.ServeRequest(w)
		if w.Response.StatusCode() != 206 {
			b.Fatalf("status: %d", w.Response.StatusCode())
		}
	}
}

func BenchmarkStaticFile_NotFound(b *testing.B) {
	dir := b.TempDir()
	h, err := staticfile.New(staticfile.Config{Root: dir})
	if err != nil {
		b.Fatal(err)
	}
	r := &fasthttp.RequestCtx{}
	r.Request.SetRequestURI("/nonexistent")
	r.Request.Header.SetMethod("GET")
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := &fasthttp.RequestCtx{}
		h.ServeRequest(w)
		if w.Response.StatusCode() != 404 {
			b.Fatalf("status: %d", w.Response.StatusCode())
		}
	}
}

func BenchmarkStaticFile_ETagCached(b *testing.B) {
	dir := setupBenchDir(b, 4096)
	h, err := staticfile.New(staticfile.Config{Root: dir})
	if err != nil {
		b.Fatal(err)
	}
	// Prime the ETag cache with a first request.
	w := &fasthttp.RequestCtx{}
	r := &fasthttp.RequestCtx{}
	r.Request.SetRequestURI("/file.bin")
	r.Request.Header.SetMethod("GET")
	h.ServeRequest(w)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := &fasthttp.RequestCtx{}
		h.ServeRequest(w)
		if w.Response.StatusCode() != 200 {
			b.Fatalf("status: %d", w.Response.StatusCode())
		}
	}
}

var _ = fmt.Sprintf
var _ = io.ReadAll
