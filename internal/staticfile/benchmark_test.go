// Package staticfile_test benchmarks the static file handler.
//
// Run with:
//
//	go test -bench=. -benchmem ./internal/staticfile/
package staticfile_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/thylong/bouine/internal/staticfile"
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
	r := httptest.NewRequest("GET", "/file.bin", nil)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			b.Fatalf("status: %d", w.Code)
		}
	}
}

func BenchmarkStaticFile_LargeFile(b *testing.B) {
	dir := setupBenchDir(b, 1024*1024) // 1 MiB
	h, err := staticfile.New(staticfile.Config{Root: dir})
	if err != nil {
		b.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/file.bin", nil)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			b.Fatalf("status: %d", w.Code)
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
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/file.bin", nil)
	h.ServeHTTP(w, r)
	etag := w.Header().Get("Etag")

	r2 := httptest.NewRequest("GET", "/file.bin", nil)
	r2.Header.Set("If-None-Match", etag)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r2)
		if w.Code != 304 {
			b.Fatalf("status: %d", w.Code)
		}
	}
}

func BenchmarkStaticFile_Range(b *testing.B) {
	dir := setupBenchDir(b, 1024*1024) // 1 MiB
	h, err := staticfile.New(staticfile.Config{Root: dir})
	if err != nil {
		b.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/file.bin", nil)
	r.Header.Set("Range", "bytes=0-1023")
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 206 {
			b.Fatalf("status: %d", w.Code)
		}
	}
}

func BenchmarkStaticFile_vsHTTPFileServer(b *testing.B) {
	dir := setupBenchDir(b, 1024)
	fs := http.FileServer(http.Dir(dir))
	r := httptest.NewRequest("GET", "/file.bin", nil)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := httptest.NewRecorder()
		fs.ServeHTTP(w, r)
		if w.Code != 200 {
			b.Fatalf("status: %d", w.Code)
		}
	}
}

func BenchmarkStaticFile_NotFound(b *testing.B) {
	dir := b.TempDir()
	h, err := staticfile.New(staticfile.Config{Root: dir})
	if err != nil {
		b.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/nonexistent", nil)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 404 {
			b.Fatalf("status: %d", w.Code)
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
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/file.bin", nil)
	h.ServeHTTP(w, r)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			b.Fatalf("status: %d", w.Code)
		}
	}
}

var _ = fmt.Sprintf
var _ = io.ReadAll
