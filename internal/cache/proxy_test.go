package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandler_PUTProxiesBodyCorrectly(t *testing.T) {
	var gotMethod, gotPath, gotBody string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(201)
		_, _ = io.WriteString(w, "OK")
	})
	h := testHandler(t, upstream)

	body := `{"response_headers":[["Cache-Control","max-age=60"]]}`
	req := httptest.NewRequest("PUT", "http://example.com/config/test-uuid", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 201 {
		t.Fatalf("status = %d, want 201; body = %q", rr.Code, rr.Body.String())
	}
	if gotMethod != "PUT" {
		t.Fatalf("upstream method = %q, want PUT", gotMethod)
	}
	if gotPath != "/config/test-uuid" {
		t.Fatalf("upstream path = %q, want /config/test-uuid", gotPath)
	}
	if gotBody != body {
		t.Fatalf("upstream body = %q, want %q", gotBody, body)
	}
}

func TestHandler_GETAfterPUTConfigSetup(t *testing.T) {
	// Simulates the cache-tests pattern: PUT /config/uuid then GET /test/uuid.
	var configuredBody string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/config/"):
			b, _ := io.ReadAll(r.Body)
			configuredBody = string(b)
			w.WriteHeader(201)
			_, _ = io.WriteString(w, "OK")
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/test/"):
			w.Header().Set("Cache-Control", "max-age=3600")
			w.Header().Set("ETag", `"v1"`)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "test-response")
		default:
			w.WriteHeader(404)
		}
	})
	h := testHandler(t, upstream)

	// Step 1: PUT config.
	putReq := httptest.NewRequest("PUT", "http://example.com/config/abc123",
		strings.NewReader(`{"test":"data"}`))
	putReq.Header.Set("Content-Type", "application/json")
	putRR := httptest.NewRecorder()
	h.ServeHTTP(putRR, putReq)

	if putRR.Code != 201 {
		t.Fatalf("PUT status = %d, want 201", putRR.Code)
	}
	if configuredBody != `{"test":"data"}` {
		t.Fatalf("config body = %q", configuredBody)
	}

	// Step 2: GET test (should be a MISS, fetched from origin).
	getRR := httptest.NewRecorder()
	h.ServeHTTP(getRR, httptest.NewRequest("GET", "http://example.com/test/abc123", nil))

	if getRR.Code != 200 {
		t.Fatalf("GET status = %d, want 200", getRR.Code)
	}
	if getRR.Body.String() != "test-response" {
		t.Fatalf("GET body = %q", getRR.Body.String())
	}
}
