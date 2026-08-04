package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"
)

func TestHandler_PUTProxiesBodyCorrectly(t *testing.T) {
	t.Parallel()
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

	body := `{"response_headers":[[header.CacheControl,"max-age=60"]]}`
	req := httptest.NewRequest("PUT", "http://example.com/config/test-uuid", strings.NewReader(body))
	req.Header.Set(header.ContentType, "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, 201, rr.Code)
	require.Equal(t, "PUT", gotMethod)
	require.Equal(t, "/config/test-uuid", gotPath)
	require.Equal(t, body, gotBody)
}

func TestHandler_GETAfterPUTConfigSetup(t *testing.T) {
	t.Parallel()
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
			w.Header().Set(header.CacheControl, "max-age=3600")
			w.Header().Set(header.ETag, `"v1"`)
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
	putReq.Header.Set(header.ContentType, "application/json")
	putRR := httptest.NewRecorder()
	h.ServeHTTP(putRR, putReq)

	require.Equal(t, 201, putRR.Code)
	require.Equal(t, `{"test":"data"}`, configuredBody)

	// Step 2: GET test (should be a MISS, fetched from origin).
	getRR := httptest.NewRecorder()
	h.ServeHTTP(getRR, httptest.NewRequest("GET", "http://example.com/test/abc123", nil))

	require.Equal(t, 200, getRR.Code)
	require.Equal(t, "test-response", getRR.Body.String())
}
