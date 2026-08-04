package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func newMaxResponseBytesHandler(t *testing.T, upstream http.Handler, maxBytes int64) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:         upstream,
		Store:            store,
		MaxResponseBytes: maxBytes,
	})
}

func TestMaxResponseBytes_OverLimitReturns502(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("x", 2048)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxResponseBytesHandler(t, upstream, 1024)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/too-big", nil))

	require.Equal(t, http.StatusBadGateway, rr.Code)
	assert.Equal(t, 1, calls)

	// Second request should also miss (nothing cached).
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/too-big", nil))
	assert.NotEqual(t, "HIT", rr2.Header().Get(header.XCache))
	assert.Equal(t, 2, calls)
}

func TestMaxResponseBytes_UnderLimitSucceeds(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("y", 512)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxResponseBytesHandler(t, upstream, 1024)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/ok", nil))
	if rr.Code != 200 || rr.Body.String() != body {
		t.Fatalf("response under limit should pass through: status=%d body=%q", rr.Code, rr.Body.String())
	}

	// Should be cached.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/ok", nil))
	assert.Equal(t, "HIT", rr2.Header().Get(header.XCache))
	assert.Equal(t, 1, calls)
}

func TestMaxResponseBytes_ExactBoundarySucceeds(t *testing.T) {
	t.Parallel()
	body := strings.Repeat("a", 512)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxResponseBytesHandler(t, upstream, 512)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/exact", nil))
	if rr.Code != 200 || rr.Body.String() != body {
		t.Fatalf("response at exact boundary should pass through: status=%d body=%q", rr.Code, rr.Body.String())
	}
}

func TestMaxResponseBytes_DefaultAppliedWhenZero(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(header.CacheControl, "max-age=60")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "small")
		}),
		Store:            store,
		MaxResponseBytes: 0, // should default to 4 MiB
	})
	const wantDefault = 4 << 20 // 4 MiB — matches defaultMaxResponseBytes
	require.Equal(t, int64(wantDefault), h.maxResponseBytes)
}

func TestMaxResponseBytes_InvalidateAndProxyOverLimit(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("z", 2048)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxResponseBytesHandler(t, upstream, 1024)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("POST", "http://example.com/post", strings.NewReader("")))

	require.Equal(t, http.StatusBadGateway, rr.Code)
}
