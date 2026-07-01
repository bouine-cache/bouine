package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/header"
)

func newMaxSizeHandler(t *testing.T, upstream http.Handler, maxSize int64) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:      upstream,
		Store:         store,
		MaxObjectSize: maxSize,
	})
}

func TestMaxObjectSize_SmallResponseCached(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "small")
	})
	h := newMaxSizeHandler(t, upstream, 1024)

	url := "http://example.com/small"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", url, nil))

	if rr.Header().Get(header.XCache) != "HIT" {
		t.Errorf("small response should be cached, got X-Cache=%q", rr.Header().Get(header.XCache))
	}
	if calls != 1 {
		t.Errorf("origin called %d times, want 1", calls)
	}
}

func TestMaxObjectSize_LargeResponseSkipped(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("x", 2048)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxSizeHandler(t, upstream, 1024)

	url := "http://example.com/large"
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest("GET", url, nil))
	if rr1.Code != 200 || rr1.Body.String() != body {
		t.Fatalf("first response wrong: status=%d body=%q", rr1.Code, rr1.Body.String())
	}

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", url, nil))

	if calls != 2 {
		t.Errorf("origin called %d times, want 2 (large response must not be cached)", calls)
	}
	if rr2.Header().Get(header.XCache) == "HIT" {
		t.Error("large response should NOT be a HIT")
	}
}

func TestMaxObjectSize_ZeroDisabled(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("x", 4096)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxSizeHandler(t, upstream, 0)

	url := "http://example.com/nolimit"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", url, nil))

	if rr.Header().Get(header.XCache) != "HIT" {
		t.Errorf("zero max_object_size should not limit caching, got X-Cache=%q", rr.Header().Get(header.XCache))
	}
	if calls != 1 {
		t.Errorf("origin called %d times, want 1", calls)
	}
}

func TestMaxObjectSize_ExactBoundaryCached(t *testing.T) {
	t.Parallel()
	body := strings.Repeat("a", 512)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxSizeHandler(t, upstream, 512)

	url := "http://example.com/exact"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	key := BuildKey(httptest.NewRequest("GET", url, nil))
	obj, _ := h.store.Get(httptest.NewRequest("GET", url, nil).Context(), key)
	if obj == nil {
		t.Fatal("response at exact boundary should be cached")
	}
}
