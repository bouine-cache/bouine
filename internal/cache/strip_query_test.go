package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestBuildKey_StripQueryParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm_source": true, "fbclid": true}, nil, nil, nil, false, false)

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=1&utm_source=email&b=2", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, nil)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_StripQueryParams_AllStripped(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"a": true, "b": true}, nil, nil, nil, false, false)

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, nil)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_StripQueryParams_NilNoEffect(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2", nil)
	k1 := BuildKey(r, nil)
	k2 := BuildKey(r, nil)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_StripQueryParams_StripsSingleParam(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm_source": true}, nil, nil, nil, false, false)

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=1&utm_source=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, nil)

	assert.Equal(t, k2, k1)
}

func TestStripQueryParams_HandlerIntegration(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("utm_source") == "" {
			t.Error("upstream should receive the full query string including utm_source")
		}
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: upstream,
		Store:    store,
		Policy:   NewKeyPolicy(map[string]bool{"utm_source": true, "fbclid": true}, nil, nil, nil, false, false),
	})

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("GET", "http://example.com/page?a=1&utm_source=email", nil))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr,
		httptest.NewRequest("GET", "http://example.com/page?a=1&utm_source=twitter", nil))

	assert.Equal(t, "HIT", rr.Header().Get(header.XCache))
	assert.Equal(t, 1, calls)
}
