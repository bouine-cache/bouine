package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/bouine-cache/bouine/internal/observability/responsewriter"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestNormaliseSource(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		want  string
	}{
		{"hot", "hot"},
		{"warm", "warm"},
		{"peer", "peer"},
		{"origin", "origin"},
		{"", ""},
		{"unknown", ""},
	}
	for _, c := range cases {
		got := normaliseSource(c.input)
		assert.Equal(t, c.want, got)
	}
}

func TestNormaliseCacheResult(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		want  string
	}{
		{"HIT", "HIT"},
		{"MISS", "MISS"},
		{"STALE", "STALE"},
		{"REVALIDATED", "REVALIDATED"},
		{"BYPASS", "BYPASS"},
		{"", "MISS"},
		{"WEIRD-CACHE-VALUE", "UNKNOWN"},
		{"hit", "UNKNOWN"},
	}
	for _, c := range cases {
		got := normaliseCacheResult(c.input)
		assert.Equal(t, c.want, got)
	}
}

// TestMiddleware_SpoofedRouteHeader_OnNoMatch verifies that an
// attacker-supplied X-Bouine-Route header on a 404 (no-match) does NOT
// appear as the route Prometheus label. The middleware must strip the
// header before dispatching to the router.
func TestMiddleware_SpoofedRouteHeader_OnNoMatch(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.XCache, "MISS")
		w.WriteHeader(http.StatusNotFound)
	}))

	req := httptest.NewRequest("GET", "/nonexistent", nil)
	req.Header.Set(header.XBouineRoute, "evil-route-12345")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	foundDefault := false
	for _, mf := range got {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			route := labelValue(metric, "route")
			assert.NotEqual(t, "evil-route-12345", route,
				"spoofed X-Bouine-Route header must not appear as route label")
			if route == "_default" {
				foundDefault = true
			}
		}
	}
	assert.True(t, foundDefault, "route label must be _default on no-match, not empty or spoofed")
}

// TestMiddleware_SpoofedRouteHeader_StrippedBeforeHandler verifies that
// the inbound X-Bouine-Route header is removed before the handler runs,
// so the handler never sees an attacker-controlled value.
func TestMiddleware_SpoofedRouteHeader_StrippedBeforeHandler(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	var seenHeader string
	h := m.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seenHeader = r.Header.Get(header.XBouineRoute)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(header.XBouineRoute, "attacker-value")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Empty(t, seenHeader,
		"handler must not see attacker-supplied X-Bouine-Route header")
}

// TestMiddleware_RouterSetsRouteLabel verifies that when the router sets
// the X-Bouine-Route header (simulating a route match), the metrics
// middleware uses it as the route label.
func TestMiddleware_RouterSetsRouteLabel(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate the router setting the header on match.
		r.Header[header.XBouineRoute] = []string{"my-route"}
		w.Header().Set(header.XCache, "MISS")
		w.WriteHeader(200)
	}))

	req := httptest.NewRequest("GET", "/api/v1/foo", nil)
	// Even if the attacker sets a spoofed value, the middleware strips it
	// before the handler runs, and the handler (router) sets the real one.
	req.Header.Set(header.XBouineRoute, "spoofed")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	found := false
	spoofedFound := false
	for _, mf := range got {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			route := labelValue(metric, "route")
			if route == "my-route" {
				found = true
			}
			if route == "spoofed" {
				spoofedFound = true
			}
		}
	}
	assert.True(t, found, "route label must be 'my-route' (set by router, not spoofed)")
	assert.False(t, spoofedFound, "spoofed route label must not appear in metrics")
}

// TestMiddleware_UnknownCacheResultMapsToUnknown verifies that an
// unknown X-Cache response header value maps to "UNKNOWN" and does not
// pass through as a Prometheus label.
func TestMiddleware_UnknownCacheResultMapsToUnknown(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.XCache, "WEIRD-CACHE-VALUE")
		w.WriteHeader(200)
	}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/test", nil))

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	found := false
	for _, mf := range got {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			cr := labelValue(metric, "cache_result")
			if cr == "UNKNOWN" {
				found = true
			}
			assert.NotEqual(t, "WEIRD-CACHE-VALUE", cr,
				"unknown X-Cache value must not appear as cache_result label")
		}
	}
	assert.True(t, found, "cache_result must be UNKNOWN for unrecognized X-Cache value")
}

func TestMiddleware_SourceLabel(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.XCache, "HIT")
		w.Header().Set(header.XCacheSource, "hot")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/test", nil))

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	var foundRequests, foundBytes bool
	for _, mf := range got {
		switch mf.GetName() {
		case "bouine_requests_total":
			for _, m := range mf.GetMetric() {
				if labelValue(m, "source") == "hot" && labelValue(m, "cache_result") == "HIT" {
					foundRequests = true
				}
			}
		case "bouine_response_bytes_total":
			for _, m := range mf.GetMetric() {
				if labelValue(m, "source") == "hot" && labelValue(m, "cache_result") == "HIT" {
					foundBytes = true
				}
			}
		}
	}
	assert.True(t, foundRequests)
	assert.True(t, foundBytes)
}

func TestMiddleware_SourceLabel_Empty(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.XCache, "BYPASS")
		w.WriteHeader(200)
	}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/test", nil))

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, mf := range got {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelValue(m, "cache_result") == "BYPASS" && labelValue(m, "source") == "" {
				return
			}
		}
	}
	t.Error("requests_total: no BYPASS series with empty source")
}

func TestResponseBytesOut_HasCacheResultAndSource(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.XCache, "MISS")
		w.Header().Set(header.XCacheSource, "origin")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body"))
	}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/test", nil))

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, mf := range got {
		if mf.GetName() != "bouine_response_bytes_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelValue(m, "cache_result") == "MISS" && labelValue(m, "source") == "origin" {
				assert.Len(t, "body", int(m.GetCounter().GetValue()))
				return
			}
		}
	}
	t.Error("response_bytes_total: no MISS/origin series found")
}

// labelValue returns the value of a Prometheus label by name.
func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func TestMethodIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, methodIndex("GET"))
	assert.Equal(t, 1, methodIndex("HEAD"))
	assert.Equal(t, 2, methodIndex("POST"))
	assert.Equal(t, 2, methodIndex(""))
}

func TestStatusIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, statusIndex(200))
	assert.Equal(t, 1, statusIndex(206))
	assert.Equal(t, 2, statusIndex(304))
	assert.Equal(t, 3, statusIndex(301))
	assert.Equal(t, 4, statusIndex(302))
	assert.Equal(t, 5, statusIndex(404))
	assert.Equal(t, 6, statusIndex(500))
	assert.Equal(t, -1, statusIndex(999))
	assert.Equal(t, -1, statusIndex(0))
}

func TestCacheResultIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, cacheResultIndex("HIT"))
	assert.Equal(t, 1, cacheResultIndex("MISS"))
	assert.Equal(t, 2, cacheResultIndex("STALE"))
	assert.Equal(t, 3, cacheResultIndex("REVALIDATED"))
	assert.Equal(t, 4, cacheResultIndex("BYPASS"))
	assert.Equal(t, -1, cacheResultIndex("UNKNOWN"))
}

func TestSourceIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, sourceIndex(string(api.SourceHot)))
	assert.Equal(t, 1, sourceIndex(string(api.SourceWarm)))
	assert.Equal(t, 2, sourceIndex(string(api.SourcePeer)))
	assert.Equal(t, 3, sourceIndex(string(api.SourceOrigin)))
	assert.Equal(t, 4, sourceIndex(""))
	assert.Equal(t, -1, sourceIndex("unknown"))
}

func TestAccessLogMessage(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "served cache hit", accessLogMessage("HIT", 200))
	assert.Equal(t, "served cache miss", accessLogMessage("MISS", 200))
	assert.Equal(t, "bypassed cache", accessLogMessage("BYPASS", 200))
	assert.Equal(t, "served stale response", accessLogMessage("STALE", 200))
	assert.Equal(t, "served revalidated response", accessLogMessage("REVALIDATED", 200))
	assert.Equal(t, "served uncached response", accessLogMessage("", 200))
	assert.Equal(t, "request completed with error", accessLogMessage("HIT", 500))
	assert.Contains(t, accessLogMessage("UNKNOWN", 200), "unknown")
}

func TestShouldLogAccess_ZeroRate(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{accessSampleRate: 0}
	assert.True(t, m.shouldLogAccess(api.Key{}))
}

func TestShouldLogAccess_WithKey(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{accessSampleRate: 100}
	key := api.Key{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	result := m.shouldLogAccess(key)
	_ = result
}

func TestBuildAccessLogAttrs(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{}
	r := httptest.NewRequest("GET", "http://example.com/path", nil)
	r.RemoteAddr = "1.2.3.4:5678"
	sw := &responsewriter.ResponseWriter{Status: 200, Bytes: 1024}
	attrs := m.buildAccessLogAttrs(r, sw, "HIT", 50*time.Millisecond)
	assert.NotEmpty(t, attrs)
	assert.Contains(t, attrs, "method")
	assert.Contains(t, attrs, "GET")
	assert.Contains(t, attrs, "host")
	assert.Contains(t, attrs, "path")
	assert.Contains(t, attrs, "status")
}
