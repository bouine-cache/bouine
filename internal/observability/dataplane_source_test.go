package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

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
		{"EVIL-ROUTE-12345", "UNKNOWN"},
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
	for _, mf := range got {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			route := labelValue(metric, "route")
			assert.NotEqual(t, "evil-route-12345", route,
				"spoofed X-Bouine-Route header must not appear as route label")
		}
	}
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
	for _, mf := range got {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if labelValue(metric, "route") == "my-route" {
				found = true
			}
		}
	}
	assert.True(t, found, "route label must be 'my-route' (set by router, not spoofed)")
}

// TestMiddleware_UnknownCacheResultMapsToUnknown verifies that an
// unknown X-Cache response header value maps to "UNKNOWN" and does not
// pass through as a Prometheus label.
func TestMiddleware_UnknownCacheResultMapsToUnknown(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.XCache, "EVIL-CACHE-VALUE")
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
		for _, metric := range mf.GetMetric() {
			cr := labelValue(metric, "cache_result")
			assert.NotEqual(t, "EVIL-CACHE-VALUE", cr,
				"unknown X-Cache value must not appear as cache_result label")
		}
	}
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
