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
	require.NoErrorf(t, err, "gather: %v", err)
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
	require.NoErrorf(t, err, "gather: %v", err)
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
	require.NoErrorf(t, err, "gather: %v", err)
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
