package observability

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetrics_Defaults(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	if m.Registry == nil {
		t.Fatal("registry nil")
	}
	got, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("default collectors registered nothing")
	}
}

func TestMetrics_Handler_ExposesRegisteredCollector(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bouine_test_counter",
		Help: "test",
	})
	m.Registry.MustRegister(counter)
	counter.Inc()

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/metrics", nil)
	m.Handler().ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("status = %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "bouine_test_counter 1") {
		t.Fatalf("metric not exposed: %s", body)
	}
}
