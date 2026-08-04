package observability

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetrics_Defaults(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	require.NotNil(t, m.Registry)
	got, err := m.Registry.Gather()
	require.NoError(t, err, "gather")
	require.NotEmpty(t, got)
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

	require.Equal(t, 200, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "bouine_test_counter 1")
}
