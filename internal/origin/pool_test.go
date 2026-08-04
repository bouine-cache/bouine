package origin

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func echoServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Echo-Host", r.Host)
		w.Header().Set("X-Echo-Path", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, r.Body)
	}))
}

func fivexxServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
}

func pool(t *testing.T, targets ...string) *Pool {
	t.Helper()
	p, err := NewPool(PoolConfig{
		Name:    "test",
		Targets: targets,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err, "NewPool")
	return p
}

func TestPool_RoundRobin(t *testing.T) {
	t.Parallel()
	s1 := echoServer(t)
	defer s1.Close()
	s2 := echoServer(t)
	defer s2.Close()

	p := pool(t, s1.Listener.Addr().String(), s2.Listener.Addr().String())
	h := p.Handler(0, nil)

	hits := map[string]int{}
	for range 10 {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/hello", nil)
		h.ServeHTTP(rr, req)
		require.Equal(t, 200, rr.Code)
		host := rr.Header().Get("X-Echo-Host")
		hits[host]++
	}
	require.Len(t, hits, 2)
}

func TestPool_PassiveHealth(t *testing.T) {
	t.Parallel()
	bad := fivexxServer(t)
	defer bad.Close()
	good := echoServer(t)
	defer good.Close()

	// Only bad target, so all requests hit it.
	p := pool(t, bad.Listener.Addr().String())
	h := p.Handler(3, nil)

	for range 5 {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		h.ServeHTTP(rr, req)
	}

	require.Len(t, p.Healthy(), 0)

	// Now add the bad target back + a good one and verify good stays.
	p.MarkHealthy(bad.Listener.Addr().String())

	p2 := pool(t, bad.Listener.Addr().String(), good.Listener.Addr().String())
	h2 := p2.Handler(3, nil)

	// Fire enough requests to eject the bad one again.
	for range 20 {
		rr := httptest.NewRecorder()
		h2.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
	}

	healthy := p2.Healthy()
	require.Len(t, healthy, 1)
	require.Equal(t, good.Listener.Addr().String(), healthy[0])
}

func TestPool_AllDown(t *testing.T) {
	t.Parallel()
	bad := fivexxServer(t)
	defer bad.Close()

	p := pool(t, bad.Listener.Addr().String())
	h := p.Handler(1, nil)

	// First request triggers ejection.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	// Second request: no healthy targets.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "/", nil))
	require.Equal(t, http.StatusBadGateway, rr2.Code)
}

func TestPool_MarkHealthy(t *testing.T) {
	t.Parallel()
	bad := fivexxServer(t)
	defer bad.Close()

	p := pool(t, bad.Listener.Addr().String())
	h := p.Handler(1, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	require.Len(t, p.Healthy(), 0)

	p.MarkHealthy(bad.Listener.Addr().String())
	require.Len(t, p.Healthy(), 1)
}

func TestPool_NoTargetsError(t *testing.T) {
	t.Parallel()
	_, err := NewPool(PoolConfig{Name: "empty", Targets: nil})
	if err == nil || !strings.Contains(err.Error(), "no targets") {
		t.Fatalf("expected no-targets error, got %v", err)
	}
}

func TestPool_ProxiesBody(t *testing.T) {
	t.Parallel()
	s := echoServer(t)
	defer s.Close()

	p := pool(t, s.Listener.Addr().String())
	h := p.Handler(0, nil)

	body := "hello bouine"
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/echo", strings.NewReader(body))
	h.ServeHTTP(rr, req)

	require.Equal(t, 200, rr.Code)
	got := rr.Body.String()
	require.Equal(t, body, got)
}

func TestPool_ResponseHeaderTimeout(t *testing.T) {
	t.Parallel()
	// Origin that delays writing response headers beyond the transport's
	// ResponseHeaderTimeout. The proxy's ErrorHandler must fire and
	// return 502 — proving the timeout is wired into the transport.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	p := pool(t, srv.Listener.Addr().String())
	transport := &http.Transport{
		ResponseHeaderTimeout: 50 * time.Millisecond,
	}
	h := p.Handler(0, transport)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusBadGateway, rr.Code)
}
