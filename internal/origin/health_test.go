package origin

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestActiveHealth_RecoversTarget(t *testing.T) {
	t.Parallel()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer healthy.Close()

	p := pool(t, healthy.Listener.Addr().String())
	// Manually eject the target.
	p.targets[0].healthy.Store(false)
	p.targets[0].errors.Store(10)

	if len(p.Healthy()) != 0 {
		t.Fatal("should be ejected")
	}

	hc := NewActiveHealthChecker(p, ActiveHealthConfig{
		Path:               "/",
		Interval:           10 * time.Millisecond,
		Timeout:            1 * time.Second,
		UnhealthyThreshold: 3,
		ExpectedCodes:      []int{200},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = hc.Run(ctx)

	if len(p.Healthy()) != 1 {
		t.Fatalf("target should be restored, got %v", p.Healthy())
	}
}

func TestActiveHealth_EjectsUnhealthy(t *testing.T) {
	t.Parallel()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	defer bad.Close()

	p := pool(t, bad.Listener.Addr().String())

	hc := NewActiveHealthChecker(p, ActiveHealthConfig{
		Path:               "/",
		Interval:           10 * time.Millisecond,
		Timeout:            1 * time.Second,
		UnhealthyThreshold: 2,
		ExpectedCodes:      []int{200},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = hc.Run(ctx)

	if len(p.Healthy()) != 0 {
		t.Fatalf("target should be ejected, got %v", p.Healthy())
	}
}
