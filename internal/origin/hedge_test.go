package origin

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/testutil/poll"
)

func TestHedgedTransport_FastResponse(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	ht := &HedgedTransport{
		Inner:   srv.Client().Transport,
		Timeout: 5 * time.Second,
	}
	req, _ := http.NewRequest("GET", srv.URL+"/fast", nil)
	resp, err := ht.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	// Fast response: hedge should not fire.
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestHedgedTransport_SlowFiresHedge(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			<-time.After(200 * time.Millisecond)
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	ht := &HedgedTransport{
		Inner:   srv.Client().Transport,
		Timeout: 50 * time.Millisecond,
	}
	req, _ := http.NewRequest("GET", srv.URL+"/slow", nil)
	resp, err := ht.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	// Hedge should have fired.
	poll.Eventually(t, 2*time.Second, 10*time.Millisecond, func() bool {
		return calls.Load() >= 2
	})
}

func TestHedgedTransport_NoGoroutineLeak(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-time.After(200 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	ht := &HedgedTransport{
		Inner:   srv.Client().Transport,
		Timeout: 50 * time.Millisecond,
	}

	before := runtime.NumGoroutine()
	const iterations = 50
	for range iterations {
		req, _ := http.NewRequest("GET", srv.URL+"/slow", nil)
		resp, err := ht.RoundTrip(req)
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		_ = resp.Body.Close()
	}

	// Poll until loser cleanup goroutines drain or timeout.
	poll.Eventually(t, 2*time.Second, 10*time.Millisecond, func() bool {
		return runtime.NumGoroutine()-before <= 5
	})
}

func TestHedgedTransport_NoHedgeForPost(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		<-time.After(50 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	ht := &HedgedTransport{
		Inner:   srv.Client().Transport,
		Timeout: 10 * time.Millisecond,
	}
	req, _ := http.NewRequest("POST", srv.URL+"/post", nil)
	resp, err := ht.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	_ = resp.Body.Close()
	// POST should never fire a hedge. Poll that calls stays at 1.
	poll.Eventually(t, 100*time.Millisecond, 10*time.Millisecond, func() bool {
		return calls.Load() == 1
	})
}
