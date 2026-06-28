package origin

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
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
			time.Sleep(200 * time.Millisecond)
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
	time.Sleep(50 * time.Millisecond)
	if calls.Load() < 2 {
		t.Fatalf("calls = %d, want >= 2", calls.Load())
	}
}

func TestHedgedTransport_NoGoroutineLeak(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
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
	deadline := time.Now().Add(2 * time.Second)
	var after int
	for {
		after = runtime.NumGoroutine()
		if after-before <= 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: before=%d after=%d delta=%d", before, after, after-before)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHedgedTransport_NoHedgeForPost(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
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
	// POST should never fire a hedge.
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1 (no hedge for POST)", calls.Load())
	}
}
