package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
)

type stubStore struct {
	objects map[api.Key]*api.Object
}

func (s *stubStore) Get(_ context.Context, key api.Key) (*api.Object, api.Source, error) {
	return s.objects[key], "", nil
}

func postFetch(t *testing.T, h *PeerFetchHandler, req api.PeerFetchRequest, hop int) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	r, _ := http.NewRequestWithContext(context.Background(), "POST", PeerFetchPath, bytes.NewReader(body))
	r.Header.Set(header.ContentType, "application/json")
	if hop > 0 {
		r.Header.Set(BouineHopHeader, fmt.Sprintf("%d", hop))
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	return rr
}

func TestPeerFetchHandler_Hit(t *testing.T) {
	t.Parallel()
	key := api.Key(42)
	store := &stubStore{objects: map[api.Key]*api.Object{
		key: {Key: key, StatusCode: 200, Body: []byte("cached")},
	}}
	h := NewPeerFetchHandler(store)
	rr := postFetch(t, h, api.PeerFetchRequest{Key: key}, 0)
	if rr.Code != 200 {
		t.Fatalf("hit: status=%d", rr.Code)
	}
	var resp api.PeerFetchResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.Hit || resp.Object == nil {
		t.Fatal("expected hit with object")
	}
}

func TestPeerFetchHandler_Miss(t *testing.T) {
	t.Parallel()
	h := NewPeerFetchHandler(&stubStore{objects: map[api.Key]*api.Object{}})
	rr := postFetch(t, h, api.PeerFetchRequest{Key: 999}, 0)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("miss: status=%d", rr.Code)
	}
}

func TestPeerFetchHandler_HopLimit(t *testing.T) {
	t.Parallel()
	h := NewPeerFetchHandler(&stubStore{})
	rr := postFetch(t, h, api.PeerFetchRequest{Key: 1}, MaxHops)
	if rr.Code != http.StatusLoopDetected {
		t.Fatalf("expected 508, got %d", rr.Code)
	}
}

func TestPeerFetchHandler_WrongMethod(t *testing.T) {
	t.Parallel()
	h := NewPeerFetchHandler(&stubStore{})
	rr := httptest.NewRecorder()
	r, _ := http.NewRequestWithContext(context.Background(), "GET", PeerFetchPath, nil)
	h.ServeHTTP(rr, r)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

func TestPeerFetcher_RecordsRoundTripLatency(t *testing.T) {
	t.Parallel()
	const delay = 5 * time.Millisecond // > 1ms so it survives Milliseconds() truncation
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(delay)
		w.Header().Set(header.ContentType, "application/json")
		_ = json.NewEncoder(w).Encode(api.PeerFetchResponse{
			Hit:    true,
			Object: &api.Object{Key: 1, StatusCode: 200, Body: []byte("cached")},
		})
	}))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil)
	obj, err := f.Fetch(context.Background(),
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: 1})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if obj == nil {
		t.Fatal("expected hit object")
	}

	hits, _, _, latN, latSumMs := f.PeerFetchStats()
	if hits != 1 || latN != 1 {
		t.Fatalf("hits=%d latN=%d, want 1,1", hits, latN)
	}
	if latSumMs <= 0 {
		t.Fatalf("latSumMs=%d, want >0 (latency must be measured around the RPC)", latSumMs)
	}
}

func TestPeerFetcher_HopLimitReached(t *testing.T) {
	t.Parallel()
	f := NewPeerFetcher(nil, nil)
	obj, err := f.Fetch(context.Background(), api.PeerInfo{Addr: "unused:0"},
		api.PeerFetchRequest{Key: 1, Hops: MaxHops})
	if err != nil {
		t.Fatalf("hop limit should return nil,nil: %v", err)
	}
	if obj != nil {
		t.Fatal("hop limit should return nil object")
	}
}

func TestPeerFetcher_OversizedResponseReturnsError(t *testing.T) {
	t.Parallel()
	validResp, err := json.Marshal(api.PeerFetchResponse{
		Hit:    true,
		Object: &api.Object{Key: 1, StatusCode: 200, Body: bytes.Repeat([]byte("A"), 4096)},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "application/json")
		_, _ = w.Write(validResp)
	}))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil)
	f.maxBodyBytes = int64(len(validResp) - 1)

	obj, err := f.Fetch(context.Background(),
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: 1})
	if err == nil {
		t.Fatal("expected error from oversized peer response, got nil")
	}
	if obj != nil {
		t.Fatalf("expected nil object on decode error, got %+v", obj)
	}
}

func TestPeerFetcher_ConcurrencySemaphoreBoundsFetches(t *testing.T) {
	t.Parallel()
	var inFlight, maxInFlight atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.PeerFetchResponse{
			Hit:    true,
			Object: &api.Object{Key: 1, StatusCode: 200, Body: []byte("x")},
		})
	}))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil)

	var wg sync.WaitGroup
	for i := 0; i < defaultPeerFetchConcurrency*3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = f.Fetch(context.Background(),
				api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
				api.PeerFetchRequest{Key: 1})
		}()
	}
	wg.Wait()

	if got := maxInFlight.Load(); got > int32(defaultPeerFetchConcurrency) {
		t.Fatalf("max concurrent peer-fetches = %d, want <= %d", got, defaultPeerFetchConcurrency)
	}
}

func TestPeerFetcher_ContextCancelWhileWaitingForSemaphore(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-block
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.PeerFetchResponse{Hit: true, Object: &api.Object{Key: 1}})
	}))
	defer srv.Close()
	defer close(block)

	f := NewPeerFetcher(nil, nil)

	var wg sync.WaitGroup
	for i := 0; i < defaultPeerFetchConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = f.Fetch(context.Background(),
				api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
				api.PeerFetchRequest{Key: 1})
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := f.Fetch(ctx,
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: 2})
	if err == nil {
		t.Fatal("expected error when context cancelled while waiting for semaphore")
	}

	wg.Wait()
}
