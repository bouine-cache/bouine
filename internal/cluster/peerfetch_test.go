package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thylong/bouine/pkg/api"
)

type stubStore struct {
	objects map[api.Key]*api.Object
}

func (s *stubStore) Get(_ context.Context, key api.Key) (*api.Object, error) {
	return s.objects[key], nil
}

func postFetch(t *testing.T, h *PeerFetchHandler, req api.PeerFetchRequest, hop int) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	r, _ := http.NewRequestWithContext(context.Background(), "POST", PeerFetchPath, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
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
