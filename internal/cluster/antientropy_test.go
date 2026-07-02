package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thylong/bouine/pkg/api"
)

type mockKeySource struct {
	keys []api.Key
}

func (m *mockKeySource) Keys() []api.Key {
	return m.keys
}

type mockBackfiller struct {
	objs  map[api.Key]*api.Object
	calls atomic.Int32
}

func (m *mockBackfiller) Fetch(_ context.Context, _ api.PeerInfo, req api.PeerFetchRequest) (*api.Object, error) {
	m.calls.Add(1)
	return m.objs[req.Key], nil
}

type mockStorer struct {
	puts atomic.Int32
}

func (m *mockStorer) Put(_ context.Context, _ api.Key, _ *api.Object) error {
	m.puts.Add(1)
	return nil
}

func TestPeerKeysHandler_ServesKeySet(t *testing.T) {
	t.Parallel()
	keys := []api.Key{1, 2, 3}
	h := NewPeerKeysHandler(&mockKeySource{keys: keys}, "test-node")
	req := httptest.NewRequest(http.MethodGet, "/v1/peer/keys", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	name, decoded, err := DecodeKeySet(rec.Body.Bytes())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if name != "test-node" {
		t.Errorf("node_name = %q, want test-node", name)
	}
	if len(decoded) != 3 {
		t.Fatalf("keys = %d, want 3", len(decoded))
	}
}

func TestPeerKeysHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	h := NewPeerKeysHandler(&mockKeySource{}, "test")
	req := httptest.NewRequest(http.MethodPost, "/v1/peer/keys", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestAntiEntropy_ReconcileBackfillsAndStores(t *testing.T) {
	t.Parallel()
	localKeys := []api.Key{1, 2, 3}
	peerKeys := []api.Key{1, 2, 3, 4, 5}
	obj4 := &api.Object{Key: 4, Body: []byte("d")}
	obj5 := &api.Object{Key: 5, Body: []byte("e")}

	peer := api.PeerInfo{Name: "peer-1", AdminAddr: ""}

	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/keys" {
			buf, _ := EncodeKeySet("peer-1", peerKeys)
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(buf)
			return
		}
		if r.URL.Path == "/v1/peer/fetch" {
			var req api.PeerFetchRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if obj, ok := map[api.Key]*api.Object{4: obj4, 5: obj5}[req.Key]; ok {
				_ = json.NewEncoder(w).Encode(api.PeerFetchResponse{Hit: true, Object: obj})
				return
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer peerSrv.Close()

	peer.AdminAddr = peerSrv.Listener.Addr().String()

	bf := &mockBackfiller{objs: map[api.Key]*api.Object{4: obj4, 5: obj5}}
	ks := &mockKeySource{keys: localKeys}
	st := &mockStorer{}

	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:      50 * time.Millisecond,
		FetchTimeout:  2 * time.Second,
		BackfillLimit: 0,
	}, "local", ks, bf, st, func() []api.PeerInfo { return []api.PeerInfo{peer} }, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ae.Start(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bf.calls.Load() >= 2 && st.puts.Load() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	if got := bf.calls.Load(); got < 2 {
		t.Fatalf("backfill fetch calls = %d, want >= 2", got)
	}
	if got := st.puts.Load(); got < 2 {
		t.Fatalf("store puts = %d, want >= 2 (objects must be stored, not just fetched)", got)
	}
}

func TestAntiEntropy_NoMissingKeysNoBackfill(t *testing.T) {
	t.Parallel()
	localKeys := []api.Key{1, 2, 3}
	peerKeys := []api.Key{1, 2, 3}

	peer := api.PeerInfo{Name: "peer-1"}
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		buf, _ := EncodeKeySet("peer-1", peerKeys)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(buf)
	}))
	defer peerSrv.Close()
	peer.AdminAddr = peerSrv.Listener.Addr().String()

	bf := &mockBackfiller{objs: map[api.Key]*api.Object{}}
	ks := &mockKeySource{keys: localKeys}
	st := &mockStorer{}

	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:     50 * time.Millisecond,
		FetchTimeout: 2 * time.Second,
	}, "local", ks, bf, st, func() []api.PeerInfo { return []api.PeerInfo{peer} }, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ae.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	if got := bf.calls.Load(); got != 0 {
		t.Fatalf("backfill calls = %d, want 0", got)
	}
	if got := st.puts.Load(); got != 0 {
		t.Fatalf("store puts = %d, want 0", got)
	}
}

func TestAntiEntropy_SkipsSelf(t *testing.T) {
	t.Parallel()
	ks := &mockKeySource{keys: []api.Key{1}}
	bf := &mockBackfiller{}
	st := &mockStorer{}

	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:     50 * time.Millisecond,
		FetchTimeout: 2 * time.Second,
	}, "local", ks, bf, st, func() []api.PeerInfo { return []api.PeerInfo{{Name: "local"}} }, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ae.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	if got := bf.calls.Load(); got != 0 {
		t.Fatalf("backfill calls = %d, want 0 (self should be skipped)", got)
	}
}

func TestAntiEntropy_ReconcileCounterFiresOnZeroDrift(t *testing.T) {
	t.Parallel()
	localKeys := []api.Key{1, 2, 3}
	peerKeys := []api.Key{1, 2, 3}

	peer := api.PeerInfo{Name: "peer-1"}
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		buf, _ := EncodeKeySet("peer-1", peerKeys)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(buf)
	}))
	defer peerSrv.Close()
	peer.AdminAddr = peerSrv.Listener.Addr().String()

	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:     50 * time.Millisecond,
		FetchTimeout: 2 * time.Second,
	}, "local", &mockKeySource{keys: localKeys}, &mockBackfiller{}, &mockStorer{},
		func() []api.PeerInfo { return []api.PeerInfo{peer} }, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ae.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var reconcileCount, keysRepaired float64
	for _, mf := range mfs {
		switch mf.GetName() {
		case "bouine_cluster_anti_entropy_reconcile_total":
			for _, s := range mf.GetMetric() {
				reconcileCount += s.GetCounter().GetValue()
			}
		case "bouine_cluster_anti_entropy_keys_repaired":
			for _, s := range mf.GetMetric() {
				keysRepaired += s.GetGauge().GetValue()
			}
		}
	}
	if reconcileCount == 0 {
		t.Fatal("reconcile_total = 0, want > 0 (counter must fire on zero-drift rounds)")
	}
	if keysRepaired != 0 {
		t.Errorf("keys_repaired = %v, want 0", keysRepaired)
	}
}

func TestAntiEntropy_FetchFailureCounter(t *testing.T) {
	t.Parallel()
	peer := api.PeerInfo{Name: "peer-1", AdminAddr: "127.0.0.1:1"}

	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:     50 * time.Millisecond,
		FetchTimeout: 100 * time.Millisecond,
	}, "local", &mockKeySource{keys: []api.Key{1}}, &mockBackfiller{}, &mockStorer{},
		func() []api.PeerInfo { return []api.PeerInfo{peer} }, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ae.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var failures float64
	for _, mf := range mfs {
		if mf.GetName() == "bouine_cluster_anti_entropy_fetch_failures_total" {
			for _, s := range mf.GetMetric() {
				failures += s.GetCounter().GetValue()
			}
		}
	}
	if failures == 0 {
		t.Fatal("fetch_failures_total = 0, want > 0 (unreachable peer must increment failure counter)")
	}
}

func TestKeysToUint64(t *testing.T) {
	t.Parallel()
	keys := []api.Key{1, 2, 3}
	out := keysToUint64(keys)
	if len(out) != 3 {
		t.Fatalf("len = %d, want 3", len(out))
	}
	for i, k := range keys {
		if out[i] != uint64(k) {
			t.Errorf("out[%d] = %d, want %d", i, out[i], uint64(k))
		}
	}
}

func TestAntiEntropy_StreamingDecodeLargeKeySet(t *testing.T) {
	t.Parallel()
	largeKeySet := make([]api.Key, 10000)
	for i := range largeKeySet {
		largeKeySet[i] = api.Key(i + 1)
	}

	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		buf, _ := EncodeKeySet("peer-1", largeKeySet)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(buf)
	}))
	defer peerSrv.Close()

	peer := api.PeerInfo{Name: "peer-1", AdminAddr: peerSrv.Listener.Addr().String()}
	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:     time.Hour,
		FetchTimeout: 2 * time.Second,
	}, "local", &mockKeySource{keys: []api.Key{}}, &mockBackfiller{}, &mockStorer{},
		func() []api.PeerInfo { return []api.PeerInfo{peer} }, nil)

	keys, ok := ae.fetchPeerKeys(context.Background(), peer)
	if !ok {
		t.Fatal("expected successful key set fetch")
	}
	if len(keys) != len(largeKeySet) {
		t.Fatalf("keys = %d, want %d", len(keys), len(largeKeySet))
	}
}
