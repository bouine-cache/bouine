package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
)

// Compile-time assertions that the concrete stores satisfy cluster.Storer.
// If OverBudget is removed from either store, this fails at build time
// instead of silently disabling anti-entropy at runtime.
var (
	_ Storer = (*storage.TieredStore)(nil)
	_ Storer = (*storage.HotStore)(nil)
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
	puts       atomic.Int32
	overBudget atomic.Bool
	backfilled sync.Map // map[api.Key]struct{}
	onPut      func(api.Key)
}

func (m *mockStorer) Put(_ context.Context, key api.Key, _ *api.Object) error {
	m.puts.Add(1)
	m.backfilled.Store(key, struct{}{})
	if m.onPut != nil {
		m.onPut(key)
	}
	return nil
}

func (m *mockStorer) OverBudget() bool {
	return m.overBudget.Load()
}

// dynamicKeySource models a real store's key set: it reflects both the
// initial local keys and any keys added via Put (via add), so the next
// anti-entropy round sees backfilled keys as present.
type dynamicKeySource struct {
	mu   sync.Mutex
	keys map[api.Key]struct{}
}

func (d *dynamicKeySource) Keys() []api.Key {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]api.Key, 0, len(d.keys))
	for k := range d.keys {
		out = append(out, k)
	}
	return out
}

func (d *dynamicKeySource) add(k api.Key) {
	d.mu.Lock()
	d.keys[k] = struct{}{}
	d.mu.Unlock()
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
				w.Header().Set(header.ContentType, "application/octet-stream")
				_, _ = w.Write(storage.EncodeObject(obj))
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

// TestAntiEntropy_SkipsBackfillWhenOverBudget reproduces issue #175:
// when the local store is over its memory budget, anti-entropy must
// skip backfill instead of fighting the eviction policy. Without this
// guard, backfilled keys re-overfill the hot tier and SIEVE evicts them
// again, creating a self-sustaining feedback loop for small hot-only
// keys that never reach the warm tier.
func TestAntiEntropy_SkipsBackfillWhenOverBudget(t *testing.T) {
	t.Parallel()
	localKeys := []api.Key{1}
	peerKeys := []api.Key{1, 2, 3}
	obj2 := &api.Object{Key: 2, Body: []byte("b")}
	obj3 := &api.Object{Key: 3, Body: []byte("c")}

	peer := api.PeerInfo{Name: "peer-1"}
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
			if obj, ok := map[api.Key]*api.Object{2: obj2, 3: obj3}[req.Key]; ok {
				w.Header().Set(header.ContentType, "application/octet-stream")
				_, _ = w.Write(storage.EncodeObject(obj))
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer peerSrv.Close()
	peer.AdminAddr = peerSrv.Listener.Addr().String()

	bf := &mockBackfiller{objs: map[api.Key]*api.Object{2: obj2, 3: obj3}}
	ks := &mockKeySource{keys: localKeys}
	st := &mockStorer{}
	st.overBudget.Store(true) // store is over budget

	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:      time.Hour, // we call reconcile directly
		FetchTimeout:  2 * time.Second,
		BackfillLimit: 0,
	}, "local", ks, bf, st, func() []api.PeerInfo { return []api.PeerInfo{peer} }, nil)

	ae.reconcile(context.Background())

	if got := bf.calls.Load(); got != 0 {
		t.Fatalf("backfill fetch calls = %d, want 0 (over-budget store must skip backfill)", got)
	}
	if got := st.puts.Load(); got != 0 {
		t.Fatalf("store puts = %d, want 0 (over-budget store must skip backfill)", got)
	}
}

// TestAntiEntropy_NoDuplicateBackfillAcrossPeers reproduces issue #175
// secondary amplification: reconcile() built localSet once then looped
// over all peers, so keys backfilled from peer 1 were not reflected when
// reconciling with peer 2 — the same keys got backfilled N times per
// round. The fix records backfilled keys in localSet so subsequent peers
// in the same round see them as present.
func TestAntiEntropy_NoDuplicateBackfillAcrossPeers(t *testing.T) {
	t.Parallel()
	// dynamicKeySource models the real store: Keys() reflects both the
	// initial local keys and any keys backfilled via Put, so the next
	// round's localSet includes them (as a real TieredStore.Keys() would).
	ks := &dynamicKeySource{keys: map[api.Key]struct{}{1: {}}}
	obj2 := &api.Object{Key: 2, Body: []byte("b")}

	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/keys" {
			buf, _ := EncodeKeySet("peer", []api.Key{1, 2})
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(buf)
			return
		}
		if r.URL.Path == "/v1/peer/fetch" {
			w.Header().Set(header.ContentType, "application/octet-stream")
			_, _ = w.Write(storage.EncodeObject(obj2))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer peerSrv.Close()
	addr := peerSrv.Listener.Addr().String()

	peers := []api.PeerInfo{
		{Name: "peer-1", AdminAddr: addr},
		{Name: "peer-2", AdminAddr: addr},
	}

	bf := &mockBackfiller{objs: map[api.Key]*api.Object{2: obj2}}
	st := &mockStorer{}
	// Wire the storer to record backfills into the key source so the
	// next round sees them as present (mirrors TieredStore.Keys()).
	st.onPut = func(key api.Key) { ks.add(key) }

	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:      time.Hour, // we call reconcile directly
		FetchTimeout:  2 * time.Second,
		BackfillLimit: 0,
	}, "local", ks, bf, st, func() []api.PeerInfo { return peers }, nil)

	ae.reconcile(context.Background())

	// Key 2 should be backfilled exactly once (by peer-1). Peer-2 must
	// see it as present in localSet and skip. With the key source
	// reflecting backfills, a second reconcile() call must also skip
	// (key 2 is now owned).
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("store puts = %d, want 1 (key 2 backfilled once by peer-1, not duplicated by peer-2)", got)
	}

	// Second round: key 2 is now in the key source, so neither peer
	// should backfill it again.
	ae.reconcile(context.Background())
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("store puts after 2nd round = %d, want 1 (key 2 already owned, must not re-backfill)", got)
	}
}

// TestAntiEntropy_MidRoundOverBudgetGuard tests the per-peer OverBudget
// check inside reconcileWithPeer: the store starts under budget, peer 1's
// backfill pushes it over budget (via onPut), and peer 2's backfill must
// be skipped by the mid-round guard even though the top-of-reconcile guard
// did not fire.
func TestAntiEntropy_MidRoundOverBudgetGuard(t *testing.T) {
	t.Parallel()

	ks := &dynamicKeySource{keys: map[api.Key]struct{}{1: {}}}
	obj2 := &api.Object{Key: 2, Body: []byte("b")}
	obj3 := &api.Object{Key: 3, Body: []byte("c")}

	// peerSrv1 returns keys {1, 2}; peerSrv2 returns {1, 2, 3}.
	peerSrv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/keys" {
			buf, _ := EncodeKeySet("peer-1", []api.Key{1, 2})
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(buf)
			return
		}
		if r.URL.Path == "/v1/peer/fetch" {
			w.Header().Set(header.ContentType, "application/octet-stream")
			_, _ = w.Write(storage.EncodeObject(obj2))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer peerSrv1.Close()

	peerSrv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/keys" {
			buf, _ := EncodeKeySet("peer-2", []api.Key{1, 2, 3})
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(buf)
			return
		}
		if r.URL.Path == "/v1/peer/fetch" {
			w.Header().Set(header.ContentType, "application/octet-stream")
			_, _ = w.Write(storage.EncodeObject(obj3))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer peerSrv2.Close()

	peers := []api.PeerInfo{
		{Name: "peer-1", AdminAddr: peerSrv1.Listener.Addr().String()},
		{Name: "peer-2", AdminAddr: peerSrv2.Listener.Addr().String()},
	}

	bf := &mockBackfiller{objs: map[api.Key]*api.Object{2: obj2, 3: obj3}}
	st := &mockStorer{}
	// Start under budget. Flip to over-budget when the first key is Put.
	st.onPut = func(key api.Key) {
		ks.add(key)
		st.overBudget.Store(true)
	}

	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:      time.Hour,
		FetchTimeout:  2 * time.Second,
		BackfillLimit: 0,
	}, "local", ks, bf, st, func() []api.PeerInfo { return peers }, nil)

	ae.reconcile(context.Background())

	// Peer 1 backfilled key 2 (1 put). The onPut hook flipped overBudget
	// to true. Peer 2 has key 3 missing, but the mid-round guard must
	// skip its backfill.
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("store puts = %d, want 1 (peer-1 backfill only; peer-2 skipped by mid-round guard)", got)
	}
	if got := bf.calls.Load(); got != 1 {
		t.Fatalf("backfill fetch calls = %d, want 1 (peer-2 fetch skipped by mid-round guard)", got)
	}
}
