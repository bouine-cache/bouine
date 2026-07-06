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

func (d *dynamicKeySource) remove(k api.Key) {
	d.mu.Lock()
	delete(d.keys, k)
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

// fakeClock is an injectable clock for cooldown tests (AGENTS.md §8: no
// time.Now() in tests). AntiEntropy.now is set to fc.Now so reconcile
// rounds observe a deterministic, advanceable time.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// newCooldownAE builds an AntiEntropy with a fake clock and a single
// in-process peer that serves peerKeys and fetches the corresponding
// objects. The key source is static (mockKeySource) so backfilled keys
// never appear local next round — modelling SIEVE evicting the
// freshly-backfilled key before the next round (#187).
func newCooldownAE(t *testing.T, localKeys, peerKeys []api.Key, cooldown time.Duration, m *Metrics) (*AntiEntropy, *fakeClock, *mockBackfiller, *mockStorer, func()) {
	t.Helper()
	objs := map[api.Key]*api.Object{}
	for _, k := range peerKeys {
		objs[k] = &api.Object{Key: k, Body: []byte("x")}
	}
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
			if obj, ok := objs[req.Key]; ok {
				w.Header().Set(header.ContentType, "application/octet-stream")
				_, _ = w.Write(storage.EncodeObject(obj))
				return
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	peer := api.PeerInfo{Name: "peer-1", AdminAddr: peerSrv.Listener.Addr().String()}
	bf := &mockBackfiller{objs: objs}
	ks := &mockKeySource{keys: localKeys}
	st := &mockStorer{}
	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:         time.Hour, // reconcile called directly
		FetchTimeout:     2 * time.Second,
		BackfillCooldown: cooldown,
	}, "local", ks, bf, st, func() []api.PeerInfo { return []api.PeerInfo{peer} }, m)
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	ae.now = fc.Now
	return ae, fc, bf, st, peerSrv.Close
}

// TestAntiEntropy_CooldownSkipsRefetch covers case (a): a key backfilled
// in round 1 is still in its cooldown window at round 2, so the
// reconciler must skip it — no peer-fetch RPC, no store Put — instead of
// re-backfilling the SIEVE-evicted key every round (#187).
func TestAntiEntropy_CooldownSkipsRefetch(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	ae, _, bf, st, closeFn := newCooldownAE(t, []api.Key{1}, []api.Key{1, 2}, 5*time.Minute, m)
	defer closeFn()

	ae.reconcile(context.Background())
	if got := bf.calls.Load(); got != 1 {
		t.Fatalf("round 1 fetch calls = %d, want 1", got)
	}
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("round 1 puts = %d, want 1", got)
	}

	// Round 2 at the same clock time: key 2 is still in cooldown and
	// absent from the local key set (SIEVE evicted it). It must be
	// skipped, not re-fetched.
	ae.reconcile(context.Background())
	if got := bf.calls.Load(); got != 1 {
		t.Fatalf("round 2 fetch calls = %d, want 1 (cooldown must skip key 2)", got)
	}
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("round 2 puts = %d, want 1 (cooldown must skip key 2)", got)
	}

	// The cooldown-skips counter must reflect the round-2 skip.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var skips float64
	for _, mf := range mfs {
		if mf.GetName() == "bouine_cluster_anti_entropy_cooldown_skips_total" {
			for _, s := range mf.GetMetric() {
				skips += s.GetCounter().GetValue()
			}
		}
	}
	if skips != 1 {
		t.Fatalf("cooldown_skips_total = %v, want 1", skips)
	}
}

// TestAntiEntropy_CooldownExpiryRefetches covers case (b): once the
// cooldown window elapses, the key is no longer skipped and is backfilled
// again.
func TestAntiEntropy_CooldownExpiryRefetches(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	ae, fc, bf, st, closeFn := newCooldownAE(t, []api.Key{1}, []api.Key{1, 2}, 5*time.Minute, m)
	defer closeFn()

	ae.reconcile(context.Background())
	if got := bf.calls.Load(); got != 1 {
		t.Fatalf("round 1 fetch calls = %d, want 1", got)
	}

	// Advance past the cooldown window. The prune step removes the
	// expired entry, and the next round backfills key 2 again.
	fc.Advance(5*time.Minute + time.Second)
	ae.reconcile(context.Background())
	if got := bf.calls.Load(); got != 2 {
		t.Fatalf("round 2 fetch calls = %d, want 2 (cooldown expired, key 2 re-backfilled)", got)
	}
	if got := st.puts.Load(); got != 2 {
		t.Fatalf("round 2 puts = %d, want 2 (cooldown expired, key 2 re-backfilled)", got)
	}
}

// TestAntiEntropy_CooldownDisabledBackcompat covers case (c): with
// BackfillCooldown = 0 the cooldown is disabled and the reconciler
// behaves as before — the SIEVE-evicted key is re-backfilled every round.
func TestAntiEntropy_CooldownDisabledBackcompat(t *testing.T) {
	t.Parallel()
	ae, _, bf, st, closeFn := newCooldownAE(t, []api.Key{1}, []api.Key{1, 2}, 0, nil)
	defer closeFn()

	ae.reconcile(context.Background())
	ae.reconcile(context.Background())
	if got := bf.calls.Load(); got != 2 {
		t.Fatalf("fetch calls = %d, want 2 (cooldown disabled, no skipping)", got)
	}
	if got := st.puts.Load(); got != 2 {
		t.Fatalf("puts = %d, want 2 (cooldown disabled, no skipping)", got)
	}
	if len(ae.cooldown) != 0 {
		t.Fatalf("cooldown map len = %d, want 0 when cooldown disabled", len(ae.cooldown))
	}
}

// TestAntiEntropy_CooldownAcrossPeersInRound covers case (d): a key
// backfilled from peer 1 is recorded in localSet so peer 2 in the same
// round sees it as present (not via cooldown). In the next round, after
// SIEVE evicts it, both peers skip the key via cooldown — the metric
// counts one skip per peer per round, and no double backfill occurs.
func TestAntiEntropy_CooldownAcrossPeersInRound(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
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
	ks := &mockKeySource{keys: []api.Key{1}} // static: key 2 never local
	st := &mockStorer{}
	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:         time.Hour,
		FetchTimeout:     2 * time.Second,
		BackfillCooldown: 5 * time.Minute,
	}, "local", ks, bf, st, func() []api.PeerInfo { return peers }, m)
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	ae.now = fc.Now

	// Round 1: peer-1 backfills key 2 (1 fetch, 1 put) and records it in
	// localSet + cooldown. Peer-2 sees key 2 in localSet — no second
	// backfill, no cooldown skip this round.
	ae.reconcile(context.Background())
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("round 1 puts = %d, want 1 (no duplicate across peers)", got)
	}
	if got := bf.calls.Load(); got != 1 {
		t.Fatalf("round 1 fetch calls = %d, want 1", got)
	}

	// Round 2, same clock: key 2 is still missing locally (SIEVE evicted)
	// and in cooldown. Both peers skip it via cooldown — 2 skips, 0
	// backfills.
	ae.reconcile(context.Background())
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("round 2 puts = %d, want 1 (cooldown skips both peers)", got)
	}
	if got := bf.calls.Load(); got != 1 {
		t.Fatalf("round 2 fetch calls = %d, want 1 (cooldown skips both peers)", got)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var skips float64
	for _, mf := range mfs {
		if mf.GetName() == "bouine_cluster_anti_entropy_cooldown_skips_total" {
			for _, s := range mf.GetMetric() {
				skips += s.GetCounter().GetValue()
			}
		}
	}
	if skips != 2 {
		t.Fatalf("cooldown_skips_total = %v, want 2 (one per peer in round 2)", skips)
	}
}

// TestAntiEntropy_CooldownPruneBoundsMemory covers case (e): expired
// cooldown entries are removed at the top of each round so the map does
// not grow unbounded. Prune is exercised directly so the assertion is not
// confounded by recordBackfill re-adding the key after a re-backfill.
func TestAntiEntropy_CooldownPruneBoundsMemory(t *testing.T) {
	t.Parallel()
	ae, fc, _, _, closeFn := newCooldownAE(t, []api.Key{1}, []api.Key{1, 2}, 5*time.Minute, nil)
	defer closeFn()

	ae.reconcile(context.Background())
	if len(ae.cooldown) != 1 {
		t.Fatalf("after round 1 cooldown len = %d, want 1", len(ae.cooldown))
	}

	// Advance past the expiry; prune must drop the entry.
	fc.Advance(5*time.Minute + time.Second)
	ae.pruneCooldown()
	if len(ae.cooldown) != 0 {
		t.Fatalf("after expiry+prune cooldown len = %d, want 0", len(ae.cooldown))
	}

	// An unexpired entry must survive prune.
	ae.cooldown[api.Key(99)] = fc.Now().Add(time.Minute)
	ae.pruneCooldown()
	if _, ok := ae.cooldown[api.Key(99)]; !ok {
		t.Fatal("prune removed an unexpired entry")
	}
}

// BenchmarkReconcileWithCooldown shows the per-key cooldown check is O(1)
// (a single map lookup) and allocation-free on the diff path. All peer
// keys are absent from localSet and within their cooldown window, so each
// key exercises the cooldown lookup and is skipped — no fetch, no Put.
func BenchmarkReconcileWithCooldown(b *testing.B) {
	const n = 10000
	peerKeys := make([]api.Key, n)
	for i := range peerKeys {
		peerKeys[i] = api.Key(i + 1)
	}
	localSet := make(map[api.Key]struct{}, 0)
	ae := &AntiEntropy{
		cfg: AntiEntropyConfig{BackfillCooldown: 5 * time.Minute},
		now: time.Now,
		cooldown: func() map[api.Key]time.Time {
			c := make(map[api.Key]time.Time, n)
			exp := time.Now().Add(5 * time.Minute)
			for i := range peerKeys {
				c[peerKeys[i]] = exp
			}
			return c
		}(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		missing, skips := ae.missingKeys(peerKeys, localSet)
		if len(missing) != 0 {
			b.Fatalf("missing = %d, want 0 (all cooled down)", len(missing))
		}
		if skips != n {
			b.Fatalf("skips = %d, want %d", skips, n)
		}
	}
}

// mutablePeerKeys wraps a mutex-guarded slice of keys so the peer key-set
// HTTP handler can return a different set each round (simulating a new key
// appearing on the peer between rounds).
type mutablePeerKeys struct {
	mu   sync.Mutex
	keys []api.Key
}

func (m *mutablePeerKeys) set(keys []api.Key) {
	m.mu.Lock()
	m.keys = keys
	m.mu.Unlock()
}

func (m *mutablePeerKeys) get() []api.Key {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]api.Key, len(m.keys))
	copy(out, m.keys)
	return out
}

// newChurnAE builds an AntiEntropy with a fake clock, a dynamic local key
// source (so tests can add/remove keys to simulate SIEVE eviction and
// promotion), and a mutable peer key set (so a new key can appear on the
// peer between rounds). The storer's onPut hook adds backfilled keys to
// the local key source, mirroring a real TieredStore.Keys(). The cooldown
// is fixed at 5m — the churn tests do not advance the clock, so the exact
// duration is irrelevant; what matters is which keys are in the cooldown
// map and which are in the local key set.
func newChurnAE(t *testing.T, localKeys, peerKeys []api.Key, churnThreshold float64, m *Metrics) (*AntiEntropy, *mockBackfiller, *mockStorer, *dynamicKeySource, *mutablePeerKeys, func()) {
	t.Helper()
	objs := map[api.Key]*api.Object{}
	for _, k := range peerKeys {
		objs[k] = &api.Object{Key: k, Body: []byte("x")}
	}
	mpk := &mutablePeerKeys{keys: peerKeys}
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/keys" {
			buf, _ := EncodeKeySet("peer-1", mpk.get())
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(buf)
			return
		}
		if r.URL.Path == "/v1/peer/fetch" {
			var req api.PeerFetchRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if obj, ok := objs[req.Key]; ok {
				w.Header().Set(header.ContentType, "application/octet-stream")
				_, _ = w.Write(storage.EncodeObject(obj))
				return
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	peer := api.PeerInfo{Name: "peer-1", AdminAddr: peerSrv.Listener.Addr().String()}
	bf := &mockBackfiller{objs: objs}
	ks := &dynamicKeySource{keys: make(map[api.Key]struct{}, len(localKeys))}
	for _, k := range localKeys {
		ks.keys[k] = struct{}{}
	}
	st := &mockStorer{}
	st.onPut = func(key api.Key) { ks.add(key) }
	ae := NewAntiEntropy(AntiEntropyConfig{
		Interval:         time.Hour, // reconcile called directly
		FetchTimeout:     2 * time.Second,
		BackfillCooldown: 5 * time.Minute,
		ChurnThreshold:   churnThreshold,
	}, "local", ks, bf, st, func() []api.PeerInfo { return []api.PeerInfo{peer} }, m)
	fc := &fakeClock{now: time.Unix(1_000_000, 0)}
	ae.now = fc.Now
	return ae, bf, st, ks, mpk, peerSrv.Close
}

// gatherChurnSkips reads the churn-skips counter from a registry.
func gatherChurnSkips(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "bouine_cluster_anti_entropy_churn_skips_total" {
			var v float64
			for _, s := range mf.GetMetric() {
				v += s.GetCounter().GetValue()
			}
			return v
		}
	}
	return 0
}

// TestAntiEntropy_ChurnDetectedSkipsRound covers case (a): a key
// backfilled in round 1 is evicted by SIEVE before round 2 (absent from
// the local key set while still in cooldown). With ChurnThreshold = 0.5
// the evicted-to-backfilled ratio is 1.0 > 0.5, so the round is skipped —
// no peer-fetch, no store Put, churn-skips counter incremented.
func TestAntiEntropy_ChurnDetectedSkipsRound(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	// static local keys (key 2 never local — SIEVE evicted it).
	ae, bf, st, ks, _, closeFn := newChurnAE(t, []api.Key{1}, []api.Key{1, 2}, 0.5, m)
	defer closeFn()

	// Round 1: key 2 is missing, gets backfilled. onPut adds it to ks,
	// but we immediately remove it to simulate SIEVE eviction.
	ae.reconcile(context.Background())
	if got := bf.calls.Load(); got != 1 {
		t.Fatalf("round 1 fetch calls = %d, want 1", got)
	}
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("round 1 puts = %d, want 1", got)
	}
	ks.remove(api.Key(2)) // simulate SIEVE evicting the freshly-backfilled key

	// Round 2: key 2 is in cooldown but absent from local keys (evicted).
	// Churn is detected (ratio 1.0 > 0.5) → round skipped.
	ae.reconcile(context.Background())
	if got := bf.calls.Load(); got != 1 {
		t.Fatalf("round 2 fetch calls = %d, want 1 (churn must skip round)", got)
	}
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("round 2 puts = %d, want 1 (churn must skip round)", got)
	}
	if got := gatherChurnSkips(t, reg); got != 1 {
		t.Fatalf("churn_skips_total = %v, want 1", got)
	}
}

// TestAntiEntropy_NoChurnBackfillProceeds covers case (b): when backfilled
// keys survive in the hot tier (present in the local key set), the
// evicted-to-backfilled ratio is 0 and churn is not detected. A new
// missing key is backfilled normally.
func TestAntiEntropy_NoChurnBackfillProceeds(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	ae, bf, st, _, mpk, closeFn := newChurnAE(t, []api.Key{1}, []api.Key{1, 2}, 0.5, m)
	defer closeFn()

	// Round 1: backfill key 2. onPut adds it to the key source (survives).
	ae.reconcile(context.Background())
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("round 1 puts = %d, want 1", got)
	}

	// Round 2: key 2 is in cooldown AND in local keys (survived). Ratio 0
	// → no churn. Add key 3 to the peer and backfiller; it is missing and
	// not in cooldown, so it gets backfilled.
	mpk.set([]api.Key{1, 2, 3})
	bf.objs[3] = &api.Object{Key: 3, Body: []byte("c")}
	ae.reconcile(context.Background())
	if got := st.puts.Load(); got != 2 {
		t.Fatalf("round 2 puts = %d, want 2 (no churn, key 3 backfilled)", got)
	}
	if got := bf.calls.Load(); got != 2 {
		t.Fatalf("round 2 fetch calls = %d, want 2", got)
	}
	if got := gatherChurnSkips(t, reg); got != 0 {
		t.Fatalf("churn_skips_total = %v, want 0 (no churn detected)", got)
	}
}

// TestAntiEntropy_ChurnThresholdZeroDisabled covers case (c): with
// ChurnThreshold = 0 churn detection is disabled (back-compat). Even when
// all backfilled keys are evicted (ratio 1.0), the round is not skipped by
// the churn guard — the cooldown guard handles the per-key skip instead.
func TestAntiEntropy_ChurnThresholdZeroDisabled(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	ae, bf, st, ks, _, closeFn := newChurnAE(t, []api.Key{1}, []api.Key{1, 2}, 0, m)
	defer closeFn()

	ae.reconcile(context.Background())
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("round 1 puts = %d, want 1", got)
	}
	ks.remove(api.Key(2)) // SIEVE evicts key 2

	// Round 2: key 2 is in cooldown, absent from local keys. Churn is
	// disabled (threshold 0), so the churn guard does not skip. The
	// cooldown guard skips key 2 per-key (no re-fetch).
	ae.reconcile(context.Background())
	if got := bf.calls.Load(); got != 1 {
		t.Fatalf("round 2 fetch calls = %d, want 1 (cooldown skips key 2, churn guard disabled)", got)
	}
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("round 2 puts = %d, want 1 (cooldown skips key 2)", got)
	}
	if got := gatherChurnSkips(t, reg); got != 0 {
		t.Fatalf("churn_skips_total = %v, want 0 (churn disabled)", got)
	}
}

// TestAntiEntropy_ChurnStopsBackfillResumes covers case (d): churn is
// detected in round 2 (key evicted while in cooldown), but once the key
// survives (promoted/served, re-added to the local key set), churn is no
// longer detected and backfill resumes for new missing keys.
func TestAntiEntropy_ChurnStopsBackfillResumes(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	ae, bf, st, ks, mpk, closeFn := newChurnAE(t, []api.Key{1}, []api.Key{1, 2}, 0.5, m)
	defer closeFn()

	// Round 1: backfill key 2, then SIEVE evicts it.
	ae.reconcile(context.Background())
	ks.remove(api.Key(2))

	// Round 2: churn detected (ratio 1.0 > 0.5) → round skipped.
	ae.reconcile(context.Background())
	if got := st.puts.Load(); got != 1 {
		t.Fatalf("round 2 puts = %d, want 1 (churn skip)", got)
	}
	if got := gatherChurnSkips(t, reg); got != 1 {
		t.Fatalf("churn_skips_total = %v, want 1 after round 2", got)
	}

	// Simulate key 2 being promoted (served → visited bit set → survives
	// SIEVE). Re-add it to the local key set. Add key 3 to the peer and
	// to the backfiller's object map so it can be fetched.
	ks.add(api.Key(2))
	mpk.set([]api.Key{1, 2, 3})
	bf.objs[3] = &api.Object{Key: 3, Body: []byte("c")}

	// Round 3: key 2 is in cooldown AND in local keys (survived). Ratio 0
	// → no churn. Key 3 is missing and not in cooldown → backfilled.
	ae.reconcile(context.Background())
	if got := st.puts.Load(); got != 2 {
		t.Fatalf("round 3 puts = %d, want 2 (churn stopped, key 3 backfilled)", got)
	}
	if got := bf.calls.Load(); got != 2 {
		t.Fatalf("round 3 fetch calls = %d, want 2 (key 3 fetched)", got)
	}
	if got := gatherChurnSkips(t, reg); got != 1 {
		t.Fatalf("churn_skips_total = %v, want 1 (no new churn skip in round 3)", got)
	}
}
