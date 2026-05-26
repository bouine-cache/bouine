package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
)

// mockPeerServer creates a test server that returns a MetricsSummary as JSON.
func mockPeerServer(t *testing.T, sum observability.MetricsSummary) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sum)
	}))
}

func TestAggregator_CollectSingleNode(t *testing.T) {
	rings := observability.NewRings("self")
	rings.Request.RecordRequest(true, false, false, 200, 5)
	rings.Request.Flush(time.Now())

	agg := NewAggregator(rings, nil, "", "", nil)
	merged, peers := agg.Collect(t.Context())

	// Single-node: peerResults contains only self.
	if len(peers) != 1 {
		t.Errorf("expected 1 peer result (self), got %d", len(peers))
	}
	var totalReq int64
	for _, b := range merged.RequestSnap {
		totalReq += b.Requests
	}
	if totalReq != 1 {
		t.Errorf("expected 1 total request in merged, got %d", totalReq)
	}
}

func TestAggregator_CollectWithPeer(t *testing.T) {
	peerSnap := make([]observability.RequestBucket, 2160)
	peerSnap[0] = observability.RequestBucket{Requests: 50, Hits: 40}
	peerSum := observability.MetricsSummary{
		NodeName:    "peer-1",
		CollectedAt: time.Now(),
		RequestSnap: peerSnap,
		RouteStats:  []observability.RouteStat{{Route: "/remote", Requests: 50, Hits: 40, HitPct: 80}},
	}

	srv := mockPeerServer(t, peerSum)
	defer srv.Close()

	peerAddr := srv.Listener.Addr().String()

	rings := observability.NewRings("self")
	rings.Request.RecordRequest(true, false, false, 200, 2)
	rings.Request.Flush(time.Now())

	peersFn := func() []api.PeerInfo {
		return []api.PeerInfo{{Name: "peer-1", AdminAddr: peerAddr}}
	}
	// selfAddr is different from peer so peer isn't self-excluded.
	agg := NewAggregator(rings, peersFn, "127.0.0.1:9999", "", nil)
	merged, peers := agg.Collect(t.Context())

	// Expect self + one remote peer.
	if len(peers) != 2 {
		t.Errorf("expected 2 peer results, got %d", len(peers))
	}

	// Count live peers.
	livePeers := 0
	for _, p := range peers {
		if !p.Stale {
			livePeers++
		}
	}
	if livePeers < 1 {
		t.Error("expected at least one live peer")
	}

	// Total requests across all buckets: self(1) + peer(50) = 51.
	var totalReq int64
	for _, b := range merged.RequestSnap {
		totalReq += b.Requests
	}
	if totalReq != 51 {
		t.Errorf("merged total requests: want 51, got %d", totalReq)
	}
}

func TestAggregator_PeerTimeout(t *testing.T) {
	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer slowSrv.Close()

	rings := observability.NewRings("self")
	peersFn := func() []api.PeerInfo {
		return []api.PeerInfo{{Name: "slow", AdminAddr: slowSrv.Listener.Addr().String()}}
	}
	agg := NewAggregator(rings, peersFn, "self:9999", "", nil)

	start := time.Now()
	_, peers := agg.Collect(t.Context())
	elapsed := time.Since(start)

	if elapsed >= 500*time.Millisecond {
		t.Errorf("Collect took too long: %v (expected < 500ms)", elapsed)
	}
	// Slow peer should be marked stale.
	for _, p := range peers {
		if p.NodeName == "slow" && !p.Stale {
			t.Error("timed-out peer should be marked stale")
		}
	}
}

func TestPeerMetricsHandler(t *testing.T) {
	rings := observability.NewRings("node-x")
	rings.Request.RecordRequest(true, false, false, 200, 7)
	rings.Request.Flush(time.Now())

	h := PeerMetricsHandler(rings)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/peer/metrics", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var sum observability.MetricsSummary
	if err := json.NewDecoder(w.Body).Decode(&sum); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if sum.NodeName != "node-x" {
		t.Errorf("NodeName: want node-x, got %q", sum.NodeName)
	}
	var total int64
	for _, b := range sum.RequestSnap {
		total += b.Requests
	}
	if total != 1 {
		t.Errorf("total requests: want 1, got %d", total)
	}
}
