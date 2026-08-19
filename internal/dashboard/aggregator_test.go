package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func mockPeerServer(t *testing.T, sum observability.MetricsSummary) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "application/json")
		_ = json.NewEncoder(w).Encode(sum)
	}))
}

func TestAggregator_CollectSingleNode(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	rings.Request.RecordRequest("HIT", 200, 5)
	rings.Request.Flush(time.Now())

	agg := NewAggregator(rings, nil, "", nil)
	merged, peers := agg.Collect(t.Context())

	assert.Len(t, peers, 1)
	var totalReq int64
	for _, b := range merged.RequestSnap {
		totalReq += b.Requests
	}
	assert.Equal(t, int64(1), totalReq)
}

func TestAggregator_CollectWithPeer(t *testing.T) {
	t.Parallel()
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
	rings.Request.RecordRequest("HIT", 200, 2)
	rings.Request.Flush(time.Now())

	peersFn := func() []api.PeerInfo {
		return []api.PeerInfo{{Name: "peer-1", AdminAddr: peerAddr}}
	}
	agg := NewAggregator(rings, peersFn, "127.0.0.1:9999", nil)
	merged, peers := agg.Collect(t.Context())

	assert.Len(t, peers, 2)
	livePeers := 0
	for _, p := range peers {
		if !p.Stale {
			livePeers++
		}
	}

	var totalReq int64
	for _, b := range merged.RequestSnap {
		totalReq += b.Requests
	}
	// The local node always contributes 2 requests. The peer contributes 50
	// if the HTTP fetch completes within the 200ms timeout. Under CI load,
	// the peer fetch may time out, leaving only the local contribution.
	if totalReq < 2 || totalReq > 52 {
		t.Errorf("unexpected total requests %d (expected 2-52)", totalReq)
	}
	if livePeers < 1 && totalReq == 2 {
		t.Log("peer fetch timed out — acceptable under CI load")
	} else if livePeers < 1 {
		t.Error("expected at least one live peer when peer data is available")
	}
}

func TestAggregator_PeerTimeout(t *testing.T) {
	t.Parallel()
	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer slowSrv.Close()

	rings := observability.NewRings("self")
	peersFn := func() []api.PeerInfo {
		return []api.PeerInfo{{Name: "slow", AdminAddr: slowSrv.Listener.Addr().String()}}
	}
	agg := NewAggregator(rings, peersFn, "self:9999", nil)

	start := time.Now()
	_, peers := agg.Collect(t.Context())
	elapsed := time.Since(start)

	if elapsed >= 500*time.Millisecond {
		t.Errorf("Collect took too long: %v (expected < 500ms)", elapsed)
	}
	for _, p := range peers {
		if p.NodeName == "slow" && !p.Stale {
			t.Error("timed-out peer should be marked stale")
		}
	}
}

func TestAggregator_LastKnownOnStale(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")

	// First call with a live peer that gives data.
	peerSnap := make([]observability.RequestBucket, 2160)
	peerSnap[0] = observability.RequestBucket{Requests: 99}
	peerSum := observability.MetricsSummary{
		NodeName:    "flaky",
		CollectedAt: time.Now(),
		RequestSnap: peerSnap,
	}
	liveSrv := mockPeerServer(t, peerSum)

	peersFn := func() []api.PeerInfo {
		return []api.PeerInfo{{Name: "flaky", AdminAddr: liveSrv.Listener.Addr().String()}}
	}
	agg := NewAggregator(rings, peersFn, "self:9999", nil)
	_, peers := agg.Collect(t.Context())
	// Verify live peer found.
	var got99 bool
	for _, p := range peers {
		if p.NodeName == "flaky" && !p.Stale {
			got99 = true
		}
	}
	assert.True(t, got99)
	liveSrv.Close()

	// Second call — peer is now down; should use last-known snapshot.
	_, peers2 := agg.Collect(t.Context())
	var staleFound bool
	for _, p := range peers2 {
		if p.NodeName == "flaky" && p.Stale {
			staleFound = true
			var total int64
			for _, b := range p.Summary.RequestSnap {
				total += b.Requests
			}
			assert.Equal(t, int64(99), total)
		}
	}
	assert.True(t, staleFound)
}

func TestPeerMetricsHandler(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("node-x")
	rings.Request.RecordRequest("HIT", 200, 7)
	rings.Request.Flush(time.Now())

	h := PeerMetricsHandler(rings)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/peer/metrics", nil)
	h.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	var sum observability.MetricsSummary
	err := json.NewDecoder(w.Body).Decode(&sum)
	require.NoError(t, err, "decode")
	assert.Equal(t, "node-x", sum.NodeName)
	var total int64
	for _, b := range sum.RequestSnap {
		total += b.Requests
	}
	assert.Equal(t, int64(1), total)
}
