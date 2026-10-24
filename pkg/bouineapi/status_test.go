package bouineapi

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
)

func TestClient_Status(t *testing.T) {
	t.Parallel()
	var gotCluster string
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		gotCluster = string(ctx.QueryArgs().Peek("cluster"))
		writeJSON(ctx, StatusReport{
			Node: StatusNode{Name: "bouine-0", Version: "0.6.0", Ready: true, UptimeSec: 42},
			Cache: StatusCache{
				Hits: 80, Misses: 20, HitRatio: 0.8, Evictions: 3,
				Hot:  StatusTier{Entries: 10, Bytes: 1024, MaxBytes: 4096},
				Warm: StatusTier{Entries: 5, Bytes: 2048, DiskBytes: 4096},
			},
			Origins: []StatusPool{{Name: "api", Targets: []StatusTarget{{
				Addr: "10.0.0.1:80", Healthy: true, AdminOverride: "none",
				ProbeSuccesses: 2, LastChange: "2026-10-23T09:00:00Z",
			}}}},
			Cluster: StatusCluster{
				Peers:     []StatusPeer{{Name: "bouine-0", Requests: 100, Hits: 80, Misses: 20}},
				PeerFetch: StatusPeerFetch{Hits: 7, Misses: 3, HopLimitHits: 1, AvgLatencyMs: 2.5},
			},
			Insights: []StatusInsight{{ID: "upstream-no-hedge", Severity: "low", Title: "t", Action: "a"}},
		})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.Status(context.Background(), false)
	require.NoError(t, err, "Status")
	assert.Equal(t, "", gotCluster, "no cluster param by default")
	require.Equal(t, "bouine-0", got.Node.Name)
	require.True(t, got.Node.Ready)
	assert.Equal(t, int64(42), got.Node.UptimeSec)
	assert.InDelta(t, 0.8, got.Cache.HitRatio, 0.0001)
	assert.Equal(t, int64(4096), got.Cache.Hot.MaxBytes)
	require.Len(t, got.Origins, 1)
	assert.Equal(t, "10.0.0.1:80", got.Origins[0].Targets[0].Addr)
	assert.Equal(t, "2026-10-23T09:00:00Z", got.Origins[0].Targets[0].LastChange)
	assert.Len(t, got.Cluster.Peers, 1)
	assert.Equal(t, int64(100), got.Cluster.Peers[0].Requests)
	assert.Equal(t, int64(1), got.Cluster.PeerFetch.HopLimitHits)
	assert.Len(t, got.Insights, 1)
}

func TestClient_StatusCluster(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		if string(ctx.QueryArgs().Peek("cluster")) != "1" {
			ctx.Error("missing cluster=1", fasthttp.StatusBadRequest)
			return
		}
		writeJSON(ctx, StatusReport{Node: StatusNode{Name: "bouine-0"}})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.Status(context.Background(), true)
	require.NoError(t, err, "Status cluster")
	assert.Equal(t, "bouine-0", got.Node.Name)
}
