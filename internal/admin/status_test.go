package admin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func statusServer(t *testing.T, fn func(ctx context.Context, cluster bool) StatusReport) *Server {
	t.Helper()
	return New(Config{
		Token:    "secret",
		StatusFn: fn,
	})
}

func authedGet(t *testing.T, s *Server, path string) (int, []byte) {
	t.Helper()
	ctx := testCtx("GET", path)
	ctx.Request.Header.Set("Authorization", "Bearer secret")
	s.Handler()(ctx)
	return ctx.Response.StatusCode(), ctx.Response.Body()
}

func TestStatusEndpoint_ServesReport(t *testing.T) {
	t.Parallel()
	s := statusServer(t, func(_ context.Context, _ bool) StatusReport {
		return StatusReport{
			Node:    StatusNode{Name: "bouine-0", Ready: true},
			Cache:   StatusCache{Hits: 8, Misses: 2, HitRatio: 0.8},
			Origins: []StatusPool{{Name: "api", Targets: []StatusTarget{{Addr: "10.0.0.1:80", Healthy: true, AdminOverride: "none"}}}},
			Cluster: StatusCluster{Peers: []StatusPeer{{Name: "bouine-0", Requests: 10, Hits: 8}}},
			Insights: []StatusInsight{{ID: "cache-low-hit-rate", Severity: "medium",
				Title: "Hit rate is low", Action: "Check TTLs"}},
		}
	})
	status, body := authedGet(t, s, "/v1/status")
	require.Equal(t, fasthttp.StatusOK, status, "body: %s", body)

	var got StatusReport
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "bouine-0", got.Node.Name)
	require.True(t, got.Node.Ready)
	require.InDelta(t, 0.8, got.Cache.HitRatio, 0.0001)
	require.Len(t, got.Origins, 1)
	require.Equal(t, "none", got.Origins[0].Targets[0].AdminOverride)
	require.Len(t, got.Cluster.Peers, 1)
	require.Len(t, got.Insights, 1)
}

func TestStatusEndpoint_ClusterParam(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"/v1/status":              false,
		"/v1/status?":             false,
		"/v1/status?cluster":      false,
		"/v1/status?cluster=0":    false,
		"/v1/status?cluster=1":    true,
		"/v1/status?cluster=true": true,
	}
	for path, want := range cases {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			var got bool
			s := statusServer(t, func(_ context.Context, cluster bool) StatusReport {
				got = cluster
				return StatusReport{}
			})
			status, _ := authedGet(t, s, path)
			require.Equal(t, fasthttp.StatusOK, status)
			assert.Equal(t, want, got, "cluster flag for %q", path)
		})
	}
}

func TestStatusEndpoint_RequiresAuth(t *testing.T) {
	t.Parallel()
	s := statusServer(t, func(context.Context, bool) StatusReport { return StatusReport{} })
	ctx := testCtx("GET", "/v1/status")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusUnauthorized, ctx.Response.StatusCode())
}

func TestStatusEndpoint_DisabledWithoutFn(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	status, _ := authedGet(t, s, "/v1/status")
	assert.Equal(t, fasthttp.StatusNotFound, status)
}
