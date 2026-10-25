package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// statusFixture is a representative /v1/status document covering all
// five sections.
func statusFixture() bouineapi.StatusReport {
	return bouineapi.StatusReport{
		Node: bouineapi.StatusNode{Name: "bouine-0", Version: "v0.6.0", StartedAt: "2026-10-24T10:00:00Z", UptimeSec: 3661, Ready: true},
		Cache: bouineapi.StatusCache{
			Hits: 80, Misses: 20, HitRatio: 0.8, Evictions: 4,
			Hot:  bouineapi.StatusTier{Entries: 12, Bytes: 2048, MaxBytes: 8192},
			Warm: bouineapi.StatusTier{Entries: 30, Bytes: 4096, MaxBytes: 16384, DiskBytes: 8192},
		},
		Origins: []bouineapi.StatusPool{{
			Name: "api",
			Targets: []bouineapi.StatusTarget{
				{Addr: "10.0.0.1:80", Healthy: true, AdminOverride: "none", ProbeSuccesses: 3},
				{Addr: "10.0.0.2:80", Healthy: false, PassiveErrors: 3, ConsecutiveErrors: 3, LastChange: "2026-10-23T09:00:00Z"},
			},
		}},
		Cluster: bouineapi.StatusCluster{
			Peers:     []bouineapi.StatusPeer{{Name: "bouine-0", Version: "v0.6.0", Requests: 100, Hits: 80, Misses: 20}},
			PeerFetch: bouineapi.StatusPeerFetch{Hits: 9, Misses: 1, HopLimitHits: 0, AvgLatencyMs: 1.5},
		},
		Insights: []bouineapi.StatusInsight{{ID: "cache-low-hit-rate", Severity: "high", Title: "Hit rate is low", Action: "Raise TTLs"}},
	}
}

// statusStub serves the fixture at /v1/status.
func statusStub(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		report := statusFixture()
		if q := r.URL.Query().Get("cluster"); q == "1" || q == "true" {
			report.Cluster.Peers[0].Stale = true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(report)
	}))
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://")
}

func TestStatusTextOutput(t *testing.T) {
	server := statusStub(t)
	stdout, _, err := run(t, "status", "--server", server)
	require.NoError(t, err)
	for _, want := range []string{
		"bouine-0",
		"cache\n    hit ratio 80.0%",
		"origins",
		"10.0.0.1:80",
		"SICK",
		"passive errors 3",
		"probe streak 3",
		"last change 1d ago",
		"cluster\n    peer-fetch",
		"insights",
		"Hit rate is low",
	} {
		assert.Contains(t, stdout, want)
	}
	assert.NotContains(t, stdout, "hop-limit", "dropped from the plain cluster line")
	assert.NotContains(t, stdout, "pe:", "counter labels must be human-readable")
	assert.NotContains(t, stdout, "stale", "local mode has no request counters")
}

func TestStatusJSONOutput(t *testing.T) {
	server := statusStub(t)
	stdout, _, err := run(t, "status", "--server", server, "--format", "json")
	require.NoError(t, err)
	var got bouineapi.StatusReport
	require.NoError(t, json.Unmarshal([]byte(stdout), &got))
	require.Equal(t, "bouine-0", got.Node.Name)
	require.InDelta(t, 0.8, got.Cache.HitRatio, 0.0001)
	require.Len(t, got.Origins, 1)
	require.Len(t, got.Insights, 1)
}

func TestStatusClusterFlag(t *testing.T) {
	server := statusStub(t)
	stdout, _, err := run(t, "status", "--server", server, "--cluster")
	require.NoError(t, err)
	assert.Contains(t, stdout, "stale", "cluster mode renders the stale flag")
	assert.True(t, strings.Contains(stdout, "100 req"), "cluster mode renders per-peer request counters")
}

// TestStatusWatchAbortsInFlightFetchOnCancel pins the Ctrl-C contract:
// a cancelled context aborts the in-flight fetch instead of hanging
// until --interval elapses. Synchronization is channel-based; no
// sleeps.
func TestStatusWatchAbortsInFlightFetchOnCancel(t *testing.T) {
	received := make(chan struct{})
	block := make(chan struct{})
	var once sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(received) })
		<-block
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(func() { close(block); ts.Close() })
	server := strings.TrimPrefix(ts.URL, "http://")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := Root()
	var stdout strings.Builder
	root.SetOut(&stdout)
	root.SetErr(&stdout)
	root.SetContext(ctx)
	root.SetArgs([]string{"status", "--server", server, "--watch", "--interval", "10ms"})
	done := make(chan error, 1)
	go func() { done <- root.Execute() }()

	<-received // fetch is in flight
	cancel()   // SIGINT equivalent
	select {
	case err := <-done:
		require.Error(t, err, "the aborted fetch surfaces as an error, promptly")
	case <-time.After(5 * time.Second):
		t.Fatal("watch loop did not return after context cancellation")
	}
	assert.Equal(t, 0, strings.Count(stdout.String(), "hit ratio"), "no partial frame")
}

func TestStatusWatchIntervalValidation(t *testing.T) {
	server := statusStub(t)
	_, _, err := run(t, "status", "--server", server, "--watch", "--interval", "0s")
	require.Error(t, err, "zero interval must be rejected")
}
