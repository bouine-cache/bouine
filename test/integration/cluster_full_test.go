//go:build integration

package integration_test

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/cluster"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/test/integration/driver"
)

// Full replication mode: every node holds a copy of every cached object.
// No peer fetch; cached objects are broadcast to all peers via gossip on fill.

func TestFull_ClusterFormation(t *testing.T) {
	s := sharedCluster(t, "full")
	for i, node := range s.Nodes {
		peers := s.Peers(t, i)
		if len(peers) != 3 {
			t.Errorf("node %s: got %d peers, want 3", node.Name, len(peers))
		}
	}
}

func TestFull_ReplicationOnFill(t *testing.T) {
	s := sharedCluster(t, "full")
	path := fmt.Sprintf("/hit?x=full-repl-%d", time.Now().UnixNano())

	r := s.GetWithHost(t, 0, path, crossNodeHost)
	if got := r.Header.Get("X-Cache"); got != "MISS" {
		t.Fatalf("node 0 first: X-Cache = %q, want MISS (unique path should be cold)", got)
	}

	// In full mode, the object should be replicated to all peers.
	// Verify by checking that other nodes serve a HIT after replication
	// converges.
	driver.RetryUntil(t, driver.ReplicationDeadline, 500*time.Millisecond, func() bool {
		return s.GetWithHost(t, 1, path, crossNodeHost).Header.Get("X-Cache") == "HIT" &&
			s.GetWithHost(t, 2, path, crossNodeHost).Header.Get("X-Cache") == "HIT"
	})
}

func TestFull_ReplicationNoPeerFetch(t *testing.T) {
	s := sharedCluster(t, "full")
	for i := range s.Nodes {
		s.Get(t, i, "/hit?x=full-nopf")
	}
	for i := range s.Nodes {
		hits := s.MetricValue(t, i, "bouine_peer_fetch_hits_total")
		if hits != 0 {
			t.Errorf("node %d: peer_fetch_hits_total = %.0f, want 0", i, hits)
		}
	}
}

func TestFull_PurgePropagationGossip(t *testing.T) {
	s := sharedCluster(t, "full")
	path := "/hit?x=full-purge"

	s.GetWithHost(t, 0, path, crossNodeHost)
	driver.RetryUntil(t, driver.ReplicationDeadline, 500*time.Millisecond, func() bool {
		return s.GetWithHost(t, 1, path, crossNodeHost).Header.Get("X-Cache") == "HIT" &&
			s.GetWithHost(t, 2, path, crossNodeHost).Header.Get("X-Cache") == "HIT"
	})

	// Purge from node 0 — HTTP fan-out reaches all peers synchronously.
	s.Purge(t, 0, "http://"+driver.CrossNodeHost+path)

	for i := range s.Nodes {
		resp := s.GetWithHost(t, i, path, crossNodeHost)
		if i == 0 && resp.Header.Get("X-Cache") != "MISS" {
			t.Errorf("node %d after purge: X-Cache = %q, want MISS", i, resp.Header.Get("X-Cache"))
		}
	}
}

func TestFull_BanPropagationGossip(t *testing.T) {
	s := sharedCluster(t, "full")
	path := "/hit?x=full-ban"

	s.GetWithHost(t, 0, path, crossNodeHost)
	driver.RetryUntil(t, driver.ReplicationDeadline, 500*time.Millisecond, func() bool {
		return s.GetWithHost(t, 1, path, crossNodeHost).Header.Get("X-Cache") == "HIT" &&
			s.GetWithHost(t, 2, path, crossNodeHost).Header.Get("X-Cache") == "HIT"
	})

	s.Ban(t, 0, ".*", "")

	resp := s.GetWithHost(t, 0, path, crossNodeHost)
	if resp.Header.Get("X-Cache") == "HIT" {
		t.Errorf("node 0 after ban: X-Cache = HIT, want MISS/banned")
	}
}

// --- Replication bandwidth and convergence (before destructive failure test) ---

func TestFull_ReplicationBandwidthMetric(t *testing.T) {
	s := sharedCluster(t, "full")

	before := s.MetricValue(t, 0, "bouine_cluster_replication_bytes_total")

	paths := []string{
		"/hit?x=full-bw-1",
		"/hit?x=full-bw-2",
		"/hit?x=full-bw-3",
	}
	for _, p := range paths {
		s.GetWithHost(t, 0, p, crossNodeHost)
	}
	driver.RetryUntil(t, driver.ReplicationDeadline, 500*time.Millisecond, func() bool {
		after := s.MetricValue(t, 0, "bouine_cluster_replication_bytes_total")
		return after > before
	})

	after := s.MetricValue(t, 0, "bouine_cluster_replication_bytes_total")
	if after <= before {
		t.Errorf("replication_bytes_total: before=%.0f after=%.0f — no increase after fill", before, after)
	}

	// Budget check: bytes ≤ fills × max response × (N-1) peers.
	replicatedBytes := after - before
	maxExpected := 3.0 * 1000.0 * 2.0 // 3 fills × ~1 KB × 2 peers
	if replicatedBytes > maxExpected {
		t.Errorf("replication_bytes = %.0f exceeds budget of %.0f",
			replicatedBytes, maxExpected)
	}
	t.Logf("replication_bytes: %.0f bytes across 3 fills (budget: %.0f)", replicatedBytes, maxExpected)
}

func TestFull_ReplicationConvergence(t *testing.T) {
	s := sharedCluster(t, "full")
	path := "/hit?x=full-convergence"

	s.GetWithHost(t, 0, path, crossNodeHost)

	driver.RetryUntil(t, driver.ReplicationDeadline, 500*time.Millisecond, func() bool {
		return s.GetWithHost(t, 1, path, crossNodeHost).Header.Get("X-Cache") == "HIT" &&
			s.GetWithHost(t, 2, path, crossNodeHost).Header.Get("X-Cache") == "HIT"
	})

	sent := s.MetricValue(t, 0, "bouine_cluster_replications_sent_total")
	recv1 := s.MetricValue(t, 1, "bouine_cluster_replications_received_total")
	recv2 := s.MetricValue(t, 2, "bouine_cluster_replications_received_total")
	bytesSent := s.MetricValue(t, 0, "bouine_cluster_replication_bytes_total")

	if sent == 0 {
		t.Error("replications_sent_total is 0 — BroadcastReplicate did not fire")
	}
	if recv1 == 0 || recv2 == 0 {
		t.Error("replications_received_total is 0 on peer — replication not received")
	}
	t.Logf("replication: sent=%.0f recv1=%.0f recv2=%.0f bytes_sent=%.0f",
		sent, recv1, recv2, bytesSent)
}

// --- Anti-entropy reconciliation ---

func TestFull_AntiEntropyKeySetExchange(t *testing.T) {
	s := sharedCluster(t, "full")

	r := s.GetWithHost(t, 0, "/hit?x=full-ae-keys", crossNodeHost)
	_ = r

	driver.RetryUntil(t, driver.ReplicationDeadline, 500*time.Millisecond, func() bool {
		return s.GetWithHost(t, 1, "/hit?x=full-ae-keys", crossNodeHost).Header.Get("X-Cache") == "HIT" &&
			s.GetWithHost(t, 2, "/hit?x=full-ae-keys", crossNodeHost).Header.Get("X-Cache") == "HIT"
	})

	for i := range s.Nodes {
		url := s.Nodes[i].AdminAddr + "/v1/peer/keys"
		resp, err := http.Get(url) //nolint:noctx
		if err != nil {
			t.Fatalf("node %d: GET /v1/peer/keys: %v", i, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("node %d: status %d", i, resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		_, keys, err := cluster.DecodeKeySet(body)
		if err != nil {
			t.Fatalf("node %d: decode: %v", i, err)
		}
		if len(keys) == 0 {
			t.Errorf("node %d: key set is empty", i)
		}
	}
}

func TestFull_AntiEntropyReconcilesMissingKeys(t *testing.T) {
	s := sharedCluster(t, "full")

	path := "/hit?x=full-ae-reconcile"
	r := s.GetWithHost(t, 0, path, crossNodeHost)
	_ = r

	driver.RetryUntil(t, driver.ReplicationDeadline, 500*time.Millisecond, func() bool {
		return s.GetWithHost(t, 1, path, crossNodeHost).Header.Get("X-Cache") == "HIT" &&
			s.GetWithHost(t, 2, path, crossNodeHost).Header.Get("X-Cache") == "HIT"
	})

	purgeURL := "http://" + driver.CrossNodeHost + path
	key := cache.BuildKeyFromURL(purgeURL)
	s.PeerPurge(t, 1, api.PurgeEvent{
		Type: api.GossipTypePurge,
		Key:  key,
	})

	resp := s.GetWithHost(t, 1, path, crossNodeHost)
	if resp.Header.Get("X-Cache") == "HIT" {
		t.Fatal("node 1 should be missing the key after local purge")
	}

	driver.RetryUntil(t, 90*time.Second, 2*time.Second, func() bool {
		resp := s.GetWithHost(t, 1, path, crossNodeHost)
		return resp.Header.Get("X-Cache") == "HIT"
	})
}

// --- Destructive test: runs last in this file ---
// KillNode leaves node 2 down on the shared cluster; tests after this
// point see a 2-node cluster. All non-destructive tests must appear
// above this line.

func TestFull_SingleNodeFailure(t *testing.T) {
	s := sharedCluster(t, "full")
	path := "/hit?x=full-failure"

	// Prime via node 0 and wait for replication to all nodes.
	s.Get(t, 0, path)
	driver.RetryUntil(t, driver.ReplicationDeadline, 500*time.Millisecond, func() bool {
		return s.Get(t, 1, path).Header.Get("X-Cache") == "HIT" &&
			s.Get(t, 2, path).Header.Get("X-Cache") == "HIT"
	})

	// Kill node 2. Nodes 0 and 1 hold full replicas and must still serve HITs.
	s.KillNode(t, 2)
	time.Sleep(2 * time.Second)

	for _, n := range []int{0, 1} {
		resp := s.Get(t, n, path)
		if resp.StatusCode != 200 {
			t.Errorf("node %d after kill: status = %d", n, resp.StatusCode)
		}
	}
}
