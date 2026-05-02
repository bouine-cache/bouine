//go:build integration

// Package chaos_test covers the chaos scenarios required by PLAN.md §4.5:
//
//   - Peer kill — a node dies mid-traffic; remaining nodes must return 200.
//   - Origin flap — origin bounces repeatedly; cache must serve stale.
//   - Partial partition — one node is paused (SIGSTOP); cluster must stay
//     consistent once the partition heals.
//   - Slow origin — high-latency origin; cache must absorb load and serve
//     hits without timeout propagation.
//   - Rolling restart — each node restarted in sequence; zero 5xx observed.
//
// Run with:
//
//	make chaos
//
// Or directly:
//
//	go test -v -race -count=1 -timeout=20m -tags=integration \
//	  ./test/chaos/...
//
// Prerequisites: Docker Engine with compose v2.
package chaos_test

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thylong/bouine/test/integration/driver"
)

// TestChaos_PeerKill populates 50 objects via node 0, kills node 2, then
// confirms that surviving nodes 0 and 1 return 200 for all cached keys.
func TestChaos_PeerKill(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong", NoAutoCleanup: true})
	defer s.Down()

	const n = 50
	for i := range n {
		url := fmt.Sprintf("%s/chaos/pk/%d", s.Nodes[0].HTTPAddr, i)
		resp, err := http.Get(url) //nolint:noctx
		if err != nil {
			t.Fatalf("populate %d: %v", i, err)
		}
		resp.Body.Close()
	}

	s.KillNode(t, 2)

	failures := 0
	for _, node := range s.Nodes[:2] {
		for i := range n {
			url := fmt.Sprintf("%s/chaos/pk/%d", node.HTTPAddr, i)
			resp, err := http.Get(url) //nolint:noctx
			if err != nil {
				t.Logf("node %s key %d: %v", node.Name, i, err)
				failures++
				continue
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Logf("node %s key %d: status %d", node.Name, i, resp.StatusCode)
				failures++
			}
		}
	}
	if failures > 0 {
		t.Errorf("peer kill: %d/%d requests failed on surviving nodes", failures, n*2)
	}

	s.RestartNode(t, 2)
}

// TestChaos_OriginFlap populates keys with stale-if-error semantics, lets
// them expire, then flaps the origin 5 times while a goroutine hammers the
// cache. Asserts zero 5xx responses.
func TestChaos_OriginFlap(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "eventual", NoAutoCleanup: true})
	defer s.Down()

	const n = 20
	for i := range n {
		url := fmt.Sprintf("%s/chaos/flap/%d", s.Nodes[0].HTTPAddr, i)
		resp, err := http.Get(url) //nolint:noctx
		if err != nil {
			t.Fatalf("populate %d: %v", i, err)
		}
		resp.Body.Close()
	}

	// Expire the short TTLs.
	time.Sleep(6 * time.Second)

	var (
		wg        sync.WaitGroup
		errors5xx atomic.Int64
		total     atomic.Int64
		stop      atomic.Bool
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 3 * time.Second}
		for !stop.Load() {
			for i := range n {
				url := fmt.Sprintf("%s/chaos/flap/%d", s.Nodes[0].HTTPAddr, i)
				resp, err := client.Get(url)
				if err != nil {
					continue
				}
				total.Add(1)
				if resp.StatusCode >= 500 {
					errors5xx.Add(1)
				}
				resp.Body.Close()
			}
		}
	}()

	s.FlapOrigin(t, 5, 500*time.Millisecond)
	time.Sleep(3 * time.Second)
	stop.Store(true)
	wg.Wait()

	t.Logf("origin flap: %d requests, %d 5xx", total.Load(), errors5xx.Load())
	if errors5xx.Load() > 0 {
		t.Errorf("expected 0 5xx during origin flap (stale-on-error), got %d/%d",
			errors5xx.Load(), total.Load())
	}
}

// TestChaos_PartialPartition pauses a node (SIGSTOP), issues a purge that it
// cannot receive, then unpauses and waits for gossip healing. Verifies all
// nodes return 200 afterward.
func TestChaos_PartialPartition(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong", NoAutoCleanup: true})
	defer s.Down()

	const refURL = "/chaos/partition/ref"
	for i, node := range s.Nodes {
		resp, err := http.Get(node.HTTPAddr + refURL) //nolint:noctx
		if err != nil {
			t.Fatalf("populate node %d: %v", i, err)
		}
		resp.Body.Close()
	}

	// Pause node 2 — it becomes a "split brain" member.
	s.PauseNode(t, 2)

	// Purge from node 0; node 2 misses the HTTP fan-out.
	s.Purge(t, 0, refURL)

	// Nodes 0 and 1 must be a MISS (purge applied).
	for _, n := range []int{0, 1} {
		resp, err := http.Get(s.Nodes[n].HTTPAddr + refURL) //nolint:noctx
		if err != nil {
			t.Fatalf("node %d post-purge: %v", n, err)
		}
		resp.Body.Close()
		if resp.Header.Get("X-Cache") == "HIT" {
			t.Errorf("node %d: expected MISS after purge, got HIT", n)
		}
	}

	// Heal the partition and wait for gossip convergence.
	s.UnpauseNode(t, 2)
	time.Sleep(driver.GossipConvergence)

	// All nodes must be reachable and return 200.
	for i, node := range s.Nodes {
		resp, err := http.Get(node.HTTPAddr + refURL) //nolint:noctx
		if err != nil {
			t.Fatalf("node %d post-heal: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("node %d post-heal: status %d", i, resp.StatusCode)
		}
	}
}

// TestChaos_SlowOrigin warms the cache through a 500 ms origin and then
// confirms that cached hits are served in < 50 ms. Skipped if tc-netem is
// unavailable in the container environment.
func TestChaos_SlowOrigin(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "eventual", NoAutoCleanup: true})
	defer s.Down()

	if err := s.ScaleOriginLatency(500); err != nil {
		t.Skipf("tc-netem unavailable (%v) — skipping", err)
	}
	t.Cleanup(func() { _ = s.ScaleOriginLatency(0) })

	const url = "/chaos/slow/1"
	start := time.Now()
	resp, err := http.Get(s.Nodes[0].HTTPAddr + url) //nolint:noctx
	if err != nil {
		t.Fatalf("warm: %v", err)
	}
	resp.Body.Close()
	t.Logf("warm request: %v (500ms origin delay expected)", time.Since(start))

	const hitBudgetMs = 50
	for range 10 {
		start = time.Now()
		resp, err = http.Get(s.Nodes[0].HTTPAddr + url) //nolint:noctx
		if err != nil {
			t.Fatalf("hit: %v", err)
		}
		resp.Body.Close()
		dur := time.Since(start)
		if dur > time.Duration(hitBudgetMs)*time.Millisecond {
			t.Errorf("hit latency %v > %dms budget", dur, hitBudgetMs)
		}
	}
}

// TestChaos_RollingRestart restarts every node once in sequence while a
// background goroutine issues requests. Asserts zero 5xx across the restart.
func TestChaos_RollingRestart(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong", NoAutoCleanup: true})
	defer s.Down()

	const n = 30
	for i := range n {
		url := fmt.Sprintf("%s/chaos/roll/%d", s.Nodes[0].HTTPAddr, i)
		resp, err := http.Get(url) //nolint:noctx
		if err != nil {
			t.Fatalf("populate %d: %v", i, err)
		}
		resp.Body.Close()
	}

	var (
		wg        sync.WaitGroup
		errors5xx atomic.Int64
		total     atomic.Int64
		stop      atomic.Bool
	)

	for _, node := range s.Nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			client := &http.Client{Timeout: 2 * time.Second}
			for !stop.Load() {
				for i := range n {
					url := fmt.Sprintf("%s/chaos/roll/%d", addr, i)
					resp, err := client.Get(url)
					if err != nil {
						continue
					}
					total.Add(1)
					if resp.StatusCode >= 500 {
						errors5xx.Add(1)
					}
					resp.Body.Close()
				}
			}
		}(node.HTTPAddr)
	}

	for i := range len(s.Nodes) {
		t.Logf("rolling restart: node %d", i)
		s.KillNode(t, i)
		time.Sleep(2 * time.Second)
		s.RestartNode(t, i)
		time.Sleep(5 * time.Second) // rejoin gossip
	}

	time.Sleep(3 * time.Second)
	stop.Store(true)
	wg.Wait()

	t.Logf("rolling restart: %d requests, %d 5xx", total.Load(), errors5xx.Load())
	if errors5xx.Load() > 0 {
		t.Errorf("rolling restart: %d 5xx responses (must be 0)", errors5xx.Load())
	}
}
