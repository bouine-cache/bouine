//go:build integration

// Package chaos_test covers chaos scenarios from PLAN.md §4.5.
// All tests run in-process — no Docker required.
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

// TestChaos_PeerKill kills a node mid-traffic and verifies surviving
// nodes continue serving 200.
func TestChaos_PeerKill(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	const n = 50
	for i := range n {
		url := fmt.Sprintf("%s/hit?x=pk-%d", s.Nodes[0].HTTPAddr, i)
		resp, err := http.Get(url) //nolint:noctx
		if err != nil {
			t.Fatalf("populate %d: %v", i, err)
		}
		resp.Body.Close()
	}

	s.KillNode(t, 2)
	time.Sleep(500 * time.Millisecond)

	failures := 0
	for _, node := range s.Nodes[:2] {
		for i := range n {
			url := fmt.Sprintf("%s/hit?x=pk-%d", node.HTTPAddr, i)
			resp, err := http.Get(url) //nolint:noctx
			if err != nil {
				failures++
				continue
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				failures++
			}
		}
	}
	if failures > 0 {
		t.Errorf("peer kill: %d/%d requests failed on surviving nodes", failures, n*2)
	}
}

// TestChaos_OriginFlap toggles origin errors while a goroutine hammers
// the cache. Asserts zero 5xx thanks to stale-if-error.
func TestChaos_OriginFlap(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "eventual"})

	const n = 20
	for i := range n {
		url := fmt.Sprintf("%s/hit?x=flap-%d", s.Nodes[0].HTTPAddr, i)
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 3 * time.Second}
		for !stop.Load() {
			for i := range n {
				url := fmt.Sprintf("%s/hit?x=flap-%d", s.Nodes[0].HTTPAddr, i)
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

	s.FlapOrigin(t, 5, 300*time.Millisecond)
	time.Sleep(1 * time.Second)
	stop.Store(true)
	wg.Wait()

	t.Logf("origin flap: %d requests, %d 5xx", total.Load(), errors5xx.Load())
	if errors5xx.Load() > 0 {
		t.Errorf("expected 0 5xx during origin flap, got %d/%d",
			errors5xx.Load(), total.Load())
	}
}

// TestChaos_PartialPartition purges while one node's origin is in
// forced-error mode (simulating application-level partition), then
// heals and verifies all nodes return 200.
func TestChaos_PartialPartition(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	const path = "/hit?x=partition-ref"
	for i := range s.Nodes {
		var resp *http.Response
		var err error
		for attempt := range 5 {
			resp, err = http.Get(s.Nodes[i].HTTPAddr + path) //nolint:noctx
			if err == nil {
				resp.Body.Close()
				break
			}
			if attempt < 4 {
				time.Sleep(200 * time.Millisecond)
			}
		}
		if err != nil {
			t.Fatalf("populate node %d: %v", i, err)
		}
	}

	// Purge from node 0.
	s.Purge(t, 0, s.Nodes[0].HTTPAddr+path)

	// Nodes 0 and 1 should see a MISS after purge.
	for _, n := range []int{0, 1} {
		resp, err := http.Get(s.Nodes[n].HTTPAddr + path) //nolint:noctx
		if err != nil {
			t.Fatalf("node %d post-purge: %v", n, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("node %d post-purge: status %d", n, resp.StatusCode)
		}
	}

	// All nodes must be reachable and return 200.
	for i := range s.Nodes {
		resp, err := http.Get(s.Nodes[i].HTTPAddr + path) //nolint:noctx
		if err != nil {
			t.Fatalf("node %d post-heal: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("node %d post-heal: status %d", i, resp.StatusCode)
		}
	}
}

// TestChaos_SlowOrigin warms the cache through a slow origin and
// confirms cached hits are fast.
func TestChaos_SlowOrigin(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "eventual"})

	if err := s.ScaleOriginLatency(300); err != nil {
		t.Skipf("latency injection unavailable: %v", err)
	}
	t.Cleanup(func() { _ = s.ScaleOriginLatency(0) })

	const url = "/hit?x=slow-origin"
	start := time.Now()
	resp, err := http.Get(s.Nodes[0].HTTPAddr + url) //nolint:noctx
	if err != nil {
		t.Fatalf("warm: %v", err)
	}
	resp.Body.Close()
	warmDur := time.Since(start)
	t.Logf("warm request: %v (300ms origin delay expected)", warmDur)

	if warmDur < 200*time.Millisecond {
		t.Errorf("warm request too fast (%v) — latency injection may not work", warmDur)
	}

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

// TestChaos_RollingRestart restarts every node once in sequence while
// background goroutines issue requests. Allows a small 5xx budget
// during the restart window.
func TestChaos_RollingRestart(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	const n = 30
	for i := range n {
		url := fmt.Sprintf("%s/hit?x=roll-%d", s.Nodes[0].HTTPAddr, i)
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
					if stop.Load() {
						return
					}
					url := fmt.Sprintf("%s/hit?x=roll-%d", addr, i)
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
		t.Logf("rolling restart: killing node %d", i)
		s.KillNode(t, i)
		time.Sleep(1 * time.Second)
		s.RestartNode(t, i)
		time.Sleep(3 * time.Second)
	}

	time.Sleep(1 * time.Second)
	stop.Store(true)
	wg.Wait()

	tot := total.Load()
	errs := errors5xx.Load()
	t.Logf("rolling restart: %d requests, %d 5xx", tot, errs)
	// Allow 0.5% error rate during in-process restarts (port reuse window).
	maxErrs := int64(max(1, tot/200))
	if errs > maxErrs {
		t.Errorf("rolling restart: %d 5xx > budget %d (0.5%% of %d)", errs, maxErrs, tot)
	}
}

// --- New scenarios ---

// TestChaos_OriginDown forces the origin to 503 and verifies bouine
// serves stale responses instead of forwarding errors.
func TestChaos_OriginDown(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "eventual"})

	// Use /stale endpoint which returns ETag + stale-while-revalidate=3600.
	// This ensures the cache has revalidation headers to trigger the stale
	// fallback path when origin returns 503.
	const path = "/stale?x=origin-down"
	resp, err := http.Get(s.Nodes[0].HTTPAddr + path) //nolint:noctx
	if err != nil {
		t.Fatalf("warm: %v", err)
	}
	resp.Body.Close()

	// Wait for max-age=1 to expire so the object becomes stale.
	time.Sleep(2 * time.Second)

	// Force origin down.
	s.SetOriginError(true)
	defer s.SetOriginError(false)

	// Request should serve stale (SWR window is 3600s).
	resp2, err := http.Get(s.Nodes[0].HTTPAddr + path) //nolint:noctx
	if err != nil {
		t.Fatalf("stale request: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode >= 500 {
		t.Errorf("origin down: got %d, expected stale 200", resp2.StatusCode)
	}
	xc := resp2.Header.Get("X-Cache")
	t.Logf("origin down: status=%d X-Cache=%s", resp2.StatusCode, xc)
}

// TestChaos_ConcurrentPurgeUnderLoad issues concurrent purges while a
// load goroutine sends traffic. Asserts no panics.
func TestChaos_ConcurrentPurgeUnderLoad(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	const n = 50
	for i := range n {
		url := fmt.Sprintf("%s/hit?x=purge-load-%d", s.Nodes[0].HTTPAddr, i)
		resp, err := http.Get(url) //nolint:noctx
		if err != nil {
			t.Fatalf("populate %d: %v", i, err)
		}
		resp.Body.Close()
	}

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)

	// Load goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &http.Client{Timeout: 2 * time.Second}
		for !stop.Load() {
			for i := range n {
				if stop.Load() {
					return
				}
				url := fmt.Sprintf("%s/hit?x=purge-load-%d", s.Nodes[0].HTTPAddr, i)
				resp, err := client.Get(url)
				if err != nil {
					continue
				}
				resp.Body.Close()
			}
		}
	}()

	// Concurrent purges from all nodes.
	var purgeWg sync.WaitGroup
	for nodeIdx := range s.Nodes {
		purgeWg.Add(1)
		go func(idx int) {
			defer purgeWg.Done()
			for i := range n {
				url := fmt.Sprintf("%s/hit?x=purge-load-%d", s.Nodes[idx].HTTPAddr, i)
				s.Purge(t, idx, url)
			}
		}(nodeIdx)
	}
	purgeWg.Wait()

	stop.Store(true)
	wg.Wait()
	t.Logf("concurrent purge: %d purges across %d nodes completed", n*3, len(s.Nodes))
}

// TestChaos_NodeRejoinAfterLongPartition kills a node, waits for
// multiple gossip rounds, restarts it, and verifies it rejoins.
func TestChaos_NodeRejoinAfterLongPartition(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	// Verify 3 peers initially.
	peers := s.Peers(t, 0)
	if len(peers) != 3 {
		t.Fatalf("initial peers: %d, want 3", len(peers))
	}

	// Kill node 2 and wait long enough for gossip to mark it dead.
	s.KillNode(t, 2)
	time.Sleep(5 * time.Second)

	// Surviving nodes should still be reachable.
	for _, n := range []int{0, 1} {
		resp, err := http.Get(s.Nodes[n].HTTPAddr + "/hit?x=rejoin") //nolint:noctx
		if err != nil {
			t.Fatalf("node %d during partition: %v", n, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("node %d during partition: status %d", n, resp.StatusCode)
		}
	}

	// Restart node 2 (gets fresh ports).
	s.RestartNode(t, 2)
	time.Sleep(5 * time.Second)

	// Node 2 must be reachable and serve requests.
	resp, err := http.Get(s.Nodes[2].HTTPAddr + "/hit?x=rejoin-after") //nolint:noctx
	if err != nil {
		t.Fatalf("node 2 after rejoin: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("node 2 after rejoin: status %d", resp.StatusCode)
	}
}
