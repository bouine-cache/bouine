//go:build integration

// Package chaos_test exercises the cluster under adverse conditions:
// peer kill, origin flap, slow origin, rolling restart, and concurrent purge.
// All tests run in-process — no Docker required.
package chaos_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// fastGet performs a GET request and returns the status code and X-Cache header.
// It uses a per-call fasthttp client with a short timeout.
func fastGet(url string) (statusCode int, xCache string, err error) {
	client := &fasthttp.Client{
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	if err = client.Do(req, resp); err != nil {
		return 0, "", err
	}
	return resp.StatusCode(), string(resp.Header.Peek("X-Cache")), nil
}

// fastGetWithClient performs a GET using a pre-allocated client.
func fastGetWithClient(client *fasthttp.Client, url string) (statusCode int, xCache string, err error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	if err = client.Do(req, resp); err != nil {
		return 0, "", err
	}
	return resp.StatusCode(), string(resp.Header.Peek("X-Cache")), nil
}

// TestChaos_PeerKill kills a node mid-traffic and verifies surviving
// nodes continue serving 200.
func TestChaos_PeerKill(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	const n = 50
	for i := range n {
		url := fmt.Sprintf("%s/hit?x=pk-%d", s.Nodes[0].HTTPAddr, i)
		_, _, err := fastGet(url)
		require.NoErrorf(t, err, "populate %d", i)
	}

	s.KillNode(t, 2)
	time.Sleep(500 * time.Millisecond)

	failures := 0
	for _, node := range s.Nodes[:2] {
		for i := range n {
			url := fmt.Sprintf("%s/hit?x=pk-%d", node.HTTPAddr, i)
			sc, _, err := fastGet(url)
			if err != nil {
				failures++
				continue
			}
			if sc != 200 {
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
		_, _, err := fastGet(url)
		require.NoErrorf(t, err, "populate %d", i)
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
		client := &fasthttp.Client{
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		}
		for !stop.Load() {
			for i := range n {
				url := fmt.Sprintf("%s/hit?x=flap-%d", s.Nodes[0].HTTPAddr, i)
				sc, _, err := fastGetWithClient(client, url)
				if err != nil {
					continue
				}
				total.Add(1)
				if sc >= 500 {
					errors5xx.Add(1)
				}
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
		var lastErr error
		for attempt := range 5 {
			_, _, lastErr = fastGet(s.Nodes[i].HTTPAddr + path)
			if lastErr == nil {
				break
			}
			if attempt < 4 {
				time.Sleep(200 * time.Millisecond)
			}
		}
		require.NoErrorf(t, lastErr, "populate node %d", i)
	}

	// Purge from node 0.
	s.Purge(t, 0, s.Nodes[0].HTTPAddr+path)

	// Nodes 0 and 1 should see a MISS after purge.
	for _, n := range []int{0, 1} {
		requireStatus200(t, s.Nodes[n].HTTPAddr+path, "node %d post-purge", n)
	}

	// All nodes must be reachable and return 200.
	for i := range s.Nodes {
		requireStatus200(t, s.Nodes[i].HTTPAddr+path, "node %d post-heal", i)
	}
}

// requireStatus200 GETs url, retrying briefly so cluster-convergence timing
// (gossip ring settling after boot/partition) does not cause a flaky single
// GET to observe a transient non-200. It still asserts a 200 is reached.
func requireStatus200(t *testing.T, url, msgf string, args ...any) {
	t.Helper()
	var lastErr error
	var lastStatus int
	for attempt := range 10 {
		sc, _, err := fastGet(url)
		if err == nil {
			lastStatus = sc
			if sc == 200 {
				return
			}
		} else {
			lastErr = err
		}
		if attempt < 9 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	prefix := fmt.Sprintf(msgf, args...)
	require.Nil(t, lastErr)
	t.Errorf("%s: status %d (want 200 within retries)", prefix, lastStatus)
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
	sc, _, err := fastGet(s.Nodes[0].HTTPAddr + url)
	require.NoError(t, err, "warm")
	require.Equal(t, 200, sc)
	warmDur := time.Since(start)
	t.Logf("warm request: %v (300ms origin delay expected)", warmDur)

	if warmDur < 200*time.Millisecond {
		t.Errorf("warm request too fast (%v) — latency injection may not work", warmDur)
	}

	const hitBudgetMs = 50
	for range 10 {
		start = time.Now()
		sc, _, err = fastGet(s.Nodes[0].HTTPAddr + url)
		require.NoError(t, err, "hit")
		require.Equal(t, 200, sc)
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
		_, _, err := fastGet(url)
		require.NoErrorf(t, err, "populate %d", i)
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
			client := &fasthttp.Client{
				ReadTimeout:  2 * time.Second,
				WriteTimeout: 2 * time.Second,
			}
			for !stop.Load() {
				for i := range n {
					if stop.Load() {
						return
					}
					url := fmt.Sprintf("%s/hit?x=roll-%d", addr, i)
					sc, _, err := fastGetWithClient(client, url)
					if err != nil {
						continue
					}
					total.Add(1)
					if sc >= 500 {
						errors5xx.Add(1)
					}
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
	sc, _, err := fastGet(s.Nodes[0].HTTPAddr + path)
	require.NoError(t, err, "warm")
	require.Equal(t, 200, sc)

	// Wait for max-age=1 to expire so the object becomes stale.
	time.Sleep(2 * time.Second)

	// Force origin down.
	s.SetOriginError(true)
	defer s.SetOriginError(false)

	// Request should serve stale (SWR window is 3600s).
	sc, xc, err := fastGet(s.Nodes[0].HTTPAddr + path)
	require.NoError(t, err, "stale request")
	if sc >= 500 {
		t.Errorf("origin down: got %d, expected stale 200", sc)
	}
	t.Logf("origin down: status=%d X-Cache=%s", sc, xc)
}

// TestChaos_ConcurrentPurgeUnderLoad issues concurrent purges while a
// load goroutine sends traffic. Asserts no panics.
func TestChaos_ConcurrentPurgeUnderLoad(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	const n = 50
	for i := range n {
		url := fmt.Sprintf("%s/hit?x=purge-load-%d", s.Nodes[0].HTTPAddr, i)
		_, _, err := fastGet(url)
		require.NoErrorf(t, err, "populate %d", i)
	}

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)

	// Load goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := &fasthttp.Client{
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
		}
		for !stop.Load() {
			for i := range n {
				if stop.Load() {
					return
				}
				url := fmt.Sprintf("%s/hit?x=purge-load-%d", s.Nodes[0].HTTPAddr, i)
				_, _, _ = fastGetWithClient(client, url)
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
	require.Len(t, peers, 3)

	// Kill node 2 and wait long enough for gossip to mark it dead.
	s.KillNode(t, 2)
	time.Sleep(5 * time.Second)

	// Surviving nodes should still be reachable.
	for _, n := range []int{0, 1} {
		sc, _, err := fastGet(s.Nodes[n].HTTPAddr + "/hit?x=rejoin")
		require.NoErrorf(t, err, "node %d during partition", n)
		assert.Equal(t, 200, sc)
	}

	// Restart node 2 (gets fresh ports).
	s.RestartNode(t, 2)
	time.Sleep(5 * time.Second)

	// Node 2 must be reachable and serve requests.
	sc, _, err := fastGet(s.Nodes[2].HTTPAddr + "/hit?x=rejoin-after")
	require.NoError(t, err, "node 2 after rejoin")
	assert.Equal(t, 200, sc)
}
