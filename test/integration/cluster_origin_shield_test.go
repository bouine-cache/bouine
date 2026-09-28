//go:build integration

package integration_test

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/test/integration/driver"
)

// shieldRunSeq gives each acceptance-wave invocation a fresh cache key,
// so -count=N and re-runs on a shared stack still see a cold key.
var shieldRunSeq atomic.Int64

// sharedShieldCluster boots (once) a strong-mode stack with the
// origin shield (cluster.origin_shield) enabled on every node.
func sharedShieldCluster(t *testing.T) *driver.ClusterStack {
	t.Helper()
	const key = "strong+shield"
	clusterMu.Lock()
	defer clusterMu.Unlock()
	if s, ok := clusterStacks[key]; ok {
		return s
	}
	s := driver.BootCluster(t, driver.ClusterOptions{
		Mode:          "strong",
		NoAutoCleanup: true,
		OriginShield:  true,
	})
	clusterClean = append(clusterClean, s.Down)
	clusterStacks[key] = s
	return s
}

// TestStrong_OriginShield_ColdKeyOneOriginRequest is the shield's
// acceptance test (plan §5): a cold key hit by concurrent requests
// across every node must cost the origin exactly ONE request for the
// whole cluster — the mass-purge/deploy/TTL-expiry stampede the shield
// exists to kill.
//
// Every node requests the same path under the same Host header so all
// nodes derive the same cache key and the same ring owner. The origin
// body embeds a per-request timestamp, so identical bodies across
// nodes prove every response came from the single shared owner fill.
//
// The origin is slowed (100ms) so the whole burst lands inside the
// owner's in-progress origin fetch (its standard request-collapsing
// latch coalesces shield forwards with its own clients — ADR-0052
// positive consequence #4).
func TestStrong_OriginShield_ColdKeyOneOriginRequest(t *testing.T) {
	s := sharedShieldCluster(t)
	require.NoError(t, s.ScaleOriginLatency(100))
	t.Cleanup(func() { _ = s.ScaleOriginLatency(0) })

	// No blind convergence sleep: each attempt runs the full acceptance
	// wave on a FRESH key and only succeeds when the ring has converged
	// (exactly one origin request). A stale-ring overshoot on attempt N
	// — a requester addressing a non-owner gets a 404 and fetches origin
	// itself (accepted mid-churn behavior, D5) — costs a second origin
	// request for THAT key only; the next attempt uses a new key and
	// cannot inherit the overshoot.
	const perNode = 3
	var (
		path        string
		originDelta int64
		bodies      map[string]int
		metrics     shieldMetrics
	)
	poll.Eventually(t, 30*time.Second, 500*time.Millisecond, func() bool {
		path = fmt.Sprintf("/api/v1/hit?x=origin-shield-%d", shieldRunSeq.Add(1))
		ok, delta, bs, ms := runShieldWave(t, s, path, perNode)
		originDelta, bodies, metrics = delta, bs, ms
		return ok
	})

	// The acceptance criterion: one origin request for the whole cluster.
	require.Equal(t, int64(1), originDelta,
		"cold key hit by %d concurrent requests across %d nodes must produce exactly 1 origin request",
		perNode*len(s.Nodes), len(s.Nodes))
	require.Len(t, bodies, 1, "all responses must come from the same origin fill")

	// The shield engaged: at least one forward was served by the owner
	// and at least one requester avoided origin. Exact counts are
	// timing-dependent (a backfilled object serves later requests
	// locally instead of forwarding).
	assert.GreaterOrEqual(t, metrics.owner, float64(1),
		"the owner must have served at least one shield forward")
	assert.GreaterOrEqual(t, metrics.waiter, float64(1),
		"at least one requester must have been served by the owner's fill")
	assert.Equal(t, metrics.waiter, metrics.saved,
		"origin-requests-saved must count every waiter served by the shield")

	// A replay wave must be fully served from cache: no new origin
	// request beyond the single wave fetch.
	afterWave := s.OriginRequests()
	for i := range s.Nodes {
		resp := s.GetWithHost(t, i, path, driver.CrossNodeHost)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	assert.Equal(t, afterWave, s.OriginRequests(),
		"replayed requests must not reach the origin again")
}

// TestStrong_OriginShield_FlagOffCostsOneOriginRequestPerNode pins
// the D9 rollout valve from the other side: with the flag off, the same
// cold-key wave costs one origin request per node — today's behavior,
// and the baseline the shield's saving is measured against.
func TestStrong_OriginShield_FlagOffCostsOneOriginRequestPerNode(t *testing.T) {
	s := sharedCluster(t, "strong")
	path := fmt.Sprintf("/api/v1/hit?x=origin-shield-off-%d", shieldRunSeq.Add(1))

	before := s.OriginRequests()
	type result struct {
		code int
		body string
	}
	results := make(chan result, len(s.Nodes))
	var wg sync.WaitGroup
	for i := range s.Nodes {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			resp := s.GetWithHost(t, n, path, driver.CrossNodeHost)
			results <- result{code: resp.StatusCode, body: string(resp.Body)}
		}(i)
	}
	wg.Wait()
	close(results)

	for r := range results {
		require.Equal(t, http.StatusOK, r.code)
	}
	// Each node misses locally, peer-fetches (owner has nothing), and
	// fetches origin itself: N origin requests for N nodes — exactly
	// the failure mode ADR-0052 exists to remove.
	assert.Equal(t, int64(len(s.Nodes)), s.OriginRequests()-before,
		"flag-off cold key must cost one origin request per node (today's behavior)")
}

// shieldMetrics snapshots the shield counters across the cluster
// after one wave.
type shieldMetrics struct {
	owner  float64
	waiter float64
	saved  float64
}

// runShieldWave fires perNode concurrent cold requests on every node
// for the same path and reports the wave's distinct bodies, the origin
// requests it consumed, and the shield-metric snapshot. ok is true
// only when the wave behaved as a converged shield: every request got
// a 200 from a single shared fill.
func runShieldWave(t *testing.T, s *driver.ClusterStack, path string, perNode int) (ok bool, originDelta int64, bodies map[string]int, metrics shieldMetrics) {
	t.Helper()
	before := s.OriginRequests()
	type result struct {
		code int
		body string
	}
	results := make(chan result, len(s.Nodes)*perNode)
	var wg sync.WaitGroup
	for i := range s.Nodes {
		for range perNode {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				resp := s.GetWithHost(t, n, path, driver.CrossNodeHost)
				results <- result{code: resp.StatusCode, body: string(resp.Body)}
			}(i)
		}
	}
	wg.Wait()
	close(results)

	bodies = map[string]int{}
	ok = true
	for r := range results {
		if r.code != http.StatusOK {
			ok = false
		}
		bodies[r.body]++
	}
	if !ok || len(bodies) != 1 {
		// Unconverged ring or partial failure: report and let the
		// caller retry on a fresh key.
		return false, s.OriginRequests() - before, bodies, metrics
	}
	for _, n := range s.Nodes {
		url := n.AdminAddr + "/metrics"
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI(url)
		if err := fasthttp.Do(req, resp); err != nil {
			t.Logf("metrics GET node %s: %v", n.Name, err)
			return false, s.OriginRequests() - before, bodies, metrics
		}
		metrics.owner += shieldMetricValue(resp.Body(), "bouine_shield_requests_total", `role="owner"`)
		metrics.waiter += shieldMetricValue(resp.Body(), "bouine_shield_requests_total", `role="waiter"`)
		metrics.saved += shieldMetricValue(resp.Body(), "bouine_shield_origin_requests_saved_total", "")
	}
	return true, s.OriginRequests() - before, bodies, metrics
}

// shieldMetricValue sums one metric series (filtered by labelSub,
// empty = all) from a /metrics body.
func shieldMetricValue(body []byte, metric, labelSub string) float64 {
	var total float64
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, metric+"{") && !strings.HasPrefix(line, metric+" ") {
			continue
		}
		if labelSub != "" && !strings.Contains(line, labelSub) {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(parts[len(parts)-1], 64)
		if err == nil {
			total += v
		}
	}
	return total
}

// TestStrong_OriginShield_HeadForwardNoPoison pins the cache-poisoning
// regression from the PR #731 review (blocker #1, ADR-0052): a HEAD
// shield forward must never leave a bodyless object under the GET
// canonical key. The HEAD goes through the shield (D8), the owner
// serves it through its standard miss path, and every subsequent GET
// — peer-fetched or backfilled, on any node — must carry the full
// origin body.
func TestStrong_OriginShield_HeadForwardNoPoison(t *testing.T) {
	s := sharedShieldCluster(t)
	path := fmt.Sprintf("/api/v1/hit?x=shield-head-%d", shieldRunSeq.Add(1))

	head := s.HeadWithHost(t, 0, path, driver.CrossNodeHost)
	require.Equal(t, http.StatusOK, head.StatusCode)
	assert.Empty(t, head.Body, "the HEAD answer itself must not carry a body")

	// The poison shape: the owner stored the HEAD answer (empty body,
	// status 200) as the cacheable object; every later GET cluster-wide
	// then serves the empty body until TTL expiry.
	for i := range s.Nodes {
		resp := s.GetWithHost(t, i, path, driver.CrossNodeHost)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.NotEmpty(t, resp.Body,
			"node %d: a GET after a HEAD shield forward must carry the full origin body — a bodyless object under the GET key is the #731 cache poison", i)
	}

	// And the GET fills stay consistent: a replayed GET on the first
	// node must still serve a full body from cache.
	resp := s.GetWithHost(t, 0, path, driver.CrossNodeHost)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, resp.Body, "replayed GET must still carry the full body")
}
