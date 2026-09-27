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

// runSeq gives each invocation of the acceptance test a fresh cache
// key, so -count=N and re-runs on a shared stack still see a cold key.
var runSeq atomic.Int64

// TestStrong_OriginShieldCoalesce is the origin-shield acceptance test:
// N nodes cold-missing the same key at the same time must cost the
// origin exactly ONE request for the whole cluster — the mass-purge /
// deploy stampede the coalesced origin shield exists to kill.
//
// Every node requests the same path under the same Host header so all
// nodes derive the same cache key and the same ring owner. The origin
// body embeds a per-request timestamp, so identical bodies across
// nodes prove every response came from the single shared origin fill.
//
// The origin is slowed (100ms) so the whole burst lands inside the
// single in-progress owner flight. Without latency the local origin
// answers in ~50µs and a peer RPC landing in the microsecond gap
// between flight completion and its store landing would start a second
// flight — the same accepted race the plain miss path documents
// (bounded overshoot under adversarial timing, never wrong content).
func TestStrong_OriginShieldCoalesce(t *testing.T) {
	s := sharedCoalesceCluster(t)
	require.NoError(t, s.ScaleOriginLatency(100))
	t.Cleanup(func() { _ = s.ScaleOriginLatency(0) })

	// No blind convergence sleep: each attempt runs the full acceptance
	// wave on a FRESH key and only succeeds when the ring has converged
	// (exactly one origin request, one shared fill). A stale-ring
	// overshoot on attempt N — a waiter addressing a non-owner gets a
	// 404 and fetches origin itself — costs a second origin request for
	// THAT key only; the next attempt uses a new key and cannot
	// inherit the overshoot.
	const perNode = 3
	var (
		path        string
		originDelta int64
		bodies      map[string]int
		metrics     coalescedMetrics
	)
	poll.Eventually(t, 30*time.Second, 500*time.Millisecond, func() bool {
		path = fmt.Sprintf("/hit?x=origin-shield-coalesce-%d", runSeq.Add(1))
		ok, delta, bs, ms := runCoalesceWave(t, s, path, perNode)
		originDelta, bodies, metrics = delta, bs, ms
		return ok
	})

	// The acceptance criterion: one origin request for the whole cluster.
	require.Equal(t, int64(1), originDelta,
		"cold key hit by %d concurrent requests across %d nodes must produce exactly 1 origin request",
		perNode*len(s.Nodes), len(s.Nodes))
	require.Len(t, bodies, 1, "all responses must come from the same origin fill")

	// Coalescing engaged: at least one peer RPC was served by the
	// owner's flight and at least one waiter avoided origin. Exact
	// counts are timing-dependent (a waiter that receives a backfilled
	// object serves later requests locally instead of RPCing).
	assert.GreaterOrEqual(t, metrics.owner, float64(1),
		"owner must have served at least one coalesced peer RPC")
	assert.GreaterOrEqual(t, metrics.waiter, float64(1),
		"at least one waiter must have been served by the owner's flight")
	assert.Equal(t, metrics.waiter, metrics.saved,
		"origin-requests-saved must count every waiter that avoided origin")

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

// coalescedMetrics snapshots the coalesced-fetch counters across the
// cluster after one wave.
type coalescedMetrics struct {
	owner  float64
	waiter float64
	saved  float64
}

// runCoalesceWave fires perNode concurrent cold requests on every node
// for the same path and reports the wave's distinct bodies, the origin
// requests it consumed, and the coalesced-metric snapshot. ok is true
// only when the wave behaved as a converged origin shield: every
// request got a 200 from the single shared fill.
func runCoalesceWave(t *testing.T, s *driver.ClusterStack, path string, perNode int) (ok bool, originDelta int64, bodies map[string]int, metrics coalescedMetrics) {
	t.Helper()
	before := s.OriginRequests()
	type result struct {
		node int
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
				results <- result{node: n, code: resp.StatusCode, body: string(resp.Body)}
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
		metrics.owner += coalescedMetricValue(resp.Body(), "bouine_coalesced_fetch_total", `role="owner"`)
		metrics.waiter += coalescedMetricValue(resp.Body(), "bouine_coalesced_fetch_total", `role="waiter"`)
		metrics.saved += coalescedMetricValue(resp.Body(), "bouine_origin_requests_saved_total", "")
	}
	return true, s.OriginRequests() - before, bodies, metrics
}

// coalescedMetricValue sums one metric series (filtered by labelSub,
// empty = all) from a /metrics body.
func coalescedMetricValue(body []byte, metric, labelSub string) float64 {
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
