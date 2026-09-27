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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

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

	path := fmt.Sprintf("/hit?x=origin-shield-coalesce-%d", runSeq.Add(1))
	before := s.OriginRequests()

	// 3 concurrent cold requests per node, all nodes at once.
	const perNode = 3
	type result struct {
		node   int
		code   int
		body   string
		xcache string
		src    string
	}
	results := make(chan result, len(s.Nodes)*perNode)
	var wg sync.WaitGroup
	for i := range s.Nodes {
		for range perNode {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				resp := s.GetWithHost(t, n, path, driver.CrossNodeHost)
				results <- result{node: n, code: resp.StatusCode, body: string(resp.Body),
					xcache: resp.Header.Get("X-Cache"), src: resp.Header.Get("X-Cache-Source")}
			}(i)
		}
	}
	wg.Wait()
	close(results)

	bodies := map[string]int{}
	for r := range results {
		require.Equal(t, http.StatusOK, r.code, "node %d status", r.node)
		bodies[r.body]++
	}
	require.Len(t, bodies, 1,
		"all responses must come from the same origin fill, got: %v", bodies)

	// The acceptance criterion: one origin request for the whole cluster.
	require.Equal(t, int64(1), s.OriginRequests()-before,
		"cold key hit by %d concurrent requests across %d nodes must produce exactly 1 origin request",
		perNode*len(s.Nodes), len(s.Nodes))

	// Coalescing engaged: at least one peer RPC was served by the
	// owner's flight and at least one waiter avoided origin. Exact
	// counts are timing-dependent (a waiter that receives a backfilled
	// object serves later requests locally instead of RPCing).
	assert.GreaterOrEqual(t, coalescedMetricSum(t, s, "bouine_coalesced_fetch_total", `role="owner"`), float64(1),
		"owner must have served at least one coalesced peer RPC")
	assert.GreaterOrEqual(t, coalescedMetricSum(t, s, "bouine_coalesced_fetch_total", `role="waiter"`), float64(1),
		"at least one waiter must have been served by the owner's flight")
	assert.Equal(t, coalescedMetricSum(t, s, "bouine_coalesced_fetch_total", `role="waiter"`),
		coalescedMetricSum(t, s, "bouine_origin_requests_saved_total", ""),
		"origin-requests-saved must count every waiter that avoided origin")

	// A replay wave must be fully served from cache: still 1 origin
	// request total.
	for i := range s.Nodes {
		resp := s.GetWithHost(t, i, path, driver.CrossNodeHost)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	assert.Equal(t, int64(1), s.OriginRequests()-before,
		"replayed requests must not reach the origin again")
}

// coalescedMetricSum sums the values of metric across the cluster,
// keeping only series whose labels contain labelSub (empty = all).
func coalescedMetricSum(t *testing.T, s *driver.ClusterStack, metric, labelSub string) float64 {
	t.Helper()
	var total float64
	for i := range s.Nodes {
		url := s.Nodes[i].AdminAddr + "/metrics"
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI(url)
		if err := fasthttp.Do(req, resp); err != nil {
			t.Fatalf("metrics GET node %d: %v", i, err)
		}
		for _, line := range strings.Split(string(resp.Body()), "\n") {
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
	}
	return total
}
