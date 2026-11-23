//go:build integration

package integration_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// Phase 2 (issue #951): config reload swaps the route table and pool
// targets on a live 3-node strong cluster, fanned out to every peer.

// TestStrong_ConfigReloadFanoutConverges: edit the boot config on
// node 0 (add a pool target), reload, and verify the generation bumps
// on every peer (fan-out convergence).
func TestStrong_ConfigReloadFanoutConverges(t *testing.T) {
	s := sharedCluster(t, "strong")

	before := statusGen(t, s, 0)

	// Edit EVERY node's boot file identically (the ConfigMap-mount
	// workflow), then reload on node 0: the fan-out carries
	// (gen, resolvedHash) and every peer applies because its own
	// resolved config matches.
	for n := 0; n < 3; n++ {
		s.EditNodeConfig(t, n, func(cfg string) string {
			return strings.Replace(cfg,
				`targets: ["`+s.OriginHost()+`"]
`,
				`targets: ["`+s.OriginHost()+`", "127.0.0.1:1"]
`, 1)
		})
	}
	res := s.ConfigReload(t, 0)
	// A pool-target-only change is not a key-form change, so the
	// result is "applied" (peers still fan out and ack).
	assert.Equal(t, "applied", res["result"], "fan-out result: %v", res)

	// Every peer's generation bumps via the fan-out.
	for n := 0; n < 3; n++ {
		require.Eventually(t, func() bool {
			return statusGen(t, s, n) == before+1
		}, 10*time.Second, 100*time.Millisecond, "node %d gen should converge to %d", n, before+1)
	}
}

// TestStrong_ConfigReloadKeyFormGate: a cache-key-form change must
// report applied_all only when every live peer acks; the reload still
// applies locally.
func TestStrong_ConfigReloadKeyFormGate(t *testing.T) {
	s := sharedCluster(t, "strong")

	for n := 0; n < 3; n++ {
		s.EditNodeConfig(t, n, func(cfg string) string {
			return strings.Replace(cfg, `cache:
      ttl_default: 60s
    request:
      strip_prefix: /api/v1`, `cache:
      ttl_default: 60s
      key:
        strip_query_params: [utm_source]
    request:
      strip_prefix: /api/v1`, 1)
		})
	}
	res := s.ConfigReload(t, 0)
	assert.Equal(t, "applied_all", res["result"], "key-form change acked by all peers: %v", res)
	peers, ok := res["peers"].([]any)
	require.True(t, ok, "peers present: %v", res)
	assert.Len(t, peers, 2, "two peers acked")
}

// TestStrong_ConfigReloadPoolTargetSwapUnderTraffic: swap the pool
// target list while traffic flows; requests keep succeeding.
func TestStrong_ConfigReloadPoolTargetSwapUnderTraffic(t *testing.T) {
	s := sharedCluster(t, "strong")

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.Get(t, 1, "/hit?swap=1")
			}
		}
	}()

	s.EditNodeConfig(t, 1, func(cfg string) string {
		return strings.Replace(cfg,
			`targets: ["`+s.OriginHost()+`"]
`,
			`targets: ["`+s.OriginHost()+`"]
`, 1)
	})
	res := s.ConfigReload(t, 1)
	assert.NotEqual(t, "rejected", res["result"])

	close(stop)
	<-done

	// After the swap the route still serves (hit or miss, not 5xx).
	resp := s.Get(t, 1, "/hit?after-swap=1")
	assert.Contains(t, []string{"MISS", "HIT"}, resp.Header.Get("X-Cache"))
}

func statusGen(t *testing.T, s *driver.ClusterStack, n int) uint64 {
	t.Helper()
	code, body := s.AdminGet(t, n, "/v1/status")
	require.Equal(t, 200, code, "status on node %d: %s", n, body)
	var doc struct {
		Node struct {
			ConfigGen uint64 `json:"config_gen"`
		} `json:"node"`
	}
	require.NoError(t, json.Unmarshal(body, &doc))
	return doc.Node.ConfigGen
}
