//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// TestPreserveHost_OriginSeesClientHost is the regression test for
// bouine presenting the pool target as the origin Host: a preserve_host
// pool must forward the client's own Host header to the origin, while a
// default pool keeps the historical behaviour of sending the pool
// target. The origin's /host-echo endpoint replies with the Host header
// it received on the wire.
func TestPreserveHost_OriginSeesClientHost(t *testing.T) {
	// preserve_host cluster: the client's Host must reach the origin.
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong", PreserveHost: true})
	t.Cleanup(s.Down)

	const path = "/host-echo"
	r := s.GetWithHost(t, 0, path, "www.example.com")
	require.Equal(t, 200, r.StatusCode)
	assert.Equal(t, "host www.example.com", string(r.Body),
		"preserve_host pools must forward the client's Host header")

	// A second request under a different Host must reach the origin too
	// (the /host-echo route is no-store, so nothing is served from cache).
	r = s.GetWithHost(t, 0, path, "www.example.de")
	require.Equal(t, 200, r.StatusCode)
	assert.Equal(t, "host www.example.de", string(r.Body),
		"each client Host must be forwarded per request")
}

// TestPreserveHost_DefaultPoolSendsPoolTarget pins the historical
// behaviour side by side: without preserve_host, the origin sees the
// pool target (the node's local origin address) as its Host.
func TestPreserveHost_DefaultPoolSendsPoolTarget(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})
	t.Cleanup(s.Down)

	originHost := s.OriginHost()
	r := s.GetWithHost(t, 0, "/host-echo", "www.example.com")
	require.Equal(t, 200, r.StatusCode)
	assert.Equal(t, "host "+originHost, string(r.Body),
		"default pools must keep sending the pool target as Host")
}

// refreshClusterOptions boots an eventual-mode cluster with the given
// preserve_host setting and refresh-before-expiry enabled on the default
// route (margin 50%, min-hits 1). Eventual mode: no owner ring, so the
// node that fills also registers the refresh entry and runs the
// background fetch locally — the refresh replay path is mode-independent
// (registry entry → origin-bound fetch → pool dial → origin).
func refreshClusterOptions(preserveHost bool) driver.ClusterOptions {
	return driver.ClusterOptions{
		Mode:         "eventual",
		PreserveHost: preserveHost,
		// TTL 6s (above the 5s minRefreshTTL floor), margin 50% → the
		// background refresh fires ~3s after the fill — one fetch per
		// TTL window, not a hot loop.
		Refresh: driver.RefreshOptions{Enabled: true, TTL: 6 * time.Second, MarginPercent: 50, MinHits: 1},
	}
}

// TestEventual_PreserveHostBackgroundRefreshCarriesClientHost is the
// regression test for the 2026-11-19 production incident (P3): the
// refresh registry dropped the client Host/TLS when registering
// the background refresh, so the replayed fetch carried an EMPTY Host and
// the pool dial's absolute-form URI rewrite made fasthttp refill it from
// the POOL TARGET — silently bypassing preserve_host on every background
// refresh. Origins that derive behaviour from the Host header (tenant
// selection) then answered for the internal vhost and 500'd.
//
// Eventual mode: no owner ring, so the node that fills also registers the
// refresh entry and runs the background fetch locally — the replay path
// (registry entry → origin-bound fetch → pool dial → origin) is
// mode-independent. The origin /host-refresh endpoint (max-age=6, ETag)
// fills the cache and answers the conditional refresh with 304;
// OriginHosts records the Host of every origin-bound fetch in arrival
// order.
func TestEventual_PreserveHostBackgroundRefreshCarriesClientHost(t *testing.T) {
	s := driver.BootCluster(t, refreshClusterOptions(true))
	t.Cleanup(s.Down)

	const path = "/host-refresh"
	const clientHost = "www.example.com"

	// Seed: the foreground MISS fills the cache (the origin sees the
	// client Host — pinned by TestPreserveHost_OriginSeesClientHost) and
	// registers the background refresh at TTL - margin (6s TTL, 50%
	// margin → ~3s after the fill).
	r := s.GetWithHost(t, 0, path, clientHost)
	require.Equal(t, 200, r.StatusCode)
	require.Equal(t, "host "+clientHost, string(r.Body))

	// Wait for the background refresh cycle: the conditional fetch must
	// reach the origin (304 → TTL refreshed in place).
	require.Eventually(t, func() bool {
		return len(s.OriginHosts(path)) >= 2
	}, 20*time.Second, 100*time.Millisecond,
		"the background refresh must fetch from the origin")

	// Every origin-bound fetch — the foreground fill AND the background
	// refresh — must carry the CLIENT Host, byte-for-byte what the
	// equivalent foreground fetch would send.
	for i, h := range s.OriginHosts(path) {
		require.Equal(t, clientHost, h,
			"origin-bound fetch #%d (foreground fill or background refresh) must carry the client Host on a preserve_host pool", i+1)
	}

	// The refresh answered 304: the object stays served as a HIT with
	// the original body.
	after := s.GetWithHost(t, 0, path, clientHost)
	require.Equal(t, 200, after.StatusCode)
	assert.Equal(t, "HIT", after.Header.Get("X-Cache"),
		"the background-refreshed object must stay a cache hit")
	assert.Equal(t, "host "+clientHost, string(after.Body))
}

// TestEventual_PreserveHostBackgroundRefreshDefaultPoolSendsPoolTarget
// pins the default-pool counterpart of the refresh fix: without
// preserve_host the background refresh arrives with the POOL TARGET as
// its Host — the same Host the equivalent foreground fetch sends (fasthttp
// overwrites a non-empty Host from the absolute-form URI when
// UseHostHeader is unset), so the fix changes nothing on default pools.
func TestEventual_PreserveHostBackgroundRefreshDefaultPoolSendsPoolTarget(t *testing.T) {
	s := driver.BootCluster(t, refreshClusterOptions(false))
	t.Cleanup(s.Down)

	const path = "/host-refresh"
	originHost := s.OriginHost()

	// Seed the fill (the origin sees the pool target — the behaviour
	// TestPreserveHost_DefaultPoolSendsPoolTarget pins on the foreground
	// path).
	r := s.GetWithHost(t, 0, path, "www.example.com")
	require.Equal(t, 200, r.StatusCode)
	require.Equal(t, "host "+originHost, string(r.Body))

	require.Eventually(t, func() bool {
		return len(s.OriginHosts(path)) >= 2
	}, 20*time.Second, 100*time.Millisecond,
		"the background refresh must fetch from the origin")

	for i, h := range s.OriginHosts(path) {
		require.Equal(t, originHost, h,
			"origin-bound fetch #%d on a default pool must carry the pool target as Host", i+1)
	}
}
