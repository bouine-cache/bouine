//go:build integration

package integration_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// sharedCookieValuesCluster boots (once) a dedicated strong-mode stack
// with the /ssr-values/ cookie-values-keyed route (issue #870). The
// dedicated stack keeps this suite's origin counters isolated from the
// plain strong-mode stack (the sharedFastPeerCluster precedent).
func sharedCookieValuesCluster(t *testing.T) *driver.ClusterStack {
	t.Helper()
	const key = "strong+cookievalues"
	clusterMu.Lock()
	defer clusterMu.Unlock()
	if s, ok := clusterStacks[key]; ok {
		return s
	}
	s := driver.BootCluster(t, driver.ClusterOptions{
		Mode:              "strong",
		NoAutoCleanup:     true,
		CookieValuesRoute: true,
	})
	clusterClean = append(clusterClean, s.Down)
	clusterStacks[key] = s
	return s
}

// TestStrong_CookieValuesClusterPartitioning pins the cluster-wide
// contract of cache.key.cookie_values (issue #870, ADR-0072): the same
// URL + the same locale value resolves to ONE variant across the whole
// 3-node strong cluster (owner local, peers via peer fetch), a
// different value resolves to its own variant with its own body, and
// unlisted cookies never fragment. The origin renders per-locale
// bodies, so a wrong-variant handoff is directly observable as a wrong
// body.
func TestStrong_CookieValuesClusterPartitioning(t *testing.T) {
	s := sharedCookieValuesCluster(t)
	path := fmt.Sprintf("/ssr-values/cookie-values?k=cv-%d", time.Now().UnixNano())
	// One Host for every node: the cache key includes it
	// (driver.CrossNodeHost — all three nodes derive the same key).
	host := driver.CrossNodeHost
	get := func(n int, cookie string) *driver.Response {
		t.Helper()
		hdrs := map[string]string{}
		if cookie != "" {
			hdrs["Cookie"] = cookie
		}
		return s.GetWithHostAndHeaders(t, n, path, host, hdrs)
	}

	// Prime the locale=en variant from node 0.
	first := get(0, "locale=en")
	require.Equal(t, 200, first.StatusCode)
	require.Equal(t, "MISS", first.Header.Get("X-Cache"))
	require.Equal(t, "render locale=en", string(first.Body))

	// Every other node resolves the SAME variant — the owner serves it
	// locally, non-owners via peer fetch — and unlisted cookies riding
	// on the request never fragment it.
	for _, n := range []int{1, 2} {
		resp := get(n, "locale=en; analytics=1; session=xyz")
		require.Equal(t, 200, resp.StatusCode, "node %d", n)
		assert.Equal(t, "HIT", resp.Header.Get("X-Cache"), "node %d: same value must HIT (local or peer)", n)
		assert.Equal(t, "render locale=en", string(resp.Body), "node %d: the en variant, never another value's body", n)
	}

	// A different value is its own variant: MISS with its own body on
	// the first ask, then HIT cluster-wide.
	fr := get(1, "locale=fr")
	require.Equal(t, "MISS", fr.Header.Get("X-Cache"))
	require.Equal(t, "render locale=fr", string(fr.Body))
	frAgain := get(2, "locale=fr; other=1")
	require.Equal(t, "HIT", frAgain.Header.Get("X-Cache"))
	require.Equal(t, "render locale=fr", string(frAgain.Body))

	// No cookie at all: the empty-value variant, its own body.
	anon := get(2, "")
	require.Equal(t, "MISS", anon.Header.Get("X-Cache"))
	require.Equal(t, "render locale=", string(anon.Body))
	anonAgain := get(0, "")
	require.Equal(t, "HIT", anonAgain.Header.Get("X-Cache"))
	require.Equal(t, "render locale=", string(anonAgain.Body))

	// A primary-key purge clears EVERY value variant of the URL.
	s.Purge(t, 0, "http://"+host+path)
	time.Sleep(500 * time.Millisecond) // gossip-purge propagation
	for _, locale := range []string{"en", "fr"} {
		resp := get(0, "locale="+locale)
		assert.Equal(t, "MISS", resp.Header.Get("X-Cache"),
			"locale=%s must re-fetch after the primary-key purge (all variants invalid)", locale)
	}
}
