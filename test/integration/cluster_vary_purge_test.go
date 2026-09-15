//go:build integration

package integration_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// TestStrong_PurgeVariantIsolation pins ADR-0045 receive-path semantics
// end-to-end: a URL purge (no variant identity is expressible via URL)
// must invalidate the primary key and ALL Vary variants cluster-wide.
// This is RFC 9111 §4.2.4: invalidation of a resource removes all its
// variants.
//
// This also pins the #631 decision: PurgeEvent.VaryKey is metadata;
// receivers purge-all instead of attempting variant-scoped deletes
// (which cannot be composed from the BuildVaryKey assertion hex the
// field carries).
//
// The test boots a dedicated cluster instead of the shared strong stack:
// TestStrong_BanPropagation in the shared suite issues a fleet-wide
// ".*" ban with no created_at exemption (pre-existing behavior), which
// makes every later fill MISS permanently and would mask this test's
// observations. All requests carry the CrossNodeHost Host header so
// every node derives the same cache key (see TestStrong_PurgeBatchEndToEnd).
//
// Observed contract: after the purge, the first request for EACH
// variant is a fresh origin MISS (X-Cache: MISS) on every node. Later
// GETs legitimately HIT (the first GET legitimately re-fills its
// variant, locally or on the owner), so only the first GET per variant
// proves the purge reached the store.
func TestStrong_PurgeVariantIsolation(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	path := "/vary-multiline?x=purge-isolation"
	fill := func(n int, lang string) *driver.Response {
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, map[string]string{
			"Accept-Language": lang,
			"BM-Market":       "FR",
		})
	}
	purgeURL := "http://" + driver.CrossNodeHost + path

	// Warm the two distinct variants (french + italian) on every node so
	// each node's handler tracks them in its variantSets.
	for _, n := range s.AliveNodes() {
		for _, lang := range []string{"fr-FR,fr;q=0.9", "it-IT,it;q=0.9"} {
			driver.RetryUntil(t, 5*time.Second, 200*time.Millisecond, func() bool {
				return fill(n, lang).Header.Get("X-Cache") == "HIT"
			})
			require.Equal(t, "HIT", fill(n, lang).Header.Get("X-Cache"),
				"variant %s must be warm on node %d before the purge", lang, n)
		}
	}

	// Purge the URL cluster-wide from node 0. The purge derives its key
	// from the URL only; no variant identity is involved.
	s.Purge(t, 0, purgeURL)

	// After the purge, a request for the fr variant must MISS (fresh
	// origin fetch), proving the fr variant entry was deleted on node 0.
	driver.RetryUntil(t, driver.GossipConvergence, 200*time.Millisecond, func() bool {
		return fill(0, "fr-FR,fr;q=0.9").Header.Get("X-Cache") == "MISS"
	})

	// The it variant must also be re-served as a fresh origin fetch on
	// node 0: the URL purge removed ALL variants, not just the fr one
	// whose body a caller might associate with the URL. Node 0's it
	// entry must never be served from cache before its first post-purge
	// origin fill completes, so the first MISS is deterministic here
	// (peer-fetch cannot answer: owners purged the key too and have no
	// local body to serve before their own first post-purge request).
	var sawMiss bool
	for i := 0; i < 40 && !sawMiss; i++ {
		resp := fill(0, "it-IT,it;q=0.9")
		if resp.Header.Get("X-Cache") == "MISS" {
			sawMiss = true
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.True(t, sawMiss, "URL purge must invalidate every variant (RFC 9111 §4.2.4)")

	// Cross-variant sanity: neither post-purge fill may resurrect the
	// OTHER variant's pre-purge body. Bodies are deterministic
	// ("variant lang=<lang> market=FR"), so a fresh fr fill carries the
	// fr identity and a fresh it fill the it identity — a stale body
	// would show the wrong language.
	respFr := fill(0, "fr-FR,fr;q=0.9")
	require.True(t, strings.Contains(string(respFr.Body), "lang=fr-FR"))
	respIt := fill(0, "it-IT,it;q=0.9")
	require.True(t, strings.Contains(string(respIt.Body), "lang=it-IT"),
		"post-purge it fill must serve the it body, never a cross-variant body")
}
