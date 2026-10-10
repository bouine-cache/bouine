//go:build integration

package integration_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// TestStrong_MultiLineVaryVariantIsolation replays the production
// cross-market incident end to end in strong cluster mode: an origin
// that sends Vary across two field lines ("Vary: Accept-Language" +
// "Vary: X-Region") must produce per-(language,market) variants
// across nodes — a non-owner requesting a different Accept-Language
// must MISS and fetch its own variant, never serve the first fill.
func TestStrong_MultiLineVaryVariantIsolation(t *testing.T) {
	s := sharedCluster(t, "strong")

	path := "/vary-multiline?x=isolation"

	// Fill the fr/FR variant via node 0.
	resp := s.GetWithHeaders(t, 0, path, map[string]string{
		"Accept-Language": "fr-FR,fr;q=0.9",
		"X-Region":        "FR",
	})
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "MISS", resp.Header.Get("X-Cache"))
	require.Contains(t, string(resp.Body), "lang=fr-FR")
	time.Sleep(300 * time.Millisecond)

	// Same URL, different language, on ANOTHER node → non-owner peer
	// path. Must be its own variant (MISS + italian body), never a
	// peer HIT carrying the french body.
	resp = s.GetWithHeaders(t, 1, path, map[string]string{
		"Accept-Language": "it-IT,it;q=0.9",
		"X-Region":        "FR",
	})
	body := string(resp.Body)
	fmt.Printf("node1 it-IT: X-Cache=%s src=%s body=%q\n",
		resp.Header.Get("X-Cache"), resp.Header.Get("X-Cache-Source"), body)
	require.NotContains(t, body, "lang=fr-FR", "must not serve the french variant body")
	require.Contains(t, body, "lang=it-IT", "node1 must fetch its own italian variant")

	// Back to the french variant via node 0 → HIT with french body.
	// The HIT may arrive via the fast-path/peer branch once the owner
	// holds the fr variant, but the write-to-owner RPC after node 0's
	// fill is fire-and-forget (builder.go PeerPut goroutine) — a fixed
	// sleep or a single GET raced it (the residual flake once the
	// suite-wide ".*" ban poisoning was removed). Poll instead: a GET
	// that misses re-fills the fr variant and re-puts it to the owner,
	// so the HIT converges; the body assertion holds on every
	// iteration because every fr response — fresh or stored — carries
	// the fr identity.
	driver.RetryUntil(t, 5*time.Second, 100*time.Millisecond, func() bool {
		resp = s.GetWithHeaders(t, 0, path, map[string]string{
			"Accept-Language": "fr-FR,fr;q=0.9",
			"X-Region":        "FR",
		})
		return resp.Header.Get("X-Cache") == "HIT" &&
			strings.Contains(string(resp.Body), "lang=fr-FR")
	})
}
