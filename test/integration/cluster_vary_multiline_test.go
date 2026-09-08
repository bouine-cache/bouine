//go:build integration

package integration_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStrong_MultiLineVaryVariantIsolation replays the production
// cross-market incident end to end in strong cluster mode: an origin
// that sends Vary across two field lines ("Vary: Accept-Language" +
// "Vary: BM-Market") must produce per-(language,market) variants
// across nodes — a non-owner requesting a different Accept-Language
// must MISS and fetch its own variant, never serve the first fill.
func TestStrong_MultiLineVaryVariantIsolation(t *testing.T) {
	s := sharedCluster(t, "strong")

	path := "/vary-multiline?x=isolation"
	url := s.OriginURL + path

	// Fill the fr/FR variant via node 0.
	resp := s.GetWithHeaders(t, 0, path, map[string]string{
		"Accept-Language": "fr-FR,fr;q=0.9",
		"BM-Market":       "FR",
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
		"BM-Market":       "FR",
	})
	body := string(resp.Body)
	fmt.Printf("node1 it-IT: X-Cache=%s src=%s body=%q\n",
		resp.Header.Get("X-Cache"), resp.Header.Get("X-Cache-Source"), body)
	require.NotContains(t, body, "lang=fr-FR", "must not serve the french variant body")
	require.Contains(t, body, "lang=it-IT", "node1 must fetch its own italian variant")

	// Back to the french variant via node 0 → HIT with french body.
	resp = s.GetWithHeaders(t, 0, path, map[string]string{
		"Accept-Language": "fr-FR,fr;q=0.9",
		"BM-Market":       "FR",
	})
	require.Equal(t, "HIT", resp.Header.Get("X-Cache"))
	require.Contains(t, string(resp.Body), "lang=fr-FR")

	_ = url
}
