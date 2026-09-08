//go:build integration

package integration_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStrong_MultiLineVaryPeerResolverLeak replays the exact preprod
// flow: the FIRST request for a fresh URL lands on a non-owner (here:
// node 1). Its local lookup misses, so it peer-fetches the PRIMARY key
// with a blank assertion. The owner (whoever owns the primary) must
// never answer with the Vary resolver body — the requester must go to
// origin and fill its OWN variant. Repeat with a second market.
func TestStrong_MultiLineVaryPeerResolverLeak(t *testing.T) {
	s := sharedCluster(t, "strong")

	// 1. Fill the french variant from node 0 only. This stores the
	// variant under its key and the Vary resolver under the primary —
	// wherever the ring places them.
	path := "/vary-multiline?x=leak"
	resp := s.GetWithHeaders(t, 0, path, map[string]string{
		"Accept-Language": "fr-FR,fr;q=0.9",
		"BM-Market":       "FR",
	})
	require.Contains(t, string(resp.Body), "lang=fr-FR")

	// 2. Let peerPut deliver the resolver to the primary's owner.
	time.Sleep(1500 * time.Millisecond)

	// 3. Request the ITALIAN variant from EVERY node with the request
	// headers flipped per node, covering every fill-node/request-node
	// ownership crossing: whichever node is not owner(primary) takes the
	// peer-fetch path with a blank assertion. Pre-fix, the owner answered
	// such fetches with the resolver body (the first variant's content).
	for _, lang := range []string{"it-IT,it;q=0.9", "fr-FR,fr;q=0.9"} {
		for _, n := range s.AliveNodes() {
			resp := s.GetWithHeaders(t, n, path, map[string]string{
				"Accept-Language": lang,
				"BM-Market":       "FR",
			})
			body := string(resp.Body)
			want := "lang=" + strings.SplitN(lang, ",", 2)[0]
			t.Logf("[%s] node%d X-Cache=%s src=%s body=%q",
				lang, n, resp.Header.Get("X-Cache"), resp.Header.Get("X-Cache-Source"), body)
			require.Contains(t, body, want,
				"a cold request must be filled with its own variant, never a peer-carried foreign body")
		}
	}
}
