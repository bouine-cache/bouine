//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// TestStrong_EncodingBucketSharedVariant pins the cluster-level payoff of
// Accept-Encoding bucketing (docs/plans/accept-encoding-bucketing.md §5.2):
// two nodes whose clients spell the same negotiation differently must
// share one stored variant. Node A fills with the Chrome dialect, node B
// — a non-owner — must then peer-fetch HIT with its br-negotiating
// spelling, and the origin must see exactly the canonical token "br"
// on the single fill.
func TestStrong_EncodingBucketSharedVariant(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	path := "/vary?x=ae-bucket-shared"
	chromeDialect := "gzip, deflate, br"
	firefoxDialect := "br, gzip, deflate"

	get := func(n int, ae string) *driver.Response {
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, map[string]string{
			"Accept-Encoding": ae,
		})
	}

	// Warm the variant on node 0 with the Chrome spelling. The origin
	// echoes the AE it received, so the body proves which token the
	// origin saw on the fill.
	driver.RetryUntil(t, 10*time.Second, 200*time.Millisecond, func() bool {
		return get(0, chromeDialect).Header.Get("X-Cache") == "HIT"
	})
	resp := get(0, chromeDialect)
	require.Equal(t, "HIT", resp.Header.Get("X-Cache"))
	require.Contains(t, string(resp.Body), "enc=br",
		"origin must receive the canonical bucket token, not the raw dialect: %s", resp.Body)

	// Node 1 asks with a different spelling of the same negotiation.
	// Pre-bucketing this was a distinct variant: a local miss, a
	// peer-fetch rejection, and a second origin fill. With bucketing it
	// must peer-fetch HIT the br bucket node 0 filled.
	before := s.OriginRequests()
	respB := get(1, firefoxDialect)
	require.Equal(t, "HIT", respB.Header.Get("X-Cache"),
		"the br bucket filled by node 0 must serve node 1's dialect")
	require.Contains(t, string(respB.Body), "enc=br",
		"the shared variant is the br fill; a wrong-variant leak would show a different token")
	require.Equal(t, before, s.OriginRequests(),
		"sharing one bucket must not cost a second origin fill")
}

// TestStrong_EncodingBucketIdentityIsDistinctVariant pins the other side
// of the bucketing boundary: an identity request (no AE at all) is a
// different variant from the br bucket, so it must MISS and fill its own
// entry rather than reuse the compressed one.
func TestStrong_EncodingBucketIdentityIsDistinctVariant(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	path := "/vary?x=ae-bucket-identity"
	get := func(n int, ae string) *driver.Response {
		headers := map[string]string{}
		if ae != "" {
			headers["Accept-Encoding"] = ae
		}
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, headers)
	}

	// Fill the br bucket.
	driver.RetryUntil(t, 10*time.Second, 200*time.Millisecond, func() bool {
		return get(0, "gzip, deflate, br").Header.Get("X-Cache") == "HIT"
	})

	// The identity request is a distinct variant: its first request is
	// a MISS (locally, or via peer-fetch refusal — either way, not a
	// HIT of the br body).
	identity := get(0, "")
	require.NotEqual(t, "HIT", identity.Header.Get("X-Cache"),
		"identity and br buckets are distinct variants")
	require.NotContains(t, string(identity.Body), "enc=br",
		"the identity fill must not leak the br variant's body: %s", identity.Body)
}
