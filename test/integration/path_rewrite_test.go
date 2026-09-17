//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPathRewrite_OriginSeesRewrittenPath is the nginx-migration
// integration regression test: a proxied route with
// request.path_rewrite must send the regex-rewritten path to the
// origin (query preserved), while the cache key keeps the original
// public path (miss → hit round trip under the public URL).
func TestPathRewrite_OriginSeesRewrittenPath(t *testing.T) {
	s := sharedCluster(t, "strong")
	path := "/payment/orchestrator/callback/echo?sig=integration"

	r := s.Get(t, 0, path)
	require.Equal(t, 200, r.StatusCode)
	// The integration cluster's route rewrites the public callback
	// prefix away, so the origin's /echo endpoint receives the rewritten
	// path and echoes it: rewritten path + forwarded query.
	assert.Equal(t, "uri /echo?sig=integration", string(r.Body),
		"origin must receive the rewritten path")

	// Second request must be a HIT under the ORIGINAL-path cache key.
	r = s.Get(t, 0, path)
	assert.Equal(t, "HIT", r.Header.Get("X-Cache"))
	assert.Equal(t, "uri /echo?sig=integration", string(r.Body),
		"cached body must come from the rewritten-path origin response")
}
