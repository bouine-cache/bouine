//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStripPrefix_OriginSeesStrippedPath is the issue #595 integration
// regression test: a proxied route with request.strip_prefix must send
// the stripped path to the origin, while the cache key keeps the
// original path (miss → hit round trip under the prefixed URL).
func TestStripPrefix_OriginSeesStrippedPath(t *testing.T) {
	s := sharedCluster(t, "strong")
	path := "/api/v1/echo?x=strip-integration"

	r := s.Get(t, 0, path)
	require.Equal(t, 200, r.StatusCode)
	// The origin's /echo endpoint echoes the request URI it received:
	// stripped path + forwarded query, no /api/v1 prefix.
	assert.Equal(t, "uri /echo?x=strip-integration", string(r.Body),
		"origin must receive the stripped path")

	// Second request must be a HIT under the ORIGINAL-path cache key.
	r = s.Get(t, 0, path)
	assert.Equal(t, "HIT", r.Header.Get("X-Cache"))
	assert.Equal(t, "uri /echo?x=strip-integration", string(r.Body),
		"cached body must come from the stripped-path origin response")
}
