//go:build integration

package integration_test

import (
	"testing"

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
