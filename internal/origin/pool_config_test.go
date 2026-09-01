package origin

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPoolConfig_AppliedDefaults(t *testing.T) {
	t.Parallel()
	p := pool(t, "127.0.0.1:1")
	c := p.ResolvedClientConfig()
	require.Equal(t, defaultDialTimeout, c.DialTimeout)
	require.Equal(t, defaultKeepAlive, c.KeepAlive)
	require.Equal(t, defaultOriginMaxConnsPerHost, c.MaxConnsPerHost)
	require.Equal(t, DefaultResponseHeaderTimeout, c.ResponseHeaderTimeout)
	require.Equal(t, DefaultMaxIdleConnDuration, c.MaxIdleConnDuration)
}

func TestPoolConfig_CustomValuesOverrideDefaults(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{
		Name:                  "custom",
		Targets:               []string{"127.0.0.1:1"},
		Logger:                newDiscardLogger(),
		DialTimeout:           3 * time.Second,
		KeepAlive:             7 * time.Second,
		MaxConnsPerHost:       12,
		ResponseHeaderTimeout: 45 * time.Second,
		MaxIdleConnDuration:   60 * time.Second,
	})
	require.NoError(t, err)
	c := p.ResolvedClientConfig()
	assert.Equal(t, 3*time.Second, c.DialTimeout)
	assert.Equal(t, 7*time.Second, c.KeepAlive)
	assert.Equal(t, 12, c.MaxConnsPerHost)
	assert.Equal(t, 45*time.Second, c.ResponseHeaderTimeout)
	assert.Equal(t, 60*time.Second, c.MaxIdleConnDuration)
}

// TestPoolClient_SharedAcrossHandlerAndClient verifies FastHandler and
// FastClient share the pool-level client so connect.max_connections
// applies per pool host, not per route handler (previously each
// FastHandler/FastClient call built its own client).
func TestPoolClient_SharedBetweenHandlerAndClient(t *testing.T) {
	t.Parallel()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.Close)

	p := pool(t, s.Listener.Addr().String())
	require.NotNil(t, p.client, "client must be built at pool construction")
	before := p.client

	_ = p.FastHandler(0)
	require.Same(t, before, p.client, "FastHandler must not replace the shared client")

	fc := p.FastClient()
	require.Same(t, before, fc.client, "FastClient must reuse the shared client")
}

func TestPoolClient_UsesConfiguredSettings(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{
		Name:                  "timeouts",
		Targets:               []string{"127.0.0.1:1"},
		Logger:                newDiscardLogger(),
		ResponseHeaderTimeout: 1234 * time.Millisecond,
		MaxIdleConnDuration:   60 * time.Second,
	})
	require.NoError(t, err)
	c := p.ResolvedClientConfig()
	require.Equal(t, 1234*time.Millisecond, c.ResponseHeaderTimeout)
	require.Equal(t, 1234*time.Millisecond, p.client.ReadTimeout)
	require.Equal(t, 60*time.Second, p.client.MaxIdleConnDuration)
	// Header-name normalizing must stay enabled: the cache layer reads
	// origin responses with canonical Peek keys, and origins commonly
	// emit lowercase header names (see newOriginClient).
	assert.False(t, p.client.DisableHeaderNamesNormalizing)
}

// TestPool_Close_ClosesIdleConnections pins the lifecycle contract:
// Pool.Close must drain the shared client's idle connections so
// rolling restarts don't leak TIME_WAIT sockets on origins.
func TestPool_Close_ClosesIdleConnections(t *testing.T) {
	t.Parallel()
	p := pool(t, "127.0.0.1:1")
	require.NoError(t, p.Close(t.Context()))
}
