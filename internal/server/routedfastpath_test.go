package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// recordingFP is a scripted api.FastPathHandler: TryHit returns a hit
// when the recorded path matches wantPath, and records the last
// (path) it was invoked with.
type recordingFP struct {
	name     string
	wantPath string
	calls    []string
	released []*api.FastPathResponse
}

func (r *recordingFP) TryHit(req *api.RawRequest, now time.Time) (*api.FastPathResponse, bool) {
	r.calls = append(r.calls, req.Path)
	if r.wantPath != "" && req.Path != r.wantPath {
		return nil, false
	}
	resp := &api.FastPathResponse{
		CacheResult: "HIT",
		Pool:        r.name,
		StatusCode:  200,
	}
	resp.BuffersArr[0] = []byte("HTTP/1.1 200 OK\r\n\r\n")
	resp.Buffers = resp.BuffersArr[:1]
	return resp, true
}

func (r *recordingFP) Release(resp *api.FastPathResponse) {
	r.released = append(r.released, resp)
}

func rawReq(method, path, host string) *api.RawRequest {
	return &api.RawRequest{Method: method, Path: path, Host: host, Scheme: "http"}
}

// TestRoutedFastPath_DelegatesByRoute pins the wrapper's core contract:
// the route table — including entries with no fast path — decides which
// handler TryHit reaches, using the router's first-match-wins
// host/prefix/methods semantics (issue #696: a store-level handler
// served hits for routes whose slow path could never match).
func TestRoutedFastPath_DelegatesByRoute(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	apiFP := &recordingFP{name: "api-pool"}
	rootFP := &recordingFP{name: "root-pool"}
	rt.AddRoute("", "/api/", "api", "api-pool", nil, ok200("api"), apiFP)
	rt.AddRoute("", "/static/", "static", "", nil, ok200("static"), nil)
	rt.AddRoute("", "/", "root", "root-pool", nil, ok200("root"), rootFP)

	rfp := NewRoutedFastPath(rt, apiFP)

	// /api/v1/x → the api handler, never root (first match wins).
	resp, ok := rfp.TryHit(rawReq("GET", "/api/v1/x", "example.com"), time.Now())
	require.True(t, ok)
	require.NotNil(t, resp)
	assert.Equal(t, "api-pool", resp.Pool)
	assert.Equal(t, []string{"/api/v1/x"}, apiFP.calls)
	assert.Empty(t, rootFP.calls)

	// /static/x matches a route with NO fast path: the wrapper must
	// decline (nil, false) BEFORE later routes are consulted — the
	// miss path runs the router, which reaches the same static
	// handler. Falling through to the / root route would serve the
	// wrong content.
	resp, ok = rfp.TryHit(rawReq("GET", "/static/x", "example.com"), time.Now())
	assert.False(t, ok)
	assert.Nil(t, resp)
	assert.Empty(t, rootFP.calls, "a nil-fp route must stop the route walk")

	// /other → the / root route's handler (the / route matches any
	// host and path).
	resp, ok = rfp.TryHit(rawReq("GET", "/other", "example.com"), time.Now())
	require.True(t, ok)
	assert.Equal(t, "root-pool", resp.Pool)

	// A host no route lists: only reachable fall-through is when every
	// route carries a host constraint.
	rtHostOnly := NewRouter(RouterConfig{})
	fp2 := &recordingFP{name: "p2"}
	rtHostOnly.AddRoute("only.example.com", "/", "only", "p2", nil, ok200("only"), fp2)
	rfp2 := NewRoutedFastPath(rtHostOnly, fp2)
	resp, ok = rfp2.TryHit(rawReq("GET", "/x", "other.example.com"), time.Now())
	assert.False(t, ok)
	assert.Nil(t, resp)
	assert.Empty(t, fp2.calls)
}

// TestRoutedFastPath_HostAndMethodsMatch pins that the wrapper applies
// the router's host and method gating before selecting a handler —
// a POST to a GET-only route must not consume a cache hit.
func TestRoutedFastPath_HostAndMethodsMatch(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	getFP := &recordingFP{name: "get-pool"}
	rt.AddRoute("api.example.com", "/v1", "api", "api-pool", []string{"GET"}, ok200("api"), getFP)

	rfp := NewRoutedFastPath(rt, getFP)

	// Matching host + method.
	_, ok := rfp.TryHit(rawReq("GET", "/v1/x", "api.example.com"), time.Now())
	require.True(t, ok)

	// Wrong host: no route match — decline even though the path matches.
	_, ok = rfp.TryHit(rawReq("GET", "/v1/x", "other.example.com"), time.Now())
	assert.False(t, ok)

	// Non-matching method: the fast path must decline (only GET/HEAD
	// requests can be hits; a POST must reach the slow path).
	_, ok = rfp.TryHit(rawReq("POST", "/v1/x", "api.example.com"), time.Now())
	assert.False(t, ok)

	// Host with a port suffix still matches (RFC 9110 §7.2 authority).
	_, ok = rfp.TryHit(rawReq("GET", "/v1/x", "api.example.com:8080"), time.Now())
	assert.True(t, ok)
}

// TestRoutedFastPath_ReleaseCrossInstance pins the release-safety
// property the parser and reactor depend on: the wrapper Releases
// through the handler injected at construction, which is not
// necessarily the producing route's handler — cache.FastPathHandler
// returns responses to global sync.Pools and ignores its receiver, so
// any instance is a valid target. The parser releases after serving,
// when the request buffer has been reused and the route can no longer
// be resolved.
func TestRoutedFastPath_ReleaseCrossInstance(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	a := &recordingFP{name: "a"}
	b := &recordingFP{name: "b"}
	rt.AddRoute("", "/a", "a", "pa", nil, ok200("a"), a)
	rt.AddRoute("", "/b", "b", "pb", nil, ok200("b"), b)

	// b is the release target: releases of a's responses must route
	// through it without panic and without re-resolving the route.
	rfp := NewRoutedFastPath(rt, b)

	respA, ok := rfp.TryHit(rawReq("GET", "/a", "example.com"), time.Now())
	require.True(t, ok)
	respB, ok := rfp.TryHit(rawReq("GET", "/b", "example.com"), time.Now())
	require.True(t, ok)

	rfp.Release(respA)
	rfp.Release(respB)
	assert.Len(t, b.released, 2)
	assert.Empty(t, a.released, "release never re-routes to the producing handler")
}

// TestStripHostPort pins the authority normalization shared with the
// router's ServeRequest/MatchByHostPath host handling.
func TestStripHostPort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, want string
	}{
		{"example.com", "example.com"},
		{"example.com:443", "example.com"},
		{"example.com:8080", "example.com"},
		{"127.0.0.1:9090", "127.0.0.1"},
		{"[::1]:9090", "[::1]"},
		{":8080", ":8080"},
		{"", ""},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, stripHostPort(tt.in), tt.in)
	}
}
