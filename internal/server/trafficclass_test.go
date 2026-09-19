package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestTrafficClassifier_Classify is the classification table pin
// (ADR-0047): exact / suffix / prefix glob forms, port stripping,
// case-insensitivity, first-match precedence, and the unclassified
// fallthrough.
func TestTrafficClassifier_Classify(t *testing.T) {
	t.Parallel()
	c := NewTrafficClassifier([]TrafficClassSpec{
		{Name: "csr", Hosts: []string{"www.backmarket.fr", "www.backmarket.de"}},
		{Name: "ssr", Hosts: []string{"*.svc.cluster.local", "www.backmarket.*"}},
	})

	tests := []struct {
		host string
		want string
	}{
		{"www.backmarket.fr", "csr"},
		{"www.backmarket.de:443", "csr"},
		{"WWW.BACKMARKET.FR", "csr"},           // case-insensitive
		{"Www.BackMarket.Fr:8443", "csr"},      // mixed case + port
		{"api.svc.cluster.local", "ssr"},       // suffix match
		{"a.b.svc.cluster.local", "ssr"},       // deep suffix match
		{"svc.cluster.local", "unclassified"},  // suffix requires a subdomain
		{".svc.cluster.local", "unclassified"}, // bare suffix is not a host
		{"www.backmarket.it", "ssr"},           // prefix match via ".*"
		{"www.backmarket", "unclassified"},     // prefix needs the trailing dot
		{"www.backmarket.", "unclassified"},    // prefix requires a label after the dot ("www.backmarket.*" = any host starting with "www.backmarket.")
		{"other.example.com", "unclassified"},
		{"", "unclassified"},
		{"evil-host", "unclassified"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, c.Classify(tt.host), tt.host)
	}
}

// TestTrafficClassifier_FirstMatchWins pins declaration-order
// precedence: a host matching patterns in two classes lands in the
// first-declared one.
func TestTrafficClassifier_FirstMatchWins(t *testing.T) {
	t.Parallel()
	c := NewTrafficClassifier([]TrafficClassSpec{
		{Name: "first", Hosts: []string{"*.example.com"}},
		{Name: "second", Hosts: []string{"api.example.com", "api.other.com"}},
	})
	assert.Equal(t, "first", c.Classify("api.example.com"))
}

// TestTrafficClassifier_NilIsUnclassified pins the absent-config shape:
// a nil classifier (the deployment default) classifies everything as
// unclassified — the label value never changes shape, only multiplies.
func TestTrafficClassifier_NilIsUnclassified(t *testing.T) {
	t.Parallel()
	var c *TrafficClassifier
	assert.Equal(t, api.TrafficClassUnclassified, c.Classify("www.example.com"))
	assert.Equal(t, api.TrafficClassUnclassified, c.Classify(""))
	assert.Nil(t, c.ClassNames())
}

// TestTrafficClassifier_ClassNames pins the ClassNames contract the
// builder test relies on: declaration order, no reserved name.
func TestTrafficClassifier_ClassNames(t *testing.T) {
	t.Parallel()
	c := NewTrafficClassifier([]TrafficClassSpec{
		{Name: "ssr", Hosts: []string{"a"}},
		{Name: "csr", Hosts: []string{"b"}},
	})
	assert.Equal(t, []string{"ssr", "csr"}, c.ClassNames())
}

// TestRouter_SetsTrafficClassUserValue is the slow-path wiring proof:
// ServeRequest stamps the classifier's value under the
// XBouineTrafficClass UserValue — including on no-route 404s (Host is
// known before routing) — and the value is absent when no classifier
// is configured so the middleware fallback applies.
func TestRouter_SetsTrafficClassUserValue(t *testing.T) {
	t.Parallel()
	c := NewTrafficClassifier([]TrafficClassSpec{
		{Name: "csr", Hosts: []string{"www.example.com"}},
	})
	rt := NewRouter(RouterConfig{TrafficClassify: c})
	rt.AddRoute("", "/api/", "api", "app", nil, ok200("api"), nil)

	ctx := serveRoute(t, rt, "GET", "www.example.com", "/api/x")
	assert.Equal(t, "csr", ctx.UserValue(header.XBouineTrafficClass))

	// No-route 404 carries the class too.
	ctx = serveRoute(t, rt, "GET", "www.example.com", "/nope")
	assert.Equal(t, 404, ctx.Response.StatusCode())
	assert.Equal(t, "csr", ctx.UserValue(header.XBouineTrafficClass))

	// Non-matching host carries the fallback.
	ctx = serveRoute(t, rt, "GET", "other.example.com", "/api/x")
	assert.Equal(t, api.TrafficClassUnclassified, ctx.UserValue(header.XBouineTrafficClass))

	// Without a classifier the UserValue is absent (middleware
	// fallback), matching the pool-less route pattern.
	rtPlain := NewRouter(RouterConfig{})
	rtPlain.AddRoute("", "/api/", "api", "app", nil, ok200("api"), nil)
	ctx = serveRoute(t, rtPlain, "GET", "www.example.com", "/api/x")
	assert.Nil(t, ctx.UserValue(header.XBouineTrafficClass))
}

// TestRoutedFastPath_StampsTrafficClass pins the H1 fast-path wiring:
// the routed wrapper stamps the response's TrafficClass from the same
// classifier the router uses, so fast-path hits carry the same label
// the slow path would — and an empty value (no classifier) stays
// empty for the metrics hook's unclassified fallback.
func TestRoutedFastPath_StampsTrafficClass(t *testing.T) {
	t.Parallel()
	c := NewTrafficClassifier([]TrafficClassSpec{
		{Name: "ssr", Hosts: []string{"*.svc.cluster.local"}},
	})
	rt := NewRouter(RouterConfig{TrafficClassify: c})
	fp := &pooledBenchFP{pool: "api-pool"}
	rt.AddRoute("api.svc.cluster.local", "/v1/", "api", "api-pool", nil, ok200("api"), fp)
	rt.AddRoute("", "/v1/", "fallback", "fallback-pool", nil, ok200("f"), fp)
	rfp := NewRoutedFastPath(rt, fp)

	req := &api.RawRequest{Method: "GET", Path: "/v1/x", Host: "api.svc.cluster.local:443"}
	resp, ok := rfp.TryHit(req, time.Now())
	require.True(t, ok)
	require.NotNil(t, resp)
	assert.Equal(t, "ssr", resp.TrafficClass)
	rfp.Release(resp)

	// Non-matching host: the classifier's stable "unclassified" string
	// — a config-owned constant, same value the metrics fallback would
	// produce for an empty field.
	req.Host = "www.example.com"
	resp, ok = rfp.TryHit(req, time.Now())
	require.True(t, ok)
	assert.Equal(t, api.TrafficClassUnclassified, resp.TrafficClass)
	rfp.Release(resp)
}
