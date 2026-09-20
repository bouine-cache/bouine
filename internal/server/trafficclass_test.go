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

// TestTrafficClassifier_ShadowedPatterns pins the boot-time shadow
// analysis: a later class's pattern an earlier class fully shadows can
// never match (declaration order is precedence, ADR-0047) and must be
// reported so the dead config is not silent. Partial overlaps — where
// the later pattern still matches hosts the earlier one misses — are
// not findings.
func TestTrafficClassifier_ShadowedPatterns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		classes []TrafficClassSpec
		want    []string // substrings expected in the findings; nil = none
	}{
		{
			name: "no overlap is silent",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"www.backmarket.fr"}},
				{Name: "ssr", Hosts: []string{"*.svc.cluster.local"}},
			},
		},
		{
			name: "identical exact pattern in a later class",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"www.example.com"}},
				{Name: "ssr", Hosts: []string{"www.example.com", "api.example.com"}},
			},
			want: []string{`"www.example.com" in class "ssr"`, `"www.example.com" in class "csr"`},
		},
		{
			name: "exact host shadowed by an earlier suffix glob",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"*.example.com"}},
				{Name: "ssr", Hosts: []string{"api.example.com"}},
			},
			want: []string{`"api.example.com" in class "ssr"`, `"*.example.com" in class "csr"`},
		},
		{
			name: "exact host shadowed by an earlier prefix glob",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"www.example.*"}},
				{Name: "ssr", Hosts: []string{"www.example.fr"}},
			},
			want: []string{`"www.example.fr" in class "ssr"`},
		},
		{
			name: "exact host shadowed by a bare-star prefix (empty continuation)",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"www*"}},
				{Name: "ssr", Hosts: []string{"www"}},
			},
			want: []string{`"www" in class "ssr"`},
		},
		{
			name: "narrower suffix shadowed by an earlier wider suffix",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"*.example.com"}},
				{Name: "ssr", Hosts: []string{"*.sub.example.com"}},
			},
			want: []string{`"*.sub.example.com" in class "ssr"`},
		},
		{
			name: "identical suffix globs in a later class",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"*.example.com"}},
				{Name: "ssr", Hosts: []string{"*.example.com"}},
			},
			want: []string{`"*.example.com" in class "ssr"`},
		},
		{
			name: "narrower prefix shadowed by an earlier wider prefix",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"www.*"}},
				{Name: "ssr", Hosts: []string{"www.api.*"}},
			},
			want: []string{`"www.api.*" in class "ssr"`},
		},
		{
			name: "wider suffix after narrower is only a partial overlap",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"*.sub.example.com"}},
				{Name: "ssr", Hosts: []string{"*.example.com"}},
			},
		},
		{
			name: "suffix vs prefix globs only partially overlap",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"*.example.com"}},
				{Name: "ssr", Hosts: []string{"api.*"}},
			},
		},
		{
			name: "wider prefix after narrower is only a partial overlap",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"www.api.*"}},
				{Name: "ssr", Hosts: []string{"www.*"}},
			},
		},
		{
			name: "same-class shadowing changes nothing",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"*.example.com", "api.example.com"}},
			},
		},
		{
			name: "first class is never shadowed",
			classes: []TrafficClassSpec{
				{Name: "csr", Hosts: []string{"*.example.com", "api.example.com"}},
				{Name: "ssr", Hosts: []string{"*.other.net"}},
			},
		},
		{
			name: "distant shadowing: middle class does not lift the shadow",
			classes: []TrafficClassSpec{
				{Name: "a", Hosts: []string{"*.example.com"}},
				{Name: "b", Hosts: []string{"*.other.net"}},
				{Name: "c", Hosts: []string{"x.example.com"}},
			},
			want: []string{`"x.example.com" in class "c"`, `class "a"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := NewTrafficClassifier(tt.classes)
			if tt.want == nil {
				assert.Empty(t, c.ShadowedPatterns())
				return
			}
			got := c.ShadowedPatterns()
			require.Len(t, got, 1)
			for _, sub := range tt.want {
				assert.Contains(t, got[0], sub)
			}
		})
	}
}

// TestTrafficClassifier_ShadowedPatterns_NilAndEmpty pins the absent
// shapes: no configured classes yields a nil classifier and nil
// findings — boot logging can iterate unconditionally.
func TestTrafficClassifier_ShadowedPatterns_NilAndEmpty(t *testing.T) {
	t.Parallel()
	var nilC *TrafficClassifier
	assert.Nil(t, nilC.ShadowedPatterns())
	assert.Nil(t, NewTrafficClassifier(nil).ShadowedPatterns())
	assert.Nil(t, NewTrafficClassifier([]TrafficClassSpec{}).ShadowedPatterns())
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

// TestTrafficClassifier_BareStarCrossesLabels pins the raw-prefix
// semantics of a bare trailing `*` (ADR-0047 Decision 1): unlike the
// `.*` form it is not anchored to a label boundary, so it matches
// across labels (`www.backmarket-evil.example.com`) and the empty
// continuation (`www.backmarket` itself).
func TestTrafficClassifier_BareStarCrossesLabels(t *testing.T) {
	t.Parallel()
	c := NewTrafficClassifier([]TrafficClassSpec{
		{Name: "wide", Hosts: []string{"www.backmarket*"}},
	})
	assert.Equal(t, "wide", c.Classify("www.backmarket-evil.example.com"))
	assert.Equal(t, "wide", c.Classify("www.backmarket"))
	assert.Equal(t, "unclassified", c.Classify("shop.example.com"))
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
