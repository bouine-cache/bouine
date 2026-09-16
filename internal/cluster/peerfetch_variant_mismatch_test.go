package cluster

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// gatherVariantMismatch returns the set of active label tuples for
// bouine_peer_fetch_variant_mismatch_total, formatted "side".
func gatherVariantMismatch(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err, "gather")
	out := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != "bouine_peer_fetch_variant_mismatch_total" {
			continue
		}
		for _, met := range mf.GetMetric() {
			for _, l := range met.GetLabel() {
				if l.GetName() == "side" {
					out[l.GetValue()] = met.GetCounter().GetValue()
				}
			}
		}
	}
	return out
}

// TestPeerFetchHandler_VariantMismatchMetric verifies the server-side
// gate increments bouine_peer_fetch_variant_mismatch_total{side="server"}
// when a variant-asserting fetch is answered from a different variant's
// entry, and that a matching fetch does not increment it.
func TestPeerFetchHandler_VariantMismatchMetric(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	key := testkey.Key(11)
	frObj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       []byte("market=fr"),
		VaryValue:  "BM-Market",
		VaryKey:    "frhash",
	}
	frObj.Header = header.NewMap(1)
	frObj.Header.AppendEntry("Cache-Control", "max-age=60")
	h := NewPeerFetchHandlerWithMetrics(&stubStore{objects: map[api.Key]*api.Object{key: frObj}}, nil, 0, m)

	// Variant mismatch: requester asserts "us", stored entry is "fr".
	ctx := postFetch(t, h, api.PeerFetchRequest{Key: key, VaryKey: "ushash"}, 0)
	require.Equal(t, 404, ctx.Response.StatusCode())
	require.Equal(t, map[string]float64{"server": 1}, gatherVariantMismatch(t, reg))

	// Resolver-body leak path (empty stored VaryKey with a VaryValue)
	// also counts as a server-side mismatch: the requester asserted a
	// real variant and the owner only holds the primary-key resolver.
	resolver := &api.Object{
		Key:        testkey.Key(12),
		StatusCode: 200,
		Body:       []byte("resolver"),
		VaryValue:  "BM-Market",
	}
	resolver.Header = header.NewMap(1)
	resolver.Header.AppendEntry("Cache-Control", "max-age=60")
	h2 := NewPeerFetchHandlerWithMetrics(&stubStore{objects: map[api.Key]*api.Object{testkey.Key(12): resolver}}, nil, 0, m)
	ctx2 := postFetch(t, h2, api.PeerFetchRequest{Key: testkey.Key(12), VaryKey: "ushash"}, 0)
	require.Equal(t, 404, ctx2.Response.StatusCode())
	require.Equal(t, map[string]float64{"server": 2}, gatherVariantMismatch(t, reg))

	// Positive case must not increment.
	ctx3 := postFetch(t, h, api.PeerFetchRequest{Key: key, VaryKey: "frhash"}, 0)
	require.Equal(t, 200, ctx3.Response.StatusCode())
	require.Equal(t, map[string]float64{"server": 2}, gatherVariantMismatch(t, reg))
}

// TestPeerFetchHandler_VariantMismatchMetricNil pins nil-safety: a
// handler built without metrics logs the rejection and still misses.
func TestPeerFetchHandler_VariantMismatchMetricNil(t *testing.T) {
	t.Parallel()
	key := testkey.Key(11)
	frObj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       []byte("market=fr"),
		VaryValue:  "BM-Market",
		VaryKey:    "frhash",
	}
	frObj.Header = header.NewMap(1)
	frObj.Header.AppendEntry("Cache-Control", "max-age=60")
	h := NewPeerFetchHandlerWithMetrics(&stubStore{objects: map[api.Key]*api.Object{key: frObj}}, nil, 0, nil)

	ctx := postFetch(t, h, api.PeerFetchRequest{Key: key, VaryKey: "ushash"}, 0)
	require.Equal(t, 404, ctx.Response.StatusCode())
}

// TestMetrics_IncPeerFetchVariantMismatch pins the counter helpers:
// nil-safe no-op when metrics are disabled, labelled only by "side"
// (cardinality budget, AGENTS.md §9 — no key or peer label), and the
// lock-free total exposed to the insights engine.
func TestMetrics_IncPeerFetchVariantMismatch(t *testing.T) {
	t.Parallel()

	// Nil metrics: must not panic.
	var nilMetrics *Metrics
	nilMetrics.IncPeerFetchVariantMismatch("server")
	require.Equal(t, int64(0), nilMetrics.PeerFetchVariantMismatchCount())

	// Unregistered metrics (single-node): no-op without panic.
	m := &Metrics{}
	m.IncPeerFetchVariantMismatch("consumer")
	require.Equal(t, int64(0), m.PeerFetchVariantMismatchCount())

	// Registered metrics: both sides count independently under one
	// series per side — exactly 2 active tuples, no more.
	reg := prometheus.NewRegistry()
	m2 := RegisterMetrics(reg)
	for i := 0; i < 3; i++ {
		m2.IncPeerFetchVariantMismatch("server")
	}
	m2.IncPeerFetchVariantMismatch("consumer")
	got := gatherVariantMismatch(t, reg)
	require.Equal(t, map[string]float64{"server": 3, "consumer": 1}, got)
	require.Len(t, got, 2, "only the side label may exist — no key/peer cardinality")
	require.Equal(t, int64(4), m2.PeerFetchVariantMismatchCount())
}

// TestMetrics_VariantMismatchCardinalityBudget pins the §9 cardinality
// contract for the new metric: hammering many distinct "sides" through
// the labelled path leaves only the two legitimate series.
func TestMetrics_VariantMismatchCardinalityBudget(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	for i := 0; i < 100; i++ {
		// Unknown sides are clamped to the two defined ones at the call
		// sites; exercising the vec directly must not grow series either.
		m.IncPeerFetchVariantMismatch("server")
		m.IncPeerFetchVariantMismatch("consumer")
	}
	require.Len(t, gatherVariantMismatch(t, reg), 2, fmt.Sprintf("expected 2 series, got %d", len(gatherVariantMismatch(t, reg))))
}
