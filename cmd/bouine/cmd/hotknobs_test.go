package cmd

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

func testSetsCounter() *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_hot_sets"}, []string{"result"})
}

// counterValue reads one labelled counter through the standard
// collection path (prometheus/client_model is already a direct
// dependency, unlike client_golang's testutil subpackage which would
// pull kylelemons/godebug into the module graph).
func counterValue(t *testing.T, cv *prometheus.CounterVec, label string) float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 1)
	cv.WithLabelValues(label).Collect(ch)
	var d dto.Metric
	require.NoError(t, (<-ch).Write(&d))
	return d.GetCounter().GetValue()
}

// fakeStore exposes the ban TTL setter interface for knob wiring
// without a full store.
type fakeStore struct {
	ttl time.Duration
}

func (f *fakeStore) SetBanTTL(d time.Duration) { f.ttl = d }
func (f *fakeStore) BanTTL() time.Duration     { return f.ttl }

// TestBanTTLKnobSourceSemantics pins the three sources: a boot config
// value yields "file", a zero boot config yields "default" (the store
// runs the built-in 24h, which must NOT read as runtime drift), and a
// diverging live value yields "runtime".
func TestBanTTLKnobSourceSemantics(t *testing.T) {
	t.Parallel()
	store := &fakeStore{ttl: storage.DefaultBanTTL}
	knob := banTTLKnob(0, store)
	assert.Equal(t, "24h0m0s", knob.get())
	assert.Equal(t, "default", knob.source(knob.get()), "unset boot config is default, not drift")
	assert.Equal(t, "24h0m0s", knob.fileValue, "fileValue is what a restart restores")

	store = &fakeStore{ttl: 90 * time.Minute}
	knob = banTTLKnob(90*time.Minute, store)
	assert.Equal(t, "file", knob.source(knob.get()))

	knob = banTTLKnob(0, store)
	assert.Equal(t, "runtime", knob.source(knob.get()), "live override diverging from the boot default is drift")
}

// TestURLRingKnobSourceSemantics mirrors the ban TTL matrix for the
// integer knob: unset boot config is "default" at 0, an explicit file
// value is "file", divergence is "runtime".
func TestURLRingKnobSourceSemantics(t *testing.T) {
	t.Parallel()
	current := 0
	knob := urlRingSampleRateKnob(0, func() int { return current }, func(int) {})
	assert.Equal(t, "default", knob.source(knob.get()))

	current = 100
	knob = urlRingSampleRateKnob(100, func() int { return current }, func(int) {})
	assert.Equal(t, "file", knob.source(knob.get()))
	assert.Equal(t, "runtime", knob.source("0"))
}

// TestApplyHotKnob_DryRunDoesNotMutate is the regression test for the
// dry-run contract: validate and report, never touch live state.
func TestApplyHotKnob_DryRunDoesNotMutate(t *testing.T) {
	t.Parallel()
	current := 0
	knob := urlRingSampleRateKnob(0,
		func() int { return current },
		func(n int) { current = n })

	res, err := applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "5", DryRun: true}, testSetsCounter())
	require.NoError(t, err)
	assert.False(t, res.Applied)
	assert.Equal(t, "5", res.NewValue, "dry run still reports the normalized value")
	assert.Equal(t, 0, current, "dry run must not apply")
	assert.Equal(t, admin.HotSetWarning, res.Warning)
}

func TestApplyHotKnob_Applies(t *testing.T) {
	t.Parallel()
	current := 0
	knob := urlRingSampleRateKnob(0,
		func() int { return current },
		func(n int) { current = n })
	sets := testSetsCounter()

	res, err := applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "100"}, sets)
	require.NoError(t, err)
	assert.True(t, res.Applied)
	assert.Equal(t, 100, current)
	assert.Equal(t, "0", res.OldValue)
	assert.Equal(t, "100", res.NewValue)
	assert.InDelta(t, 1.0, counterValue(t, sets, "applied"), 0)
}

func TestApplyHotKnob_RejectedIncrementsCounter(t *testing.T) {
	t.Parallel()
	current := 0
	knob := urlRingSampleRateKnob(0,
		func() int { return current },
		func(n int) { current = n })
	sets := testSetsCounter()

	_, err := applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "-3"}, sets)
	require.Error(t, err)
	assert.Equal(t, 0, current)
	assert.InDelta(t, 1.0, counterValue(t, sets, "rejected"), 0)
}

// TestApplyHotKnob_DryRunCounter verifies the audit label for dry
// runs is distinct from applied and rejected.
func TestApplyHotKnob_DryRunCounter(t *testing.T) {
	t.Parallel()
	knob := urlRingSampleRateKnob(0, func() int { return 0 }, func(int) {})
	sets := testSetsCounter()

	_, err := applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "5", DryRun: true}, sets)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, counterValue(t, sets, "dry_run"), 0)
	assert.InDelta(t, 0.0, counterValue(t, sets, "applied"), 0)
}

// TestBanTTLKnob_StoreWithoutSetter verifies the capability error is
// reported identically for dry runs and real sets, so a dry run
// predicts the real outcome.
func TestBanTTLKnob_StoreWithoutSetter(t *testing.T) {
	t.Parallel()
	knob := banTTLKnob(0, struct{}{})
	_, err := applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "1h", DryRun: true}, testSetsCounter())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not hot-appliable")

	_, err = applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "1h"}, testSetsCounter())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not hot-appliable")
}

func TestHotSetFnUnknownKey(t *testing.T) {
	t.Parallel()
	e := &engine{logger: slog.New(slog.DiscardHandler)}
	_, err := e.hotSetFn(func() []hotKnob {
		return []hotKnob{banTTLKnob(0, &fakeStore{})}
	}, testSetsCounter(), nil)(t.Context(), admin.HotSetRequest{Key: "nope", Value: "1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown hot config key")

	_, err = e.hotSetFn(func() []hotKnob { return nil },
		testSetsCounter(), nil)(t.Context(), admin.HotSetRequest{Key: "route:api.ttl"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not hot-appliable", "route-scoped keys get the cold-only explanation")
}

// TestHotSetFnAppliesAndAudits exercises the engine closure end to
// end: known key, applied result, normalized value.
func TestHotSetFnAppliesAndAudits(t *testing.T) {
	t.Parallel()
	e := &engine{logger: slog.New(slog.DiscardHandler)}
	store := &fakeStore{ttl: storage.DefaultBanTTL}
	res, err := e.hotSetFn(func() []hotKnob {
		return []hotKnob{banTTLKnob(0, store)}
	}, testSetsCounter(), nil)(t.Context(), admin.HotSetRequest{Key: "cluster.ban_ttl", Value: "3600"})
	require.NoError(t, err)
	assert.True(t, res.Applied)
	assert.Equal(t, time.Hour, store.ttl, "bare integers read as seconds")
	assert.Equal(t, "1h0m0s", res.NewValue)
	assert.Equal(t, 0, res.Peers, "no broadcaster means no fan-out")
}

// TestHotSetFnBroadcastsAfterApply pins the fan-out contract: an
// applied set propagates the key and raw value to the broadcast hook
// and reports the acknowledged peer count; a dry run never does.
func TestHotSetFnBroadcastsAfterApply(t *testing.T) {
	t.Parallel()
	e := &engine{logger: slog.New(slog.DiscardHandler)}
	current := 0
	knobs := func() []hotKnob {
		return []hotKnob{urlRingSampleRateKnob(0,
			func() int { return current },
			func(n int) { current = n })}
	}
	var gotKey, gotValue string
	broadcast := func(_ context.Context, key, value string) int {
		gotKey, gotValue = key, value
		return 2
	}

	res, err := e.hotSetFn(knobs, testSetsCounter(), broadcast)(
		t.Context(), admin.HotSetRequest{Key: "url_ring_sample_rate", Value: "50"})
	require.NoError(t, err)
	assert.Equal(t, "url_ring_sample_rate", gotKey)
	assert.Equal(t, "50", gotValue)
	assert.Equal(t, 2, res.Peers, "the acknowledged count reaches the wire result")

	gotKey = ""
	res, err = e.hotSetFn(knobs, testSetsCounter(), broadcast)(
		t.Context(), admin.HotSetRequest{Key: "url_ring_sample_rate", Value: "7", DryRun: true})
	require.NoError(t, err)
	assert.Empty(t, gotKey, "dry runs must not fan out")
	assert.Equal(t, 0, res.Peers)
}

func TestParseHotDuration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{in: "90m", want: 90 * time.Minute, ok: true},
		{in: "1h30m", want: 90 * time.Minute, ok: true},
		{in: "3600", want: time.Hour, ok: true},
		{in: "0", want: 0, ok: true},
		{in: "-5", ok: false},
		{in: "-90m", ok: false},
		{in: "bogus", ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := parseHotDuration(tc.in)
			if !tc.ok {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRenderHotKnobs(t *testing.T) {
	t.Parallel()
	out := renderHotKnobs([]bouineapi.HotKnob{
		{Key: "cluster.ban_ttl", Value: "24h0m0s", Source: "default"},
		{Key: "url_ring_sample_rate", Value: "100", Source: "runtime"},
	})
	require.Contains(t, out, "KEY")
	require.Contains(t, out, "VALUE")
	require.Contains(t, out, "SOURCE")
	assert.Contains(t, out, "cluster.ban_ttl")
	assert.Contains(t, out, "url_ring_sample_rate")
}

func TestRenderHotSet(t *testing.T) {
	t.Parallel()
	applied := renderHotSet(&bouineapi.HotSetResult{
		Key: "cluster.ban_ttl", OldValue: "24h0m0s", NewValue: "1h30m0s",
		Applied: true, Warning: admin.HotSetWarning,
	})
	assert.Contains(t, applied, "cluster.ban_ttl: 24h0m0s -> 1h30m0s")
	assert.Contains(t, applied, "warning: not persisted")

	dry := renderHotSet(&bouineapi.HotSetResult{
		Key: "cluster.ban_ttl", OldValue: "24h0m0s", NewValue: "1h30m0s", Applied: false,
	})
	assert.Contains(t, dry, "dry-run: would set cluster.ban_ttl: 24h0m0s -> 1h30m0s (not applied)")
}

// testPool builds a real origin.Pool for knob wiring tests.
func testPool(t *testing.T, name string, consecutive5xx int, ejectFor time.Duration) *origin.Pool {
	t.Helper()
	p, err := origin.NewPool(origin.PoolConfig{
		Name:           name,
		Targets:        []string{"127.0.0.1:18080"},
		Consecutive5xx: consecutive5xx,
		EjectFor:       ejectFor,
	})
	require.NoError(t, err)
	return p
}

// TestPoolKnobsSortedAndKeyed pins the per-pool scoping contract:
// every pool contributes its two health knobs under pool:<name>.
// keys, sorted by pool name for a deterministic GET document.
func TestPoolKnobsSortedAndKeyed(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.UpstreamPools = []config.UpstreamPool{
		{Name: "www", Health: config.HealthPolicy{Passive: config.PassiveHealthCheck{Consecutive5xx: 3}}},
		{Name: "api", Health: config.HealthPolicy{Passive: config.PassiveHealthCheck{EjectFor: time.Minute}}},
	}
	pools := map[string]*origin.Pool{
		"api": testPool(t, "api", 0, time.Minute),
		"www": testPool(t, "www", 3, 0),
	}

	knobs := poolKnobs(&cfg, &cfg, pools)
	require.Len(t, knobs, 4)
	keys := []string{knobs[0].key, knobs[1].key, knobs[2].key, knobs[3].key}
	assert.Equal(t, []string{
		"pool:api.health.passive.consecutive_5xx",
		"pool:api.health.passive.eject_for",
		"pool:www.health.passive.consecutive_5xx",
		"pool:www.health.passive.eject_for",
	}, keys, "sorted by pool name, consecutive_5xx before eject_for")

	apiThreshold := knobs[0]
	assert.Equal(t, "0", apiThreshold.get())
	assert.Equal(t, "default", apiThreshold.source(apiThreshold.get()))
	assert.Equal(t, "file", knobs[2].source(knobs[2].get()), "www's boot-config 3 reads as file")
	assert.Equal(t, "file", knobs[1].source(knobs[1].get()), "api's boot-config 1m reads as file")
}

// TestPoolConsecutive5xxKnob covers the live swap: applied sets reach
// the pool atomics, dry runs do not mutate, and the source flips to
// runtime on drift.
func TestPoolConsecutive5xxKnob(t *testing.T) {
	t.Parallel()
	p := testPool(t, "api", 5, 0)
	knob := poolConsecutive5xxKnob("api", 5, p)
	assert.Equal(t, "file", knob.source(knob.get()))

	res, err := applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "9", DryRun: true}, testSetsCounter())
	require.NoError(t, err)
	assert.False(t, res.Applied)
	assert.Equal(t, 5, p.Consecutive5xx(), "dry run must not mutate")

	res, err = applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "9"}, testSetsCounter())
	require.NoError(t, err)
	assert.True(t, res.Applied)
	assert.Equal(t, "9", res.NewValue)
	assert.Equal(t, 9, p.Consecutive5xx())
	assert.Equal(t, "runtime", knob.source(knob.get()))

	_, err = applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "-1"}, testSetsCounter())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be >= 0")
}

// TestPoolEjectForKnob pins the cold-start boundary: eject_for is
// hot-changeable only when the boot config started the reaper — an
// explicit eject_for, or passive health enabled at boot (the unset
// window then resolves to the built-in default, which runs the
// reaper). Both dry runs and real sets on a reaper-less pool fail
// with the same precise error, so a dry run predicts reality.
func TestPoolEjectForKnob(t *testing.T) {
	t.Parallel()
	p := testPool(t, "api", 0, 0)
	knob := poolEjectForKnob("api", 0, 0, 0, p)
	assert.Equal(t, "default", knob.source(knob.get()))

	_, err := applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "5m", DryRun: true}, testSetsCounter())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reaper is not running")

	_, err = applyHotKnob(knob, admin.HotSetRequest{Key: knob.key, Value: "5m"}, testSetsCounter())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "set eject_for in your YAML and restart")
	assert.Equal(t, time.Duration(0), p.EjectFor(), "rejected sets never mutate")

	// Boot-configured reaper: the knob is fully live.
	p2 := testPool(t, "api", 0, time.Hour)
	knob2 := poolEjectForKnob("api", time.Hour, time.Hour, 0, p2)
	assert.Equal(t, "file", knob2.source(knob2.get()))
	_, err = applyHotKnob(knob2, admin.HotSetRequest{Key: knob2.key, Value: "90s", DryRun: true}, testSetsCounter())
	require.NoError(t, err)
	assert.Equal(t, time.Hour, p2.EjectFor(), "dry run must not mutate")
	res, err := applyHotKnob(knob2, admin.HotSetRequest{Key: knob2.key, Value: "90s"}, testSetsCounter())
	require.NoError(t, err)
	assert.True(t, res.Applied)
	assert.Equal(t, "1m30s", res.NewValue)
	assert.Equal(t, 90*time.Second, p2.EjectFor())

	// Passive health at boot with the window unset: the pool resolved
	// the zero to the built-in 30s default, so the reaper runs and the
	// knob is live even though the boot file value is 0.
	p3 := testPool(t, "api", 10, 0)
	require.Equal(t, 30*time.Second, p3.EjectFor())
	knob3 := poolEjectForKnob("api", 0, 0, 10, p3)
	res, err = applyHotKnob(knob3, admin.HotSetRequest{Key: knob3.key, Value: "45s"}, testSetsCounter())
	require.NoError(t, err)
	assert.True(t, res.Applied)
	assert.Equal(t, 45*time.Second, p3.EjectFor())
}

// TestClusterDisabledReason pins the three admin-surface states:
// live cluster (empty), no listener (how to enable), failed init.
func TestClusterDisabledReason(t *testing.T) {
	t.Parallel()
	defaults := config.Defaults()
	e := &engine{cfg: &defaults}
	rs := &runState{peersFn: func() []api.PeerInfo { return nil }}
	assert.Empty(t, e.clusterDisabledReason(rs), "live cluster explains nothing")

	rs = &runState{}
	assert.Contains(t, e.clusterDisabledReason(rs), "set listen.cluster")

	e.cfg.Listen.Cluster = "127.0.0.1:7946"
	assert.Contains(t, e.clusterDisabledReason(rs), "cluster init failed")
}
