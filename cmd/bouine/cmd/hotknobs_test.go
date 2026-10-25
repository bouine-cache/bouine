package cmd

import (
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/storage"
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
	}, testSetsCounter())(t.Context(), admin.HotSetRequest{Key: "nope", Value: "1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown hot config key")
}

// TestHotSetFnAppliesAndAudits exercises the engine closure end to
// end: known key, applied result, normalized value.
func TestHotSetFnAppliesAndAudits(t *testing.T) {
	t.Parallel()
	e := &engine{logger: slog.New(slog.DiscardHandler)}
	store := &fakeStore{ttl: storage.DefaultBanTTL}
	res, err := e.hotSetFn(func() []hotKnob {
		return []hotKnob{banTTLKnob(0, store)}
	}, testSetsCounter())(t.Context(), admin.HotSetRequest{Key: "cluster.ban_ttl", Value: "3600"})
	require.NoError(t, err)
	assert.True(t, res.Applied)
	assert.Equal(t, time.Hour, store.ttl, "bare integers read as seconds")
	assert.Equal(t, "1h0m0s", res.NewValue)
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
