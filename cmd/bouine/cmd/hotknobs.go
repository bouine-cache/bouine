package cmd

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/storage"
)

// hotKnob is one runtime-tunable config knob (#815). get and set are
// engine-owned closures over the running components; set validates,
// normalizes, and applies atomically, returning the normalized value.
//
//nolint:govet // fieldalignment: admin-plane type, not on any hot path
type hotKnob struct {
	key         string
	description string
	// def is the knob's built-in default, the value the boot config
	// falls back to when it leaves the knob at zero.
	def string
	// fileValue is the effective boot value: what a restart restores.
	// For a knob the file leaves at zero this is the built-in default,
	// not "0", so drift rows read truthfully ("back to 24h0m0s").
	fileValue string
	// fromFile reports whether the boot config set the knob
	// explicitly; false means fileValue comes from defaults.
	fromFile bool
	get      func() string
	// set validates and normalizes the value, applying it only when
	// apply is true. With apply false (bouine config set --dry-run)
	// it must not mutate any live state, so a dry run predicts the
	// real outcome, including capability errors.
	set func(value string, apply bool) (string, error)
}

// source reports where the running value comes from: "file" when it
// matches an explicit boot-config value, "default" when it matches
// the effective value the file left to defaults, "runtime" when it
// drifted (the surface `bouine config diff` reports).
func (k hotKnob) source(value string) string {
	if value == k.fileValue {
		if k.fromFile {
			return "file"
		}
		return "default"
	}
	return "runtime"
}

// hotKnobsFn builds the GET /v1/config/hot document: every knob with
// its live value, source, and the boot-config value it will return to
// at restart.
func (e *engine) hotKnobsFn(knobs func() []hotKnob) func() []admin.HotKnob {
	return func() []admin.HotKnob {
		list := knobs()
		out := make([]admin.HotKnob, 0, len(list))
		for _, k := range list {
			value := k.get()
			out = append(out, admin.HotKnob{
				Key:         k.key,
				Value:       value,
				Source:      k.source(value),
				FileValue:   k.fileValue,
				Default:     k.def,
				Description: k.description,
			})
		}
		return out
	}
}

// hotApplyLocal finds req's knob and applies it through the registry
// (dry runs validate and normalize only). It never fans out to peers:
// the admin surface and the peer route share this core, and only the
// admin surface propagates — that asymmetry is what makes the
// fan-out loop-free. issuer names the originating node in the audit
// log (empty for local admin requests).
func (e *engine) hotApplyLocal(knobs func() []hotKnob, sets *prometheus.CounterVec, req admin.HotSetRequest, issuer string) (admin.HotSetResult, error) {
	for _, k := range knobs() {
		if k.key != req.Key {
			continue
		}
		res, err := applyHotKnob(k, req, sets)
		if err != nil {
			return admin.HotSetResult{}, err
		}
		e.logger.Info("hot config set",
			"key", req.Key,
			"old", res.OldValue,
			"new", res.NewValue,
			"dry_run", req.DryRun,
			"issuer", issuer)
		return res, nil
	}
	return admin.HotSetResult{}, unknownHotKeyError(req.Key)
}

// unknownHotKeyError explains why a key is not in the hot registry,
// including the cold-only per-route namespace so operators get the
// fix, not just the refusal.
func unknownHotKeyError(key string) error {
	if strings.HasPrefix(key, "route:") {
		return fmt.Errorf("per-route values are not hot-appliable: route tables are built at startup; set the route in your YAML and redeploy")
	}
	return fmt.Errorf("unknown hot config key %q; run 'bouine config get' to list keys", key)
}

// hotSetFn is the admin POST /v1/config/hot applier: one local
// application, then a synchronous cluster fan-out so the fleet's
// runtime overrides stay identical (issue #815 follow-up). Dry runs
// never fan out. broadcast is nil on a single-node engine; the result
// carries how many peers acknowledged.
func (e *engine) hotSetFn(knobs func() []hotKnob, sets *prometheus.CounterVec, broadcast func(context.Context, string, string) int) func(context.Context, admin.HotSetRequest) (admin.HotSetResult, error) {
	return func(ctx context.Context, req admin.HotSetRequest) (admin.HotSetResult, error) {
		res, err := e.hotApplyLocal(knobs, sets, req, "")
		if err != nil || req.DryRun || broadcast == nil {
			return res, err
		}
		res.Peers = broadcast(ctx, req.Key, req.Value)
		return res, nil
	}
}

// hotBroadcastFn adapts the cluster broadcaster to the hotSetFn
// signature; nil when no cluster is running.
func (e *engine) hotBroadcastFn(rs *runState) func(context.Context, string, string) int {
	if rs.broadcaster == nil {
		return nil
	}
	return rs.broadcaster.BroadcastHotSet
}

func applyHotKnob(k hotKnob, req admin.HotSetRequest, sets *prometheus.CounterVec) (admin.HotSetResult, error) {
	old := k.get()
	normalized, err := k.set(req.Value, !req.DryRun)
	if err != nil {
		sets.WithLabelValues("rejected").Inc()
		return admin.HotSetResult{}, err
	}
	res := admin.HotSetResult{
		Key:      req.Key,
		OldValue: old,
		NewValue: normalized,
		Applied:  !req.DryRun,
		Warning:  admin.HotSetWarning,
	}
	if req.DryRun {
		sets.WithLabelValues("dry_run").Inc()
		return res, nil
	}
	sets.WithLabelValues("applied").Inc()
	return res, nil
}

// hotKnobList builds the engine's hot knob set over the running
// components (issue #815). Every knob added here is instantly
// available to `bouine config get/set/diff`; knobs whose component is
// missing report unavailable instead of failing the endpoint.
func (e *engine) hotKnobList(rs *runState) []hotKnob {
	knobs := []hotKnob{
		banTTLKnob(e.cfg.Cluster.BanTTL, rs.store),
		urlRingSampleRateKnob(e.cfg.URLRingSampleRate,
			rs.rings.URL.SampleRate, rs.rings.URL.SetSampleRate),
	}
	return append(knobs, poolKnobs(e.cfg, rs.pools)...)
}

// poolKnobs builds the per-pool scoped knobs — bouine config
// pool:<name>.<field> (issue #815 follow-up): every pool contributes
// its hot-appliable health fields, sorted by name so the GET document
// is deterministic. Boot file values come from upstream_pools in the
// config; live values come from the pool itself.
func poolKnobs(cfg *config.Config, pools map[string]*origin.Pool) []hotKnob {
	fileValues := make(map[string]config.HealthPolicy, len(cfg.UpstreamPools))
	for _, pool := range cfg.UpstreamPools {
		fileValues[pool.Name] = pool.Health
	}
	names := make([]string, 0, len(pools))
	for name := range pools {
		names = append(names, name)
	}
	sort.Strings(names)
	knobs := make([]hotKnob, 0, 2*len(names))
	for _, name := range names {
		health := fileValues[name]
		knobs = append(knobs,
			poolConsecutive5xxKnob(name, health.Passive.Consecutive5xx, pools[name]),
			poolEjectForKnob(name, health.Passive.EjectFor, pools[name]),
		)
	}
	return knobs
}

// hotSetsMetric registers the hot-config audit counter on the
// engine's Prometheus registry.
func (e *engine) hotSetsMetric() *prometheus.CounterVec {
	sets := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bouine_config_hot_sets_total",
		Help: "Hot config point mutations via POST /v1/config/hot, by result (applied, dry_run, rejected).",
	}, []string{"result"})
	e.metrics.Registry.MustRegister(sets)
	return sets
}

// banTTLKnob builds the cluster.ban_ttl knob over whichever store
// implementation exposes the setter.
func banTTLKnob(fileValue time.Duration, store any) hotKnob {
	// Zero in the boot config means the built-in default applies, so
	// the effective restore value (and the knob default shown in the
	// wire doc) is DefaultBanTTL, not 0s.
	effective := storage.DefaultBanTTL
	if fileValue > 0 {
		effective = fileValue
	}
	return hotKnob{
		key:         "cluster.ban_ttl",
		description: "how long a lazy invalidation ban stays active before prune paths drop it; 0 applies the built-in 24h default",
		def:         storage.DefaultBanTTL.String(),
		fileValue:   effective.String(),
		fromFile:    fileValue > 0,
		get: func() string {
			if ts, ok := store.(interface {
				SetBanTTL(time.Duration)
				BanTTL() time.Duration
			}); ok {
				return ts.BanTTL().String()
			}
			return effective.String()
		},
		set: func(value string, apply bool) (string, error) {
			d, err := parseHotDuration(value)
			if err != nil {
				return "", fmt.Errorf("invalid duration %q: %w", value, err)
			}
			// The capability check runs before apply so a dry run on a
			// store without the setter reports the same error a real
			// set would.
			ts, ok := store.(interface {
				SetBanTTL(time.Duration)
				BanTTL() time.Duration
			})
			if !ok {
				return "", fmt.Errorf("cluster.ban_ttl is not hot-appliable on this store")
			}
			if apply {
				ts.SetBanTTL(d)
			}
			return d.String(), nil
		},
	}
}

// urlRingSampleRateKnob builds the url_ring_sample_rate knob over the
// observability rings. The built-in default is 0 (record everything),
// so an unset knob and a zero-valued file entry are the same value.
func urlRingSampleRateKnob(fileValue int, get func() int, set func(int)) hotKnob {
	return hotKnob{
		key:         "url_ring_sample_rate",
		description: "1-in-N sampling for the dashboard URL ring; 0 records every non-HIT request, 100 records 1 in 100",
		def:         "0",
		fileValue:   strconv.Itoa(fileValue),
		fromFile:    fileValue > 0,
		get: func() string {
			return strconv.Itoa(get())
		},
		set: func(value string, apply bool) (string, error) {
			n, err := strconv.Atoi(value)
			if err != nil {
				return "", fmt.Errorf("invalid integer %q", value)
			}
			if n < 0 {
				return "", fmt.Errorf("sample rate must be >= 0, got %d", n)
			}
			if apply {
				set(n)
			}
			return strconv.Itoa(n), nil
		},
	}
}

// poolConsecutive5xxKnob builds the pool:<name>.health.passive.consecutive_5xx
// knob: how many consecutive failed fetches passively eject a target
// from this pool. Fully hot: the cache-path fetch reads the atomic
// threshold per request, so a change takes effect immediately.
func poolConsecutive5xxKnob(poolName string, fileValue int, p *origin.Pool) hotKnob {
	return hotKnob{
		key:         "pool:" + poolName + ".health.passive.consecutive_5xx",
		description: "consecutive failed fetches before a target of this pool is passively ejected; 0 disables passive ejection",
		def:         "0",
		fileValue:   strconv.Itoa(fileValue),
		fromFile:    fileValue > 0,
		get:         func() string { return strconv.Itoa(p.Consecutive5xx()) },
		set: func(value string, apply bool) (string, error) {
			n, err := strconv.Atoi(value)
			if err != nil {
				return "", fmt.Errorf("invalid integer %q", value)
			}
			if n < 0 {
				return "", fmt.Errorf("threshold must be >= 0, got %d", n)
			}
			if apply {
				p.SetConsecutive5xx(n)
			}
			return strconv.Itoa(n), nil
		},
	}
}

// poolEjectForKnob builds the pool:<name>.health.passive.eject_for
// knob: how long a passively ejected target stays out before the
// EjectReaper restores it. Hot-changeable only when the boot config
// set eject_for — the reaper goroutine is started once at boot — so
// both dry runs and real sets on a reaper-less pool fail with the
// same precise error.
func poolEjectForKnob(poolName string, fileValue time.Duration, p *origin.Pool) hotKnob {
	reaperRuns := fileValue > 0
	return hotKnob{
		key: "pool:" + poolName + ".health.passive.eject_for",
		description: "how long a passively ejected target of this pool stays out before the reaper restores it; " +
			"0 keeps targets out until a probe succeeds. Runtime changes need the boot config to set eject_for (the reaper starts once, at boot)",
		def:       "0",
		fileValue: fileValue.String(),
		fromFile:  reaperRuns,
		get:       func() string { return p.EjectFor().String() },
		set: func(value string, apply bool) (string, error) {
			if !reaperRuns {
				return "", fmt.Errorf("eject_for reaper is not running for pool %q: the boot config left health.passive.eject_for unset, "+
					"so no reaper would honor a runtime change; set eject_for in your YAML and restart", poolName)
			}
			d, err := parseHotDuration(value)
			if err != nil {
				return "", fmt.Errorf("invalid duration %q: %w", value, err)
			}
			if apply {
				p.SetEjectFor(d)
			}
			return d.String(), nil
		},
	}
}

// parseHotDuration accepts Go duration syntax ("90m", "1h30m") or a
// bare integer read as seconds, matching how operators type values.
func parseHotDuration(value string) (time.Duration, error) {
	if secs, err := strconv.Atoi(value); err == nil {
		if secs < 0 {
			return 0, fmt.Errorf("negative durations are not allowed")
		}
		return time.Duration(secs) * time.Second, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, fmt.Errorf("negative durations are not allowed")
	}
	return d, nil
}
