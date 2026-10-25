package cmd

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/admin"
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

// hotSetFn applies one validated point mutation. DryRun validates and
// reports without applying. Every attempt is audited (log + counter);
// the admin write rate limit applies to the endpoint itself.
func (e *engine) hotSetFn(knobs func() []hotKnob, sets *prometheus.CounterVec) func(context.Context, admin.HotSetRequest) (admin.HotSetResult, error) {
	return func(_ context.Context, req admin.HotSetRequest) (admin.HotSetResult, error) {
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
				"dry_run", req.DryRun)
			return res, nil
		}
		return admin.HotSetResult{}, fmt.Errorf("unknown hot config key %q", req.Key)
	}
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
	return []hotKnob{
		banTTLKnob(e.cfg.Cluster.BanTTL, rs.store),
		urlRingSampleRateKnob(e.cfg.URLRingSampleRate,
			rs.rings.URL.SampleRate, rs.rings.URL.SetSampleRate),
	}
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
