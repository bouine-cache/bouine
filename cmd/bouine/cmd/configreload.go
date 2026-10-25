package cmd

import (
	"context"
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/config"
)

// reloadFn serves POST /v1/config/reload (bouine config reload,
// issue #817, phase 1: hot-config knobs). A parse or validation
// failure leaves the running daemon untouched — the error carries the
// file path and the load error, nothing else happens. Knob-level
// refusals (e.g. eject_for on a reaper-less pool) apply the rest of
// the hot subset and surface under needs_restart, per the issue's
// "explicit report of what could not be applied". On success the
// registry's notion of the file config swaps to the reloaded file so
// drift reporting (`bouine config diff`) answers against what the
// last reload loaded.
func (e *engine) reloadFn(rs *runState, reloads *prometheus.CounterVec) func(context.Context, admin.ConfigReloadRequest) (admin.ConfigReloadResult, error) {
	return func(_ context.Context, req admin.ConfigReloadRequest) (admin.ConfigReloadResult, error) {
		if !e.cfg.Experimental.ConfigReload {
			return admin.ConfigReloadResult{}, admin.ErrReloadDisabled
		}
		path := req.Path
		if path == "" {
			path = e.configPath
		}
		newCfg, err := config.Load(path)
		if err != nil {
			reloads.WithLabelValues("rejected").Inc()
			return admin.ConfigReloadResult{Path: path}, fmt.Errorf("reload %s: %w", path, err)
		}

		res := e.applyReload(rs, newCfg)
		res.Path = path
		e.hotCfg.Store(newCfg)

		result := "noop"
		if len(res.Applied) > 0 {
			result = "applied"
		}
		reloads.WithLabelValues(result).Inc()
		e.logger.Info("config reload",
			"path", path,
			"result", result,
			"applied", len(res.Applied),
			"unchanged", len(res.Unchanged),
			"needs_restart", len(res.NeedsRestart))
		return res, nil
	}
}

// applyReload applies the hot-appliable subset of newCfg through the
// knob registry and classifies everything else. Two passes keep the
// hot subset as atomic as its parts: pre-validate every differing
// knob (the values come from a validated file, but capability errors
// like a reaper-less eject_for can still refuse), then apply. The
// cold-only diff compares against e.cfg — the boot config the daemon
// actually runs with, since no reload can change cold fields.
func (e *engine) applyReload(rs *runState, newCfg *config.Config) admin.ConfigReloadResult {
	var res admin.ConfigReloadResult
	type pending struct {
		knob hotKnob
		old  string
		new  string
	}
	var toApply []pending
	for _, k := range e.hotKnobsFor(newCfg, rs) {
		live := k.get()
		if live == k.fileValue {
			res.Unchanged = append(res.Unchanged, k.key)
			continue
		}
		normalized, err := k.set(k.fileValue, false)
		if err != nil {
			res.NeedsRestart = append(res.NeedsRestart, k.key+": "+err.Error())
			continue
		}
		toApply = append(toApply, pending{knob: k, old: live, new: normalized})
	}
	for _, p := range toApply {
		if _, err := p.knob.set(p.new, true); err != nil {
			res.NeedsRestart = append(res.NeedsRestart, p.knob.key+": "+err.Error())
			continue
		}
		res.Applied = append(res.Applied, admin.ConfigReloadChange{
			Key: p.knob.key, Old: p.old, New: p.new,
		})
	}

	covered := reloadCoveredPaths(newCfg, rs)
	for _, row := range diffConfigs(e.cfg, newCfg) {
		if _, ok := covered[row.Path]; !ok {
			res.NeedsRestart = append(res.NeedsRestart,
				fmt.Sprintf("%s: %s -> %s", row.Path, row.A, row.B))
		}
	}
	sort.Strings(res.NeedsRestart)
	return res
}

// reloadCoveredPaths returns the config paths the hot-knob registry
// applies during a reload, in diffConfigs vocabulary: the global knob
// paths plus the per-pool passive health fields of every pool that is
// both live and named in cfg. Cold rows the registry covers are
// excluded from needs_restart; everything else that differs (pool
// membership, ordering, routes, listeners, TLS, ...) is reported.
func reloadCoveredPaths(cfg *config.Config, rs *runState) map[string]struct{} {
	covered := map[string]struct{}{
		"cluster.ban_ttl":      {},
		"url_ring_sample_rate": {},
	}
	for i, pool := range cfg.UpstreamPools {
		if _, live := rs.pools[pool.Name]; live {
			covered[fmt.Sprintf("upstream_pools[%d].health.passive.consecutive_5xx", i)] = struct{}{}
			covered[fmt.Sprintf("upstream_pools[%d].health.passive.eject_for", i)] = struct{}{}
		}
	}
	return covered
}

// reloadMetrics registers the config-reload audit counter on the
// engine's Prometheus registry.
func (e *engine) reloadMetrics() *prometheus.CounterVec {
	reloads := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bouine_config_reloads_total",
		Help: "Config reloads via POST /v1/config/reload, by result (applied, noop, rejected).",
	}, []string{"result"})
	e.metrics.Registry.MustRegister(reloads)
	return reloads
}
