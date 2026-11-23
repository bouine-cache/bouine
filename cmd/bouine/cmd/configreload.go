package cmd

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/cluster"
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
	return func(ctx context.Context, req admin.ConfigReloadRequest) (admin.ConfigReloadResult, error) {
		if !e.cfg.Experimental.ConfigReload {
			return admin.ConfigReloadResult{}, admin.ErrReloadDisabled
		}
		// Decision 5: the endpoint is restricted to the boot config
		// path; a differing non-empty path gets 400 (silently ignoring
		// it is worse).
		newCfg, err := e.loadBootConfig(req, reloads)
		if err != nil {
			return admin.ConfigReloadResult{Path: e.configPath}, err
		}
		for _, w := range newCfg.Warnings {
			e.logger.Warn("config advisory after reload", "path", w.Path, "message", w.Message)
		}

		// Phase 2 (issue #951): build and atomically swap the
		// routes+pools snapshot, then apply the hot knobs on top.
		// Hash the freshly loaded config BEFORE the swap mutates any
		// state; the peer re-resolves its own boot file the same way.
		hash := resolvedConfigSHA256(newCfg)
		res, gen, keyFormChange := e.applyConfigReload(ctx, rs, newCfg, reloads)
		res.Path = e.configPath
		e.hotCfg.Store(newCfg)

		e.fanoutAndReport(rs, reloads, ctx, &res, hash, gen, keyFormChange)
		return res, nil
	}
}

// loadBootConfig enforces decision 5 (the endpoint reloads only the
// boot path) and loads+validates it, counting rejections.
func (e *engine) loadBootConfig(req admin.ConfigReloadRequest, reloads *prometheus.CounterVec) (*config.Config, error) {
	if req.Path != "" && req.Path != e.configPath {
		reloads.WithLabelValues("rejected").Inc()
		return nil, fmt.Errorf("reload path %q is not the boot config %q: only the daemon's startup file can be reloaded", req.Path, e.configPath)
	}
	newCfg, err := config.Load(e.configPath)
	if err != nil {
		reloads.WithLabelValues("rejected").Inc()
		return nil, fmt.Errorf("reload %s: %w", e.configPath, err)
	}
	return newCfg, nil
}

// fanoutAndReport runs the cluster fan-out for a just-applied reload,
// folds the per-peer acks into res, applies the key-form gate
// downgrade (decision 7), and emits the metric + audit log.
func (e *engine) fanoutAndReport(rs *runState, reloads *prometheus.CounterVec, ctx context.Context, res *admin.ConfigReloadResult, hash string, gen uint64, keyFormChange bool) {
	result := res.Result
	if result == "" {
		result = "noop"
		if len(res.Applied) > 0 {
			result = "applied"
		}
	}
	if rs.broadcaster != nil && gen > 0 {
		peerResults := rs.broadcaster.BroadcastConfigReload(ctx, gen, hash)
		for _, pr := range peerResults {
			ack := admin.PeerReloadAck{Peer: pr.Peer, Status: pr.Status}
			if pr.Err != nil {
				ack.Status = "error"
				ack.Error = pr.Err.Error()
			}
			res.Peers = append(res.Peers, ack)
		}
		if keyFormChange && result == "applied_all" && !allPeersAcked(peerResults) {
			result = "applied_local_only"
			res.Result = result
		}
	}
	reloads.WithLabelValues(result).Inc()
	e.logger.Info("config reload",
		"path", e.configPath,
		"result", result,
		"gen", gen,
		"applied", len(res.Applied),
		"unchanged", len(res.Unchanged),
		"needs_restart", len(res.NeedsRestart),
		"peers", len(res.Peers))
}

// allPeersAcked reports whether every fan-out peer answered applied or
// current (the key-form gate's ack condition, decision 7).
func allPeersAcked(results []cluster.ConfigReloadPeerResult) bool {
	for _, pr := range results {
		if pr.Status != "applied" && pr.Status != "current" {
			return false
		}
	}
	return true
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

	// The cold-only diff compares against the boot config e.cfg (cold
	// fields never change at runtime — the running truth for them is
	// always the boot config), while knob coverage is name-keyed
	// against both the boot and last-applied configs.
	covered := reloadCoveredPaths(newCfg, rs, oldSnapshotCfg(e, rs))
	for _, row := range diffConfigs(e.cfg, newCfg) {
		if coveredPath(row.Path, covered) {
			continue
		}
		res.NeedsRestart = append(res.NeedsRestart,
			fmt.Sprintf("%s: %s -> %s", row.Path, row.A, row.B))
	}
	sort.Strings(res.NeedsRestart)
	return res
}

// oldSnapshotCfg returns the config the running snapshot was built
// from when available (phase 2, issue #951), else the boot config.
func oldSnapshotCfg(e *engine, rs *runState) *config.Config {
	if snap := rs.snapshot.Load(); snap != nil && snap.cfg != nil {
		return snap.cfg
	}
	return e.cfg
}

// coveredPath reports whether a diff row path is covered by the
// reload: exact match, or a child of a covered container (routes[],
// upstream_pools[] in phase 2).
func coveredPath(path string, covered map[string]struct{}) bool {
	if _, ok := covered[path]; ok {
		return true
	}
	for prefix := range covered {
		if strings.HasPrefix(path, prefix+"[") || strings.HasPrefix(path, prefix+".") {
			return true
		}
	}
	return false
}

// reloadCoveredPaths returns the config paths a reload actually
// applies, in diffConfigs vocabulary: the global knob paths, the
// per-pool passive health fields of pools live in BOTH the old and new
// configs, and the whole routes[]/upstream_pools[] surface when the
// phase-2 swap covers it (issue #951). Proposal 6: name-keyed — the
// index in the path comes from the NEW file, and membership is
// checked by name on both sides, so inserting a pool at index 0 does
// not misclassify coverage.
func reloadCoveredPaths(cfg *config.Config, rs *runState, oldCfg *config.Config) map[string]struct{} {
	covered := map[string]struct{}{
		"cluster.ban_ttl":      {},
		"url_ring_sample_rate": {},
		// Phase 2: routes and pool membership/targets swap hot.
		"routes":         {},
		"upstream_pools": {},
	}
	oldNames := make(map[string]struct{}, len(oldCfg.UpstreamPools))
	for _, p := range oldCfg.UpstreamPools {
		oldNames[p.Name] = struct{}{}
	}
	for i, pool := range cfg.UpstreamPools {
		if _, live := rs.pools[pool.Name]; live {
			if _, wasOld := oldNames[pool.Name]; wasOld {
				covered[fmt.Sprintf("upstream_pools[%d].health.passive.consecutive_5xx", i)] = struct{}{}
				covered[fmt.Sprintf("upstream_pools[%d].health.passive.eject_for", i)] = struct{}{}
			}
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
