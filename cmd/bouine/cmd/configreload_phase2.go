package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/server"
)

// configSnapshot is the immutable hot-swappable state published
// through the engine's atomic pointer (issue #951, phase 2): the
// route table plus the pool registry, plus the last-applied resolved
// config (the single source of truth for drift reporting — the phase-1
// hotCfg atomic is folded in here) and the monotonic config
// generation used by the cluster fan-out loop guard and the
// generation-skew convergence alarm.
//
// The snapshot is built whole and swapped with one atomic store, so a
// reload that fails anywhere during the build leaves the running
// snapshot untouched (per-class atomicity, decision 2).
type configSnapshot struct {
	// cfg is the resolved config that produced this snapshot (after
	// config.Load + Validate). Drift reporting and the fan-out hash
	// read through it.
	cfg *config.Config
	// pools is the pool registry: name -> pool. Pools present in both
	// old and new snapshots are the SAME *origin.Pool object (targets
	// swapped in place under the pool mutex); added pools are new
	// objects; removed pools are closed at swap.
	pools map[string]*origin.Pool
	// routes is the immutable router table installed with
	// Router.SetTable. Entries for unchanged routes reuse the old
	// routeEntry (handler + fastPath objects identical, decision 10).
	routes *server.RouteTable
	// handlersByRoute maps route identity -> live *cache.Handler so
	// the swap can Close changed-route handlers and the shutdown
	// drain can join the swapped-out set.
	handlersByRoute map[string]*cache.Handler
	// gen is the config generation: boot = 1, each applied reload
	// increments. Exposed in status for fleet-convergence checks.
	gen uint64
}

// routeIdentity is the validated unique identity of a route (decision
// 12): the name (explicit or auto-derived by Validate) plus the full
// match predicate, mirroring the uniqueness check in
// config.Validate (loader.go). It keys the name-keyed diff and the
// handler reuse map.
func routeIdentity(rc config.Route) string {
	return rc.Name + "|" + rc.Match.Host + "|" + rc.Match.PathLabel() + "|" + fmt.Sprintf("%v", rc.Match.Methods)
}

// keyFormFingerprint is the cache-key-form-affecting subset of a route
// (decision 7): any change to the (identity -> key form) set —
// additions, removals, renames, and in-place edits — fires the
// all-peers-ack gate.
func keyFormFingerprint(rc config.Route) string {
	k := rc.Cache.Key
	b, _ := json.Marshal([]any{
		k.IncludeHost, k.VerbatimEncoding, k.IncludeHeaders, k.ExcludeHeaders,
		k.CookiePresence, k.CookieValues, k.StripQueryParams, k.KeepQueryParams,
		k.StripQueryPrefix, k.StripEmptyParams, k.DedupQueryParams,
	})
	return string(b)
}

// keyFormSet maps route identity -> key-form fingerprint.
func keyFormSet(routes []config.Route) map[string]string {
	m := make(map[string]string, len(routes))
	for _, rc := range routes {
		m[routeIdentity(rc)] = keyFormFingerprint(rc)
	}
	return m
}

// keyFormChanged reports whether the (identity -> key form) set
// differs between old and new configs, including add/remove/rename.
func keyFormChanged(oldCfg, newCfg *config.Config) bool {
	oldSet := keyFormSet(oldCfg.Routes)
	newSet := keyFormSet(newCfg.Routes)
	if len(oldSet) != len(newSet) {
		return true
	}
	for id, fp := range newSet {
		if oldFP, ok := oldSet[id]; !ok || oldFP != fp {
			return true
		}
	}
	return false
}

// resolvedConfigSHA256 is the fan-out payload hash (decision 8): the
// SHA-256 of the canonical YAML marshal of the RESOLVED config, never
// the file bytes — config.Load interpolates ${VAR} per daemon, so
// identical files can resolve differently across peers. YAML (not
// JSON) because the config tree's MarshalJSON on some leaf types
// renders display strings (e.g. ByteSize), which would make the hash
// depend on marshal flavor; YAML marshals the raw typed values.
//
// The per-node fields (listen.* ports, cluster.node_name/join) are
// zeroed before hashing: they are legitimately different across peers
// and must not count as divergence. The hazard this hash exists to
// catch is ${VAR} interpolation and file content differing on the
// shared, replicated fields.
func resolvedConfigSHA256(cfg *config.Config) string {
	c := *cfg
	c.Listen = config.Listen{}
	c.Cluster.NodeName = ""
	c.Cluster.Join = nil
	cfg = &c
	b, err := yaml.Marshal(cfg)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// swapPools reconciles the pool registry between old and new configs
// (decisions 1 and 11): pools present in both keep their *origin.Pool
// object and get their target list swapped under the pool mutex
// (unchanged addrs keep their *Target objects); added pools are
// constructed fresh; removed pools are returned for Close.
//
// The connect.* and health.active.* settings of existing pools are
// cold (baked into the fasthttp.Client at boot / boot-started probe
// goroutines), so only the target list moves.
func (e *engine) swapPools(newCfg *config.Config, oldPools map[string]*origin.Pool, metrics *origin.Metrics) (map[string]*origin.Pool, []*origin.Pool, error) {
	newPools := make(map[string]*origin.Pool, len(newCfg.UpstreamPools))
	var removed []*origin.Pool
	for _, pc := range newCfg.UpstreamPools {
		if old, ok := oldPools[pc.Name]; ok {
			targets, err := mergeTargets(old, pc.Targets, metrics)
			if err != nil {
				return nil, nil, fmt.Errorf("pool %q: %w", pc.Name, err)
			}
			old.SetTargets(targets)
			newPools[pc.Name] = old
			continue
		}
		p, err := origin.NewPool(buildPoolConfig(pc, e.logger, metrics))
		if err != nil {
			return nil, nil, err
		}
		newPools[pc.Name] = p
	}
	for name, p := range oldPools {
		if _, ok := newPools[name]; !ok {
			removed = append(removed, p)
		}
	}
	sort.Slice(removed, func(i, j int) bool { return removed[i].Name < removed[j].Name })
	return newPools, removed, nil
}

// mergeTargets builds the new target slice for an existing pool:
// unchanged addresses reuse the old *Target (preserving eject/error
// state); new addresses get a fresh healthy target.
func mergeTargets(p *origin.Pool, addrs []string, metrics *origin.Metrics) ([]*origin.Target, error) {
	out := make([]*origin.Target, 0, len(addrs))
	for _, addr := range addrs {
		if t := p.TargetByAddr(addr); t != nil {
			out = append(out, t)
			continue
		}
		t, err := origin.NewTargetForReload(addr, metrics)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// applyConfigReload builds and atomically swaps the routes+pools
// snapshot, then applies the hot knobs. Returns the admin result.
// Any build error leaves the running snapshot untouched.
func (e *engine) applyConfigReload(ctx context.Context, rs *runState, newCfg *config.Config, reloads *prometheus.CounterVec) (admin.ConfigReloadResult, uint64, bool) {
	old := rs.snapshot.Load()
	oldCfg := old.cfg

	newGen := old.gen + 1
	res := admin.ConfigReloadResult{Path: e.configPath}
	res.Gen = newGen

	// (a) pool registry reconciliation: construct added pools, swap
	// target lists on common pools, collect removed pools.
	newPools, removedPools, err := e.swapPools(newCfg, old.pools, rs.originMetrics)
	if err != nil {
		reloads.WithLabelValues("rejected").Inc()
		res.NeedsRestart = append(res.NeedsRestart, "routes/pools build failed: "+err.Error())
		return res, old.gen, false
	}

	// (b)+(c) route table build: reuse unchanged routeEntries, build
	// handlers for changed routes against the correct pool objects.
	// Built BEFORE the store so a build failure leaves the running
	// snapshot untouched; the pool target swaps from (a) already
	// landed, which is the documented benign window (routes and target
	// lists are independent dimensions).
	tb, newHandlers, changedHandlers, ok := e.buildRouteTableForReload(rs, oldCfg, newCfg, old, newPools, reloads, &res)
	if !ok {
		return res, old.gen, false
	}

	// key-form gate (decision 7): refused unless every live peer acks.
	keyFormChange := keyFormChanged(oldCfg, newCfg)

	// (d) one atomic store.
	newSnap := &configSnapshot{
		cfg:             newCfg,
		gen:             newGen,
		pools:           newPools,
		routes:          tb.Table(),
		handlersByRoute: newHandlers,
	}
	rs.router.SetTable(newSnap.routes)
	rs.snapshot.Store(newSnap)

	// Close-at-swap (decision 10): changed-route handlers are Closed;
	// swapped-out handlers join the draining set that shutdown joins.
	for _, h := range changedHandlers {
		_ = h.Close(ctx)
		rs.drainSwapped(h)
	}
	// Removed pools: Close (CloseIdleConnections); in-flight fetches
	// complete on their own — after the atomic swap no route can
	// reference them (config.Validate rejects unknown pools).
	for _, p := range removedPools {
		_ = p.Close(ctx)
	}
	// Added pools: start probe + reaper goroutines under the
	// supervised group (decision 11).
	e.startPoolGoroutines(rs, newCfg, newPools, old.pools)

	// Hot knobs still apply on top (phase 1 subset).
	knobRes := e.applyReload(rs, newCfg)
	res.Applied = append(res.Applied, knobRes.Applied...)
	res.Unchanged = append(res.Unchanged, knobRes.Unchanged...)
	res.NeedsRestart = append(res.NeedsRestart, knobRes.NeedsRestart...)

	result := "applied"
	if keyFormChange {
		result = "applied_all"
	}
	res.Result = result
	reloads.WithLabelValues(result).Inc()
	return res, newGen, keyFormChange
}

// routeHotConfigEqual reports whether every hot-swappable field of a
// route is unchanged between old and new (identity already matched).
func routeHotConfigEqual(old, new config.Route) bool {
	a, _ := json.Marshal(old)
	b, _ := json.Marshal(new)
	return string(a) == string(b)
}

// publishBootSnapshot builds the initial routes+pools snapshot after
// buildDataPlane so the reload path and the fan-out have a baseline
// (gen = 1).
func (e *engine) publishBootSnapshot(rs *runState) {
	if rs.router == nil {
		return
	}
	rs.router.Freeze()
	rs.snapshot.Store(&configSnapshot{
		cfg:             e.cfg,
		gen:             1,
		pools:           rs.pools,
		routes:          rs.router.LiveTable(),
		handlersByRoute: rs.handlersByRoute,
	})
}

// buildRouteTableForReload builds the swapped-in route table (swap
// steps b+c): unchanged routes reuse their old routeEntry, changed
// routes get freshly built handlers against the new pool registry.
// On any build failure it records the row in res.NeedsRestart and
// returns ok=false so the caller leaves the running snapshot alone.
func (e *engine) buildRouteTableForReload(rs *runState, oldCfg, newCfg *config.Config, old *configSnapshot, newPools map[string]*origin.Pool, reloads *prometheus.CounterVec, res *admin.ConfigReloadResult) (*server.TableBuilder, map[string]*cache.Handler, []*cache.Handler, bool) {
	tb := server.NewTableBuilder()
	newHandlers := make(map[string]*cache.Handler, len(newCfg.Routes))
	var changedHandlers []*cache.Handler
	oldByIdentity := make(map[string]config.Route, len(oldCfg.Routes))
	for _, rc := range oldCfg.Routes {
		oldByIdentity[routeIdentity(rc)] = rc
	}
	for _, rc := range newCfg.Routes {
		id := routeIdentity(rc)
		if oldEntry, ok := old.routes.EntryByIdentity(id); ok && routeHotConfigEqual(oldByIdentity[id], rc) {
			methods := make([]string, 0, len(oldEntry.Methods))
			for m := range oldEntry.Methods {
				methods = append(methods, m)
			}
			sort.Strings(methods)
			if perr := tb.WithIdentity(id).Add(server.RouteSpec{
				Host: oldEntry.Host, PathPrefix: oldEntry.PathPrefix, Path: oldEntry.Path,
				Label: oldEntry.Label, Pool: oldEntry.Pool, Methods: methods,
				Handler: oldEntry.Handler, FastPath: oldEntry.FastPath,
			}); perr != nil {
				reloads.WithLabelValues("rejected").Inc()
				res.NeedsRestart = append(res.NeedsRestart, perr.Error())
				return nil, nil, nil, false
			}
			newHandlers[id] = old.handlersByRoute[id]
			continue
		}
		handler, fp, perr := e.buildRouteHandlerForReload(rs, rc, newPools)
		if perr != nil {
			reloads.WithLabelValues("rejected").Inc()
			res.NeedsRestart = append(res.NeedsRestart, fmt.Sprintf("route %q build failed: %s", rc.Name, perr))
			return nil, nil, nil, false
		}
		if handler == nil {
			continue
		}
		if perr := tb.WithIdentity(id).Add(server.RouteSpec{
			Host: rc.Match.Host, PathPrefix: rc.Match.PathPrefix, Path: rc.Match.Path,
			Label: rc.Name, Pool: rc.Pool, Methods: rc.Match.Methods,
			Handler: handler, FastPath: fp,
		}); perr != nil {
			reloads.WithLabelValues("rejected").Inc()
			res.NeedsRestart = append(res.NeedsRestart, fmt.Sprintf("route %q: %s", rc.Name, perr))
			return nil, nil, nil, false
		}
		if h := old.handlersByRoute[id]; h != nil {
			changedHandlers = append(changedHandlers, h)
		}
		newHandlers[id] = rs.handlersByRoute[id]
	}
	return tb, newHandlers, changedHandlers, true
}
