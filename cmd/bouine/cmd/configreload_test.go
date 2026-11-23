package cmd

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/server"
	"github.com/bouine-cache/bouine/internal/storage"
)

// reloadFixture builds an engine + runState wired like production: a
// real hot store, a real rings instance, and live pools matching the
// boot config. The boot config is loaded from bootYAML; configPath
// points at it so default reloads work.
func reloadFixture(t *testing.T, bootYAML string) (*engine, *runState, string) {
	t.Helper()
	dir := t.TempDir()
	bootPath := filepath.Join(dir, "boot.yaml")
	require.NoError(t, os.WriteFile(bootPath, []byte(bootYAML), 0o600))
	cfg, err := config.Load(bootPath)
	require.NoError(t, err, "boot config must validate")

	e := newEngine(cfg, bootPath, slog.New(slog.DiscardHandler))
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	rs := &runState{
		store: store,
		rings: observability.NewRings("node-0"),
		pools: map[string]*origin.Pool{},
	}
	for _, pool := range cfg.UpstreamPools {
		p, perr := origin.NewPool(origin.PoolConfig{
			Name:    pool.Name,
			Targets: pool.Targets,
		})
		require.NoError(t, perr)
		rs.pools[pool.Name] = p
	}
	// Phase 2 (issue #951): the fixture needs a snapshot and a router
	// so applyConfigReload can swap them.
	tb := server.NewTableBuilder()
	rs.router = server.NewRouter(server.RouterConfig{})
	rs.router.Freeze()
	rs.snapshot.Store(&configSnapshot{
		cfg:             cfg,
		gen:             1,
		pools:           rs.pools,
		routes:          tb.Table(),
		handlersByRoute: map[string]*cache.Handler{},
	})
	return e, rs, dir
}

// writeReloadFile writes config YAML next to the fixture (kept for
// tests that still need a side file even though reload only reads the
// boot path).
func writeReloadFile(t *testing.T, dir, body string) {
	t.Helper()
	path := filepath.Join(dir, "new.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
}

func fixtureBanTTL(t *testing.T, rs *runState) time.Duration {
	t.Helper()
	ts, ok := rs.store.(interface {
		SetBanTTL(time.Duration)
		BanTTL() time.Duration
	})
	require.True(t, ok, "fixture store must expose the ban TTL setter")
	return ts.BanTTL()
}

const reloadBootYAML = `listen:
  http: 127.0.0.1:18080
cluster:
  ban_ttl: 24h
url_ring_sample_rate: 0
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
experimental:
  config_reload: true
`

func testReloadCounter() *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_reloads"}, []string{"result"})
}

func TestReloadFn_DisabledByDefault(t *testing.T) {
	t.Parallel()
	defaults := config.Defaults()
	e := newEngine(&defaults, "unused.yaml", slog.New(slog.DiscardHandler))
	_, err := e.reloadFn(&runState{}, testReloadCounter())(t.Context(), admin.ConfigReloadRequest{})
	require.Error(t, err)
	assert.ErrorIs(t, err, admin.ErrReloadDisabled)
}

func TestReloadFn_AppliesKnobsAndReportsColdFields(t *testing.T) {
	t.Parallel()
	e, rs, dir := reloadFixture(t, reloadBootYAML)
	writeReloadFile(t, dir, `listen:
  http: 127.0.0.1:18081
cluster:
  ban_ttl: 90m
url_ring_sample_rate: 100
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
experimental:
  config_reload: true
`)

	bootPath := filepath.Join(dir, "boot.yaml")
	require.NoError(t, os.WriteFile(bootPath, []byte(`listen:
  http: 127.0.0.1:18081
cluster:
  ban_ttl: 90m
url_ring_sample_rate: 100
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
experimental:
  config_reload: true
`), 0o600))

	res, err := e.reloadFn(rs, testReloadCounter())(t.Context(), admin.ConfigReloadRequest{})
	require.NoError(t, err)

	require.Len(t, res.Applied, 2, "applied: %v", res.Applied)
	byKey := map[string]admin.ConfigReloadChange{}
	for _, ch := range res.Applied {
		byKey[ch.Key] = ch
	}
	assert.Equal(t, "24h0m0s", byKey["cluster.ban_ttl"].Old)
	assert.Equal(t, "1h30m0s", byKey["cluster.ban_ttl"].New)
	assert.Equal(t, "0", byKey["url_ring_sample_rate"].Old)
	assert.Equal(t, "100", byKey["url_ring_sample_rate"].New)

	assert.Equal(t, 90*time.Minute, fixtureBanTTL(t, rs))
	assert.Equal(t, 100, rs.rings.URL.SampleRate())

	require.Len(t, res.NeedsRestart, 1, "needs_restart: %v", res.NeedsRestart)
	assert.Contains(t, res.NeedsRestart[0], "listen.http: 127.0.0.1:18080 -> 127.0.0.1:18081")

	// The registry now reports against the reloaded file: the applied
	// knobs read as "file", not runtime drift.
	for _, k := range e.hotKnobList(rs) {
		if k.key == "cluster.ban_ttl" {
			assert.Equal(t, "file", k.source(k.get()), "post-reload drift answers against the last-loaded file")
		}
	}
}

func TestReloadFn_ParseFailureLeavesEverythingUntouched(t *testing.T) {
	t.Parallel()
	e, rs, dir := reloadFixture(t, reloadBootYAML)
	writeReloadFile(t, dir, "listen:\n  http: 127.0.0.1:18081\ncluster:\n  ban_ttl: 90m\nnot_a_key: 1\n")

	bootPath := filepath.Join(dir, "boot.yaml")
	require.NoError(t, os.WriteFile(bootPath, []byte("listen:\n  http: 127.0.0.1:18081\ncluster:\n  ban_ttl: 90m\nnot_a_key: 1\n"), 0o600))

	_, err := e.reloadFn(rs, testReloadCounter())(t.Context(), admin.ConfigReloadRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not_a_key", "the load error surfaces")

	// No partial application: live values untouched, registry still
	// answers against the boot config.
	assert.Equal(t, storage.DefaultBanTTL, fixtureBanTTL(t, rs))
	assert.Equal(t, 0, rs.rings.URL.SampleRate())
	for _, k := range e.hotKnobList(rs) {
		if k.key == "cluster.ban_ttl" {
			assert.Equal(t, "file", k.source(k.get()), "failed reload must not swap the file config")
			assert.Equal(t, "24h0m0s", k.fileValue, "file value stays the boot config's")
		}
	}
}

func TestReloadFn_RevertsRuntimeDrift(t *testing.T) {
	t.Parallel()
	e, rs, dir := reloadFixture(t, reloadBootYAML)
	rs.rings.URL.SetSampleRate(100) // runtime drift

	writeReloadFile(t, dir, reloadBootYAML)
	res, err := e.reloadFn(rs, testReloadCounter())(t.Context(), admin.ConfigReloadRequest{})
	require.NoError(t, err)

	require.Len(t, res.Applied, 1)
	assert.Equal(t, "url_ring_sample_rate", res.Applied[0].Key)
	assert.Equal(t, "100", res.Applied[0].Old)
	assert.Equal(t, "0", res.Applied[0].New, "reload reverts drift to the file value")
	assert.Empty(t, res.NeedsRestart)
}

func TestReloadFn_EjectForReaperlessPoolNeedsRestart(t *testing.T) {
	t.Parallel()
	e, rs, dir := reloadFixture(t, reloadBootYAML)
	writeReloadFile(t, dir, `listen:
  http: 127.0.0.1:18080
cluster:
  ban_ttl: 90m
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
    health:
      passive:
        eject_for: 5m
experimental:
  config_reload: true
`)

	bootPath := filepath.Join(dir, "boot.yaml")
	require.NoError(t, os.WriteFile(bootPath, []byte(`listen:
  http: 127.0.0.1:18080
cluster:
  ban_ttl: 90m
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
    health:
      passive:
        eject_for: 5m
experimental:
  config_reload: true
`), 0o600))

	res, err := e.reloadFn(rs, testReloadCounter())(t.Context(), admin.ConfigReloadRequest{})
	require.NoError(t, err)

	assert.Len(t, res.Applied, 1, "the hot subset still applies: %v", res.Applied)
	assert.Equal(t, "cluster.ban_ttl", res.Applied[0].Key)

	var ejectItem string
	for _, item := range res.NeedsRestart {
		if strings.Contains(item, "eject_for") {
			ejectItem = item
		}
	}
	require.NotEmpty(t, ejectItem, "needs_restart: %v", res.NeedsRestart)
	assert.Contains(t, ejectItem, "pool:api.health.passive.eject_for")
	assert.Contains(t, ejectItem, "reaper is not running")
}

func TestReloadFn_PoolRemovedNeedsRestart(t *testing.T) {
	t.Parallel()
	boot := `listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
  - name: www
    targets: ["127.0.0.1:18000"]
experimental:
  config_reload: true
`
	e, rs, dir := reloadFixture(t, boot)
	writeReloadFile(t, dir, `listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
experimental:
  config_reload: true
`)

	bootPath := filepath.Join(dir, "boot.yaml")
	require.NoError(t, os.WriteFile(bootPath, []byte(`listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
experimental:
  config_reload: true
`), 0o600))

	res, err := e.reloadFn(rs, testReloadCounter())(t.Context(), admin.ConfigReloadRequest{})
	require.NoError(t, err)

	// Phase 2 (issue #951): pool removal is hot — the pool registry
	// swaps, the removed pool is Closed, and no needs_restart row is
	// produced for it.
	joined := strings.Join(res.NeedsRestart, "\n")
	assert.NotContains(t, joined, "upstream_pools", "pool removal is hot in phase 2: %v", res.NeedsRestart)
	snap := rs.snapshot.Load()
	assert.NotContains(t, snap.pools, "www", "removed pool leaves the registry")
	assert.Contains(t, snap.pools, "api")
}

func TestReloadFn_DefaultPathIsEngineConfigPath(t *testing.T) {
	t.Parallel()
	e, rs, dir := reloadFixture(t, reloadBootYAML)
	// Edit the boot file in place — the operator's real workflow.
	bootPath := filepath.Join(dir, "boot.yaml")
	require.NoError(t, os.WriteFile(bootPath, []byte(`listen:
  http: 127.0.0.1:18080
cluster:
  ban_ttl: 30m
experimental:
  config_reload: true
`), 0o600))

	res, err := e.reloadFn(rs, testReloadCounter())(t.Context(), admin.ConfigReloadRequest{})
	require.NoError(t, err)
	assert.Equal(t, bootPath, res.Path)
	require.Len(t, res.Applied, 1)
	assert.Equal(t, "cluster.ban_ttl", res.Applied[0].Key)
	assert.Equal(t, "30m0s", res.Applied[0].New)
}

func TestReloadFn_NoopReportsUnchanged(t *testing.T) {
	t.Parallel()
	e, rs, dir := reloadFixture(t, reloadBootYAML)
	writeReloadFile(t, dir, reloadBootYAML)

	res, err := e.reloadFn(rs, testReloadCounter())(t.Context(), admin.ConfigReloadRequest{})
	require.NoError(t, err)
	assert.Empty(t, res.Applied)
	assert.Empty(t, res.NeedsRestart)
	assert.NotEmpty(t, res.Unchanged, "knobs at the file value are reported unchanged")
}

func phase2Cfg(t *testing.T, yaml string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(yaml))
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	return cfg
}

const phase2BootYAML = `listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
routes:
  - name: api
    pool: api
    match:
      host: api.example.com
      path_prefix: /v1
experimental:
  config_reload: true
`

// TestKeyFormChanged_AddRemoveRename: the gate fires on any change to
// the (identity -> key form) set, including additions, removals and
// renames (issue #951 decision 7).
func TestKeyFormChanged_AddRemoveRename(t *testing.T) {
	t.Parallel()
	old := phase2Cfg(t, phase2BootYAML)

	// in-place key-field edit
	edited := phase2Cfg(t, `listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
routes:
  - name: api
    pool: api
    match:
      host: api.example.com
      path_prefix: /v1
    cache:
      key:
        strip_query_params: [utm_source]
experimental:
  config_reload: true
`)
	assert.True(t, keyFormChanged(old, edited), "in-place key-field edit fires the gate")

	// route added with a key form
	added := phase2Cfg(t, `listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
routes:
  - name: api
    pool: api
    match:
      host: api.example.com
      path_prefix: /v1
  - name: other
    pool: api
    match:
      host: other.example.com
    cache:
      key:
        verbatim_encoding: true
experimental:
  config_reload: true
`)
	assert.True(t, keyFormChanged(old, added), "route addition fires the gate")

	// rename: same route, different name, same key form — remove+add
	renamed := phase2Cfg(t, `listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
routes:
  - name: api-renamed
    pool: api
    match:
      host: api.example.com
      path_prefix: /v1
experimental:
  config_reload: true
`)
	assert.True(t, keyFormChanged(old, renamed), "rename (remove+add) fires the gate")

	// non-key-form change only: pool target edit
	targets := phase2Cfg(t, `listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:20000"]
routes:
  - name: api
    pool: api
    match:
      host: api.example.com
      path_prefix: /v1
experimental:
  config_reload: true
`)
	assert.False(t, keyFormChanged(old, targets), "pool target change does not touch key form")
}

// TestResolvedConfigSHA256_EnvDivergence: the fan-out hashes the
// RESOLVED config, so two daemons with the same file but different
// ${VAR} environments produce different hashes (decision 8).
func TestResolvedConfigSHA256_EnvDivergence(t *testing.T) {
	// The listen.port is zeroed before hashing (per-node field), so
	// the divergence must come from a SHARED field: the pool target.
	yamlWithVar := `listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["${BOUINE_TEST_TARGET:-127.0.0.1:19999}"]
experimental:
  config_reload: true
`
	t.Setenv("BOUINE_TEST_TARGET", "127.0.0.1:19999")
	a := phase2Cfg(t, yamlWithVar)
	t.Setenv("BOUINE_TEST_TARGET", "10.0.0.1:19999")
	b := phase2Cfg(t, yamlWithVar)
	assert.NotEqual(t, resolvedConfigSHA256(a), resolvedConfigSHA256(b),
		"same file, different env -> different resolved hash (stale_config on the peer)")
	assert.Equal(t, resolvedConfigSHA256(a), resolvedConfigSHA256(a))
}

// TestRouteIdentityUniqueValidation: two routes deriving the same
// identity are rejected by config.Validate (decision 12).
func TestRouteIdentityUniqueValidation(t *testing.T) {
	t.Parallel()
	_, err := config.Parse([]byte(`listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
routes:
  - pool: api
    match:
      host: dup.example.com
      path_prefix: /v1
  - pool: api
    match:
      host: dup.example.com
      path_prefix: /v1
`))
	require.Error(t, err)
	_, namedErr := config.Parse([]byte(`listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
routes:
  - name: a
    pool: api
    match:
      host: dup.example.com
  - name: b
    pool: api
    match:
      host: dup.example.com
`))
	require.NoError(t, namedErr, "explicit distinct names are fine even on the same host")
}

// TestRouteIdentityExplicitNameBeatsDerived: an explicit name is the
// identity even when the match predicate matches another route.
func TestRouteIdentityExplicitNameBeatsDerived(t *testing.T) {
	t.Parallel()
	cfg := phase2Cfg(t, `listen:
  http: 127.0.0.1:18080
upstream_pools:
  - name: api
    targets: ["127.0.0.1:19999"]
routes:
  - name: named
    pool: api
    match:
      host: h.example.com
      path_prefix: /x
  - pool: api
    match:
      host: h.example.com
      path_prefix: /y
`)
	// Identity = name + full match predicate (mirrors config.Validate),
	// so the same name on different predicates is still distinct.
	assert.Contains(t, routeIdentity(cfg.Routes[0]), "named|")
	assert.NotEqual(t, routeIdentity(cfg.Routes[0]), routeIdentity(cfg.Routes[1]))
}
