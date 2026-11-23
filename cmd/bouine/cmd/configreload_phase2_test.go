package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/config"
)

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
