package config

import (
	"strings"
	"testing"
	"time"
)

// The near-cache config block (ADR-0071): strict decode of the
// experimental key, and the validation bounds that make the feature's
// blast radius explicit.

func nearConfigYAML(block string) string {
	return `
listen:
  admin: ":9000"
upstream_pools:
  - name: app
    targets: ["a:1"]
routes:
  - match: { path_prefix: "/" }
    pool: app
` + block
}

func TestNearCacheConfig_Decode(t *testing.T) {
	t.Parallel()
	cfg, err := Parse([]byte(nearConfigYAML(`
experimental:
  near_cache:
    enabled: true
    routes: ["/"]
    max_bytes: 64MiB
    max_ttl: 45s
`)))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	nc := cfg.Experimental.NearCache
	if nc == nil {
		t.Fatal("experimental.near_cache must decode")
	}
	if !nc.Enabled {
		t.Fatal("enabled must decode true")
	}
	if len(nc.Routes) != 1 || nc.Routes[0] != "/" {
		t.Fatalf("routes: got %v, want [/]", nc.Routes)
	}
	if int64(nc.MaxBytes) != 64<<20 {
		t.Fatalf("max_bytes: got %d, want %d", int64(nc.MaxBytes), int64(64<<20))
	}
	if nc.MaxTTL != 45*time.Second {
		t.Fatalf("max_ttl: got %v, want 45s", nc.MaxTTL)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a bounded enabled block must validate: %v", err)
	}
}

func TestNearCacheConfig_AbsentMeansDisabled(t *testing.T) {
	t.Parallel()
	cfg, err := Parse([]byte(nearConfigYAML("")))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Experimental.NearCache != nil {
		t.Fatal("no near_cache block must decode to nil (disabled)")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestNearCacheConfig_PresentButDisabledSkipsBounds(t *testing.T) {
	t.Parallel()
	// A disabled block may carry out-of-bound knobs: validation only
	// applies to an enabled feature, so a config can ship the block
	// ahead of the flip without satisfying caps it does not use.
	cfg, err := Parse([]byte(nearConfigYAML(`
experimental:
  near_cache:
    enabled: false
    max_bytes: 999MiB
    max_ttl: 1h
`)))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a disabled block must not be bounds-checked: %v", err)
	}
}

func TestNearCacheConfig_ValidationBounds(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		block string
		want  string
	}{
		{
			"max_bytes over cap",
			"    max_bytes: 512MiB\n",
			"experimental.near_cache.max_bytes",
		},
		{
			"max_ttl over cap",
			"    max_ttl: 30m\n",
			"experimental.near_cache.max_ttl",
		},
		{
			"max_bytes negative",
			"    max_bytes: -1\n",
			"experimental.near_cache.max_bytes",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Parse validates underneath, so an enabled block with an
			// out-of-bound knob fails at decode with the field name.
			_, err := Parse([]byte(nearConfigYAML("experimental:\n  near_cache:\n    enabled: true\n" + tc.block)))
			if err == nil {
				t.Fatalf("expected validation error for %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestNearCacheConfig_UnknownKeyRejected(t *testing.T) {
	t.Parallel()
	_, err := Parse([]byte(nearConfigYAML(`
experimental:
  near_cache:
    enabled: true
    admit_window: 30s
`)))
	if err == nil {
		t.Fatal("strict decode must reject unknown near_cache keys")
	}
	if !strings.Contains(err.Error(), "admit_window") {
		t.Fatalf("error %q does not mention the unknown key", err)
	}
}

// TestNearCacheConfig_RoutesAllowlist pins the route-scoping rules
// (ADR-0071 am #2): a listed name must match a configured route
// (explicit `name:` or the derived host:path/path label — "/" is the
// derived name of the path_prefix "/" route above), an empty list is
// the valid fail-closed shape, and an empty-string entry is rejected
// rather than silently selecting nothing.
func TestNearCacheConfig_RoutesAllowlist(t *testing.T) {
	t.Parallel()
	t.Run("derived route name is selectable", func(t *testing.T) {
		t.Parallel()
		cfg, err := Parse([]byte(nearConfigYAML(`
experimental:
  near_cache:
    enabled: true
    routes: ["/"]
`)))
		if err != nil {
			t.Fatalf("the derived route name must validate: %v", err)
		}
		if got := cfg.Experimental.NearCache.Routes; len(got) != 1 || got[0] != "/" {
			t.Fatalf("routes: got %v", got)
		}
	})

	t.Run("explicit route name is selectable", func(t *testing.T) {
		t.Parallel()
		yaml := `
listen:
  admin: ":9000"
upstream_pools:
  - name: app
    targets: ["a:1"]
routes:
  - name: reviews
    match: { path_prefix: "/reviews/" }
    pool: app
experimental:
  near_cache:
    enabled: true
    routes: ["reviews"]
`
		if _, err := Parse([]byte(yaml)); err != nil {
			t.Fatalf("an explicit route name must validate: %v", err)
		}
	})

	t.Run("empty list is the valid fail-closed shape", func(t *testing.T) {
		t.Parallel()
		// No routes key at all: enabled with an empty allowlist must
		// load (the engine logs the inert warning), not fail.
		if _, err := Parse([]byte(nearConfigYAML(`
experimental:
  near_cache:
    enabled: true
`))); err != nil {
			t.Fatalf("an empty allowlist is the fail-closed rollout shape, not an error: %v", err)
		}
	})

	t.Run("unknown route name is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := Parse([]byte(nearConfigYAML(`
experimental:
  near_cache:
    enabled: true
    routes: ["ghost"]
`)))
		if err == nil {
			t.Fatal("a typo in the allowlist must fail the load, not silently de-scope the route")
		}
		if !strings.Contains(err.Error(), "ghost") || !strings.Contains(err.Error(), "near_cache.routes") {
			t.Fatalf("error %q must name the offending allowlist entry", err)
		}
	})

	t.Run("empty string entry is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := Parse([]byte(nearConfigYAML(`
experimental:
  near_cache:
    enabled: true
    routes: [""]
`)))
		if err == nil {
			t.Fatal("an empty allowlist entry must fail the load")
		}
		if !strings.Contains(err.Error(), "near_cache.routes") {
			t.Fatalf("error %q must point at the allowlist", err)
		}
	})
}
