package config

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fieldErrs extracts every *FieldError from a joined validation error.
func fieldErrs(t *testing.T, err error) []*FieldError {
	t.Helper()
	require.Error(t, err)
	var out []*FieldError
	for _, e := range unwrapAll(err) {
		var fe *FieldError
		if errors.As(e, &fe) {
			out = append(out, fe)
		}
	}
	return out
}

// validBase returns a minimal valid config each test mutates.
func validBase() Config {
	return Config{
		Listen:        Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{{Name: "app", Targets: []string{"a:1"}}},
		Routes:        []Route{{Name: "r0", Pool: "app", Cache: RouteCache{TTLDefault: 60}}},
	}
}

func TestValidate_FieldError_SinglePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{
			"listen read_timeout upper bound",
			func(c *Config) { c.Listen.ReadTimeout = 10 * time.Minute },
			"listen.read_timeout",
		},
		{
			"experimental cross-field",
			func(c *Config) { c.Experimental.H1Reactor = true },
			"experimental.h1_reactor",
		},
		{
			"gogc invalid",
			func(c *Config) { g := 0; c.GOGC = &g },
			"gogc",
		},
		{
			"route cache jitter range",
			func(c *Config) { c.Routes[0].Cache.JitterPercent = 99 },
			"routes[0].cache.jitter_percent",
		},
		{
			"route cache key cross-field",
			func(c *Config) {
				c.Routes[0].Cache.Key = RouteKey{
					KeepQueryParams:  []string{"a"},
					StripQueryParams: []string{"b"},
				}
			},
			"routes[0].cache.key.keep_query_params",
		},
		{
			"route references unknown pool",
			func(c *Config) { c.Routes[0].Pool = "nope" },
			"routes[0].pool",
		},
		{
			"upstream pool duration",
			func(c *Config) { c.UpstreamPools[0].Connect.Timeout = -1 },
			"upstream_pools[0].connect.timeout",
		},
		{
			"cluster ban_ttl floor",
			func(c *Config) {
				c.Listen.Cluster = ":7946"
				c.Cluster.BanTTL = 500
			},
			"cluster.ban_ttl",
		},
		{
			"storage warm sync interval sentinel",
			func(c *Config) { c.Storage.WarmSyncInterval = -2 },
			"storage.warm_sync_interval",
		},
		{
			"admin idle timeout negative",
			func(c *Config) { c.Admin.IdleTimeout = -1 },
			"admin.idle_timeout",
		},
		{
			"static index contains slash",
			func(c *Config) {
				c.Routes[0].Pool = ""
				c.Routes[0].Static = StaticConfig{Root: "/srv", Index: []string{"a/b"}}
			},
			"routes[0].static.index[0]",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			tc.mut(&cfg)
			err := cfg.Validate()
			fes := fieldErrs(t, err)
			require.Len(t, fes, 1, "expected exactly one field error, got: %v", err)
			assert.Equal(t, tc.want, fes[0].Path)
			assert.NotEmpty(t, fes[0].Message)
		})
	}
}

func TestValidate_FieldError_MultiError(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Listen.ReadTimeout = -1
	cfg.Routes[0].Cache.JitterPercent = 99
	cfg.Routes = append(cfg.Routes, Route{Name: "r1", Pool: "app", Cache: RouteCache{FetchTimeout: -1}})
	err := cfg.Validate()
	require.Error(t, err)
	fes := fieldErrs(t, err)
	require.Len(t, fes, 3)
	paths := []string{fes[0].Path, fes[1].Path, fes[2].Path}
	assert.ElementsMatch(t, []string{
		"listen.read_timeout",
		"routes[0].cache.jitter_percent",
		"routes[1].cache.fetch_timeout",
	}, paths)
}

func TestFieldError_ErrorFormat(t *testing.T) {
	t.Parallel()
	fe := &FieldError{Path: "routes[2].cache.fetch_timeout", Message: "must be >= 0, got -1s"}
	assert.Equal(t, "config: routes[2].cache.fetch_timeout: must be >= 0, got -1s", fe.Error())
}

func TestValidate_AllValid_NoFieldErrors(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Storage = Storage{WarmDir: "/tmp/warm", WarmSyncInterval: -1, WALSyncInterval: -1}
	assert.NoError(t, cfg.Validate())
}
