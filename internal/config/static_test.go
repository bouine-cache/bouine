package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRoute_StaticOnly(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Listen: Listen{Admin: ":9000"},
		Routes: []Route{
			{Static: StaticConfig{Root: "/var/www"}},
		},
	}
	{
		err := cfg.Validate()
		require.NoErrorf(t, err, "static-only route should validate: %v", err)
	}
}

func TestValidateRoute_BothPoolAndStatic(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Listen: Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{
			{Name: "origin", Targets: []string{"localhost:8080"}},
		},
		Routes: []Route{
			{Pool: "origin", Static: StaticConfig{Root: "/var/www"}},
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
}

func TestValidateRoute_NeitherPoolNorStatic(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Listen: Listen{Admin: ":9000"},
		Routes: []Route{
			{Name: "empty"},
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
}

func TestValidateRoute_StaticRelativeRoot(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Listen: Listen{Admin: ":9000"},
		Routes: []Route{
			{Static: StaticConfig{Root: "relative/path"}},
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
}

func TestValidateRoute_StaticIndexWithSlash(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Listen: Listen{Admin: ":9000"},
		Routes: []Route{
			{Static: StaticConfig{Root: "/var/www", Index: []string{"sub/index.html"}}},
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
}

func TestValidateRoute_StaticNegativeMaxFileSize(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Listen: Listen{Admin: ":9000"},
		Routes: []Route{
			{Static: StaticConfig{Root: "/var/www", MaxFileSize: ByteSize(-1)}},
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
}

func TestValidateRoute_StaticWithPoolStillWorks(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Listen: Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{
			{Name: "origin", Targets: []string{"localhost:8080"}},
		},
		Routes: []Route{
			{Match: RouteMatch{PathPrefix: "/api/"}, Pool: "origin"},
			{Match: RouteMatch{PathPrefix: "/assets/"}, Static: StaticConfig{Root: "/var/www/assets"}},
		},
	}
	{
		err := cfg.Validate()
		require.NoErrorf(t, err, "mixed pool + static routes should validate: %v", err)
	}
}

func TestParse_StaticRoute(t *testing.T) {
	t.Parallel()
	yaml := `
listen:
  admin: ":9000"
routes:
  - name: assets
    match: { path_prefix: /assets/ }
    static:
      root: /var/www/assets
    request:
      strip_prefix: /assets/
`
	cfg, err := Parse([]byte(yaml))
	require.NoErrorf(t, err, "Parse: %v", err)
	require.Len(t, cfg.Routes, 1)
	require.Equal(t, "/var/www/assets", cfg.Routes[0].Static.Root)
	require.Equal(t, "/assets/", cfg.Routes[0].Request.StripPrefix)
}
