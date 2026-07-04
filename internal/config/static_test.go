package config

import (
	"testing"
)

func TestValidateRoute_StaticOnly(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Listen: Listen{Admin: ":9000"},
		Routes: []Route{
			{Static: StaticConfig{Root: "/var/www"}},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("static-only route should validate: %v", err)
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
	if err == nil {
		t.Fatal("route with both pool and static should fail validation")
	}
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
	if err == nil {
		t.Fatal("route with neither pool nor static should fail validation")
	}
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
	if err == nil {
		t.Fatal("static.root must be absolute")
	}
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
	if err == nil {
		t.Fatal("static.index with '/' should fail validation")
	}
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
	if err == nil {
		t.Fatal("static.max_file_size < 0 should fail validation")
	}
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
	if err := cfg.Validate(); err != nil {
		t.Fatalf("mixed pool + static routes should validate: %v", err)
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
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(cfg.Routes))
	}
	if cfg.Routes[0].Static.Root != "/var/www/assets" {
		t.Fatalf("static.root: got %q", cfg.Routes[0].Static.Root)
	}
	if cfg.Routes[0].Request.StripPrefix != "/assets/" {
		t.Fatalf("strip_prefix: got %q", cfg.Routes[0].Request.StripPrefix)
	}
}
