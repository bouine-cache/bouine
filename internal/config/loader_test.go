package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaults_AdminListenerEnabled(t *testing.T) {
	t.Parallel()
	d := Defaults()
	if d.Listen.Admin == "" {
		t.Fatal("admin listener should be enabled by default")
	}
}

func TestParse_EmptyYieldsDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Listen.Admin == "" {
		t.Fatal("expected admin default")
	}
}

func TestParse_RejectsUnknownKeys(t *testing.T) {
	t.Parallel()
	_, err := Parse([]byte("nonsensical_field: 1\n"))
	if err == nil {
		t.Fatal("expected error on unknown key")
	}
}

func TestParse_RejectsDuplicatePool(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
  - name: app
    targets: [b:1]
`
	_, err := Parse([]byte(yamlSrc))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate-pool error, got %v", err)
	}
}

func TestParse_RejectsUnknownPoolInRoute(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { host: example.com }
    pool: missing
`
	_, err := Parse([]byte(yamlSrc))
	if err == nil || !strings.Contains(err.Error(), "unknown pool") {
		t.Fatalf("expected unknown-pool error, got %v", err)
	}
}

func TestParse_HappyPath(t *testing.T) {
	t.Parallel()
	yamlSrc := `
listen:
  http:  ":80"
  https: ":443"
  admin: ":9000"
storage:
  hot_max_bytes: 2Go
  warm_max_bytes: 20Go
upstream_pools:
  - name: app
    targets: [app.local:8080]
    health:
      active:
        path: /healthz
        interval: 5s
        timeout: 1s
        unhealthy_threshold: 3
routes:
  - match: { host: api.example.com }
    pool: app
    cache:
      ttl_default: 60s
      stale_while_revalidate: 30s
`
	cfg, err := Parse([]byte(yamlSrc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := cfg.Storage.HotMaxBytes.Bytes(); got != 2_000_000_000 {
		t.Fatalf("HotMaxBytes = %d, want %d", got, 2_000_000_000)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].Pool != "app" {
		t.Fatalf("unexpected routes: %+v", cfg.Routes)
	}
	if cfg.UpstreamPools[0].Health.Active.Interval.Seconds() != 5 {
		t.Fatalf("interval = %v", cfg.UpstreamPools[0].Health.Active.Interval)
	}
}

func TestLoad_FromDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	if err := os.WriteFile(path, []byte("listen:\n  admin: ':9001'\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Listen.Admin != ":9001" {
		t.Fatalf("admin = %q", cfg.Listen.Admin)
	}
}

func TestByteSize_Forms(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"1024", 1024},
		{"1KB", 1000},
		{"1KiB", 1024},
		{"2GiB", 2 << 30},
		{"1.5MiB", int64(1.5 * (1 << 20))},
		{"128Mi", 128 << 20},
		{"1Gi", 1 << 30},
		{"4Ki", 4 << 10},
		{"1Ti", 1 << 40},
		{"1Ko", 1000},
		{"512Mo", 512_000_000},
		{"2Go", 2_000_000_000},
		{"1To", 1_000_000_000_000},
	}
	for _, tc := range cases {
		var b ByteSize
		yamlSrc := []byte("hot_max_bytes: " + tc.in + "\n")
		var s Storage
		if err := yamlUnmarshal(t, yamlSrc, &s); err != nil {
			t.Errorf("parse %q: %v", tc.in, err)
			continue
		}
		b = s.HotMaxBytes
		if b.Bytes() != tc.want {
			t.Errorf("%q -> %d, want %d", tc.in, b.Bytes(), tc.want)
		}
	}
}

func TestClusterMode_DefaultIsStrong(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	if cfg.Cluster.Mode != ClusterModeStrong {
		t.Fatalf("default cluster mode = %q, want %q", cfg.Cluster.Mode, ClusterModeStrong)
	}
}

func TestClusterMode_EmptyDefaultsToStrong(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: true}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Cluster.Mode != ClusterModeStrong {
		t.Fatalf("empty mode = %q, want %q", cfg.Cluster.Mode, ClusterModeStrong)
	}
}

func TestClusterMode_ValidModes(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{ClusterModeStrong, ClusterModeEventual, ClusterModeFull} {
		cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: true, Mode: mode}}
		if err := cfg.Validate(); err != nil {
			t.Errorf("mode %q: unexpected error: %v", mode, err)
		}
		if cfg.Cluster.Mode != mode {
			t.Errorf("mode %q: got %q", mode, cfg.Cluster.Mode)
		}
	}
}

func TestClusterMode_InvalidValue(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: true, Mode: "invalid"}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid cluster mode")
	}
	if !strings.Contains(err.Error(), "cluster.mode must be") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterMode_NonStrongRequiresEnabled(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: false, Mode: ClusterModeEventual}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for eventual mode without cluster enabled")
	}
	if !strings.Contains(err.Error(), "requires cluster.enabled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterMode_StrongWithoutEnabled(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: false, Mode: ClusterModeStrong}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("strong mode without enabled should be valid: %v", err)
	}
}

// --- TTLOverride validation ---

func TestParse_TTLOverride_ValidYAML(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { host: example.com }
    pool: app
    cache:
      ttl_override: 1h
      ttl_default:  30s
`
	cfg, err := Parse([]byte(yamlSrc))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected 1 route")
	}
	if cfg.Routes[0].Cache.TTLOverride != 1*60*60*1e9 {
		t.Errorf("TTLOverride = %v, want 1h", cfg.Routes[0].Cache.TTLOverride)
	}
}

func TestValidate_TTLOverride_NegativeRejected(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	route := Route{Pool: "app", Cache: RouteCache{TTLOverride: -1}}
	cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "ttl_override") {
		t.Fatalf("expected ttl_override validation error, got %v", err)
	}
}
