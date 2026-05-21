package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaults_AdminListenerEnabled(t *testing.T) {
	d := Defaults()
	if d.Listen.Admin == "" {
		t.Fatal("admin listener should be enabled by default")
	}
}

func TestParse_EmptyYieldsDefaults(t *testing.T) {
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Listen.Admin == "" {
		t.Fatal("expected admin default")
	}
}

func TestParse_RejectsUnknownKeys(t *testing.T) {
	_, err := Parse([]byte("nonsensical_field: 1\n"))
	if err == nil {
		t.Fatal("expected error on unknown key")
	}
}

func TestParse_RejectsDuplicatePool(t *testing.T) {
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
	yamlSrc := `
listen:
  http:  ":80"
  https: ":443"
  admin: ":9000"
storage:
  hot_max_bytes: 2Go
  warm_max_bytes: 50Go
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
