package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWatcher_Reload_Success(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")

	// Write a minimal valid config.
	if err := os.WriteFile(cfgPath, []byte("listen:\n  http: \":8080\"\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	var received *Config
	w := NewWatcher(WatcherConfig{
		ConfigPath: cfgPath,
		OnConfig:   func(cfg *Config) { received = cfg },
	})

	if err := w.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if received == nil {
		t.Fatal("OnConfig was not called")
	}
	if received.Listen.HTTP != ":8080" {
		t.Fatalf("HTTP addr = %q, want :8080", received.Listen.HTTP)
	}
}

func TestWatcher_Reload_ParseError(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bad.yaml")

	if err := os.WriteFile(cfgPath, []byte(": not: valid: yaml::\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	called := false
	w := NewWatcher(WatcherConfig{
		ConfigPath: cfgPath,
		OnConfig:   func(_ *Config) { called = true },
	})

	err := w.Reload()
	if err == nil {
		t.Fatal("expected error for bad YAML, got nil")
	}
	if called {
		t.Fatal("OnConfig must not be called on parse error")
	}
}

func TestWatcher_Reload_NoOp_WhenUnconfigured(t *testing.T) {
	w := NewWatcher(WatcherConfig{})
	if err := w.Reload(); err != nil {
		t.Fatalf("Reload with no config path: %v", err)
	}
}
