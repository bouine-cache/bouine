// Package reload provides hot-reload of the bouine config file and
// TLS certificates. It watches for file changes via fsnotify and
// also listens for SIGHUP.
//
// The caller provides a callback; reload parses the new config and
// calls the callback atomically. If parsing fails, the old config
// stays in effect and the error is logged.
package config

import (
	"context"
	"crypto/tls"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/fsnotify/fsnotify"
)

// Watcher watches a config file and optional TLS cert/key files for
// changes and invokes callbacks on reload.
type Watcher struct {
	configPath string
	certPaths  []string
	logger     *slog.Logger

	mu       sync.RWMutex
	cfg      *Config
	tlsCfg   *tls.Config
	onConfig func(*Config)
	onTLS    func(*tls.Config)
}

// WatcherConfig configures a Watcher.
type WatcherConfig struct {
	ConfigPath string
	CertPaths  []string // cert + key files to watch
	Logger     *slog.Logger
	OnConfig   func(*Config)
	OnTLS      func(*tls.Config)
}

// NewWatcher creates a reload watcher. Call Run to start.
func NewWatcher(cfg WatcherConfig) *Watcher {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Watcher{
		configPath: cfg.ConfigPath,
		certPaths:  cfg.CertPaths,
		logger:     cfg.Logger,
		onConfig:   cfg.OnConfig,
		onTLS:      cfg.OnTLS,
	}
}

// Run starts watching. Blocks until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = watcher.Close() }()

	// Watch config file.
	if w.configPath != "" {
		if err := watcher.Add(w.configPath); err != nil {
			w.logger.Warn("cannot watch config", "path", w.configPath, "error", err)
		}
	}
	// Watch TLS cert/key files.
	for _, p := range w.certPaths {
		if err := watcher.Add(p); err != nil {
			w.logger.Warn("cannot watch cert", "path", p, "error", err)
		}
	}

	// Also listen for SIGHUP.
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	defer signal.Stop(sighup)

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
				w.handleFileChange(event.Name)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			w.logger.Warn("fsnotify error", "error", err)
		case <-sighup:
			w.logger.Info("SIGHUP received, reloading")
			w.reloadConfig()
			w.reloadTLS()
		}
	}
}

func (w *Watcher) handleFileChange(path string) {
	if path == w.configPath {
		w.reloadConfig()
		return
	}
	for _, p := range w.certPaths {
		if path == p {
			w.reloadTLS()
			return
		}
	}
}

func (w *Watcher) reloadConfig() {
	if w.configPath == "" || w.onConfig == nil {
		return
	}
	cfg, err := Load(w.configPath)
	if err != nil {
		w.logger.Error("config reload failed", "error", err)
		return
	}
	w.mu.Lock()
	w.cfg = cfg
	w.mu.Unlock()
	w.onConfig(cfg)
	w.logger.Info("config reloaded", "path", w.configPath)
}

func (w *Watcher) reloadTLS() {
	if len(w.certPaths) < 2 || w.onTLS == nil {
		return
	}
	// certPaths[0] = cert, certPaths[1] = key.
	cert, err := tls.LoadX509KeyPair(w.certPaths[0], w.certPaths[1])
	if err != nil {
		w.logger.Error("TLS reload failed", "error", err)
		return
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	w.mu.Lock()
	w.tlsCfg = tlsCfg
	w.mu.Unlock()
	w.onTLS(tlsCfg)
	w.logger.Info("TLS certs reloaded")
}
