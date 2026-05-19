// Package admin is the L7 control plane. It serves the admin API,
// health/readiness probes, metrics, and (in later phases) the
// dashboard SPA. The admin surface MUST stay on its own listener; it
// is never bound on the data-plane port (see AGENTS.md §2.2).
//
// In phase 0 only /healthz and /readyz are wired so that K8s probes
// pass during early development. Purge, ban, refresh, config and the
// dashboard land in phase 4 / 6.
package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"

	"github.com/thylong/bouine/internal/buildinfo"
	"github.com/thylong/bouine/internal/observability"
)

// Config controls the admin Fiber app.
//
// Stable.
type Config struct {
	// Addr is the listen address (e.g. ":9000"). Empty defaults to
	// ":9000".
	Addr string
	// Logger is the structured logger. Required.
	Logger *slog.Logger
	// ReadyFn reports whether the server is ready to serve traffic.
	// nil is treated as "always ready" (sensible default during phase 0).
	ReadyFn func() bool
	// Metrics is the Prometheus registry. If non-nil, /metrics is
	// mounted.
	Metrics *observability.Metrics
}

// Server is the admin Fiber app wrapped with its lifecycle methods.
//
// Stable.
type Server struct {
	cfg Config
	app *fiber.App
}

// New constructs the admin server. It does not start listening; call
// Serve.
//
// Stable.
func New(cfg Config) *Server {
	if cfg.Addr == "" {
		cfg.Addr = ":9000"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	app := fiber.New(fiber.Config{
		AppName: "bouine-admin",
		// Strict admin contract: short read/write deadlines.
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})

	s := &Server{cfg: cfg, app: app}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.app.Get("/healthz", s.healthz)
	s.app.Get("/readyz", s.readyz)
	s.app.Get("/version", s.version)
	if s.cfg.Metrics != nil {
		s.app.Get("/metrics", adaptor.HTTPHandler(s.cfg.Metrics.Handler()))
	}
}

func (s *Server) healthz(c fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok"})
}

func (s *Server) readyz(c fiber.Ctx) error {
	if s.cfg.ReadyFn != nil && !s.cfg.ReadyFn() {
		return c.Status(http.StatusServiceUnavailable).
			JSON(fiber.Map{"status": "not-ready"})
	}
	return c.JSON(fiber.Map{"status": "ready"})
}

func (s *Server) version(c fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"version": buildinfo.Version,
		"commit":  buildinfo.Commit,
		"date":    buildinfo.Date,
	})
}

// App returns the underlying Fiber app for testing.
//
// Unstable.
func (s *Server) App() *fiber.App {
	return s.app
}

// Serve blocks until the server returns or ctx is cancelled. On context
// cancellation a graceful shutdown is initiated.
//
// Stable.
func (s *Server) Serve(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.cfg.Logger.Info("admin server listening", "addr", s.cfg.Addr)
		errCh <- s.app.Listen(s.cfg.Addr, fiber.ListenConfig{
			DisableStartupMessage: true,
		})
	}()

	select {
	case <-ctx.Done():
		// Detach from the parent ctx (already cancelled) but bound the
		// shutdown by a fresh timeout.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := s.app.ShutdownWithContext(shutdownCtx); err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
