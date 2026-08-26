// Package observability is the L7 layer. It centralizes structured logging,
// Prometheus metrics, data-plane RED counters, and pprof wiring.
// OTEL traces are planned.
package observability

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// Options controls the slog logger created by New.
//
// Stable.
type Options struct {
	// Output is the io.Writer to write to. nil defaults to os.Stdout.
	Output io.Writer
	// Level is the minimum log level. Empty defaults to "info".
	Level string
	// Format is "json" (default) or "text".
	Format string
}

// New returns a configured *slog.Logger. It never returns nil; invalid
// options fall back to safe defaults.
//
// Stable.
func New(opts Options) *slog.Logger {
	level := parseLevel(opts.Level)

	out := opts.Output
	if out == nil {
		out = os.Stdout
	}

	handlerOpts := &slog.HandlerOptions{
		Level: level,
	}

	var handler slog.Handler
	switch strings.ToLower(opts.Format) {
	case "text":
		handler = slog.NewTextHandler(out, handlerOpts)
	default:
		handler = slog.NewJSONHandler(out, handlerOpts)
	}

	return slog.New(handler)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
