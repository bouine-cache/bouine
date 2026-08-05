package observability

import (
	"log/slog"
	"sync/atomic"

	"github.com/bouine-cache/bouine/pkg/api"
)

// DefaultKeySampleRate is the default 1-in-N sampling rate for
// Info-level log records.
const DefaultKeySampleRate uint64 = 1000

// Logger is the structured logging interface used throughout bouine.
// Both *slog.Logger, *SampledLogger, and NoopLogger satisfy it.
// Components receive a Logger from their caller (typically the engine)
// and never create their own. Tests use NoopLogger when they don't
// need to assert on log output.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
}

// NoopLogger is a Logger that discards all records. It is the zero
// value for the Logger interface in tests and in components that are
// constructed without a logger. Using NoopLogger instead of nil means
// every component can call logger.Info(...) unconditionally — no
// nil-checks, no defensive fallbacks.
type NoopLogger struct{}

// Info implements Logger by discarding the record.
func (NoopLogger) Info(string, ...any) {}

// Warn implements Logger by discarding the record.
func (NoopLogger) Warn(string, ...any) {}

// Error implements Logger by discarding the record.
func (NoopLogger) Error(string, ...any) {}

// Debug implements Logger by discarding the record.
func (NoopLogger) Debug(string, ...any) {}

// ResolveLogger returns l if non-nil, otherwise NoopLogger{}. Use it
// in constructors to default a nil Logger field in one line:
//
//	logger := observability.ResolveLogger(cfg.Logger)
func ResolveLogger(l Logger) Logger {
	if l == nil {
		return NoopLogger{}
	}
	return l
}

// SampledLogger wraps a slog.Logger with deterministic sampling.
// Info is always sampled: if a "key" attribute is present, sampling
// is deterministic (key % sampleRate); otherwise a counter-based
// 1-in-sampleRate fallback is used. Warn, Error, and Debug are never
// sampled.
type SampledLogger struct {
	logger     *slog.Logger
	sampleRate uint64
	counter    atomic.Uint64
}

// NewSampledLogger wraps base with key-based sampling at
// 1-in-sampleRate. A sampleRate of 0 disables sampling (every
// key is logged). base must be non-nil; callers that don't need
// logging should use NoopLogger instead.
func NewSampledLogger(base *slog.Logger, sampleRate uint64) *SampledLogger {
	return &SampledLogger{logger: base, sampleRate: sampleRate}
}

// Info emits an info record. All Info records are sampled at
// 1-in-sampleRate: deterministically by key when a "key" attribute
// is present, or by counter otherwise. The sampling check is a
// linear scan of args — negligible cost relative to the slog call
// that follows.
func (l *SampledLogger) Info(msg string, args ...any) {
	if l.sampleRate != 0 {
		if key, ok := extractKey(args); ok {
			if key.Hash%l.sampleRate != 0 {
				return
			}
		} else if l.counter.Add(1)%l.sampleRate != 0 {
			return
		}
	}
	l.logger.Info(msg, args...)
}

// Warn emits a warn record. Never sampled.
func (l *SampledLogger) Warn(msg string, args ...any) { l.logger.Warn(msg, args...) }

// Error emits an error record. Never sampled.
func (l *SampledLogger) Error(msg string, args ...any) { l.logger.Error(msg, args...) }

// Debug emits a debug record. Never sampled.
func (l *SampledLogger) Debug(msg string, args ...any) { l.logger.Debug(msg, args...) }

// extractKey scans args for a "key" attribute pair and returns its
// api.Key value. args follow slog's alternating key-value convention.
func extractKey(args []any) (api.Key, bool) {
	for i := 0; i+1 < len(args); i += 2 {
		if s, ok := args[i].(string); ok && s == "key" {
			if k, ok := args[i+1].(api.Key); ok {
				return k, true
			}
		}
	}
	return api.Key{}, false
}
