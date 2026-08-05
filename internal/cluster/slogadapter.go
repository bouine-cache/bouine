package cluster

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bouine-cache/bouine/internal/observability"
)

// slogAdapter bridges memberlist's stdlib *log.Logger output into slog,
// parsing [LEVEL] tokens and re-emitting with component=memberlist.
// handlerQueueFullMsg is the exact substring memberlist logs when a
// per-peer handoff queue overflows. Anchored to memberlist@v0.6.0
// net.go:472:
//
//	m.logger.Printf("[WARN] memberlist: handler queue full, dropping message (%d) %s", ...)
//
// If a memberlist upgrade changes this wording, the gossip-drops
// metric silently stops counting. Update this constant and the
// corresponding test corpus when bumping memberlist.
const handlerQueueFullMsg = "handler queue full"

type slogAdapter struct {
	logger    observability.Logger
	component string
	// metrics is read atomically so that SetMetrics can update it
	// concurrently with memberlist's logging goroutine, which starts
	// inside memberlist.Create (before the caller can call SetMetrics).
	metrics atomic.Pointer[Metrics]

	mu  sync.Mutex
	buf bytes.Buffer
}

// newSlogAdapter returns an io.Writer that forwards memberlist log lines
// to logger as structured slog records tagged with component=memberlist.
// The returned adapter holds an atomic metrics pointer; call setMetrics
// before memberlist starts logging to ensure drops are counted.
func newSlogAdapter(logger observability.Logger) *slogAdapter {
	return &slogAdapter{logger: logger, component: "memberlist"}
}

// setMetrics atomically sets the metrics pointer. Safe to call
// concurrently with Write — the pointer is read atomically in emit.
func (a *slogAdapter) setMetrics(m *Metrics) {
	a.metrics.Store(m)
}

// Write implements io.Writer, buffering partial lines until a newline arrives.
func (a *slogAdapter) Write(p []byte) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.buf.Write(p)
	for {
		line, ok := a.readLine()
		if !ok {
			break
		}
		a.emit(line)
	}
	return len(p), nil
}

// readLine returns the next complete line from the buffer.
func (a *slogAdapter) readLine() (string, bool) {
	idx := bytes.IndexByte(a.buf.Bytes(), '\n')
	if idx < 0 {
		return "", false
	}
	line := string(a.buf.Next(idx + 1))
	return strings.TrimRight(line, "\r\n"), true
}

// emit parses and forwards a single memberlist log line.
func (a *slogAdapter) emit(line string) {
	if line == "" {
		return
	}
	level, msg := parseMemberlistLine(line)
	switch level {
	case "WARN":
		a.logger.Warn(msg, "component", a.component)
	case "ERR", "ERROR":
		a.logger.Error(msg, "component", a.component)
	case "DEBUG":
		a.logger.Debug(msg, "component", a.component)
	default: // INFO and anything unrecognised
		a.logger.Info(msg, "component", a.component)
	}
	if level == "WARN" && strings.Contains(msg, handlerQueueFullMsg) {
		if m := a.metrics.Load(); m != nil {
			m.IncGossipDrop()
		}
	}
}

// parseMemberlistLine strips the log.LstdFlags timestamp and parses the [LEVEL]
// token, anchoring after the timestamp to avoid misparsing brackets in messages.
// The "memberlist: " prefix is stripped. Unrecognised lines fall back to INFO.
func parseMemberlistLine(line string) (level, msg string) {
	// log.LstdFlags emits "2006/01/02 15:04:05 " (20 chars) then the payload.
	const tsLen = 20
	payload := line
	if len(line) >= tsLen {
		payload = line[tsLen:]
	}
	if !strings.HasPrefix(payload, "[") {
		return "INFO", strings.TrimSpace(payload)
	}
	endBracket := strings.Index(payload, "]")
	if endBracket < 0 {
		return "INFO", strings.TrimSpace(payload)
	}
	level = strings.TrimSpace(payload[1:endBracket])
	rest := strings.TrimSpace(payload[endBracket+1:])
	rest = strings.TrimPrefix(rest, "memberlist: ")
	return level, rest
}
