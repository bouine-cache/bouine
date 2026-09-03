package header

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsEventStreamContentType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		ct   string
		want bool
	}{
		{"exact", "text/event-stream", true},
		{"with charset", "text/event-stream; charset=utf-8", true},
		{"charset no space", "text/event-stream;charset=utf-8", true},
		{"trailing space", "text/event-stream ", true},
		{"leading OWS", "  text/event-stream", true},
		{"uppercase type", "Text/Event-Stream", true},
		{"mixed case subtype", "text/Event-Stream", true},
		{"case with params", "TEXT/EVENT-STREAM; charset=UTF-8", true},
		{"suffix is different type", "text/event-streamx", false},
		{"suffix with param", "text/event-stream-v2; charset=utf-8", false},
		{"plain text", "text/plain", false},
		{"json", "application/json", false},
		{"html", "text/html; charset=utf-8", false},
		{"empty", "", false},
		{"prefix only", "text/event", false},
		{"x prefix", "x-text/event-stream", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, IsEventStreamContentType([]byte(tc.ct)))
		})
	}
}

func TestAcceptsEventStream(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		accept string
		want   bool
	}{
		{"exact", "text/event-stream", true},
		{"among others", "application/json, text/event-stream, */*", true},
		{"with q-value", "text/event-stream;q=0.9", true},
		{"with charset", "text/event-stream; charset=utf-8", true},
		{"case insensitive", "TEXT/EVENT-STREAM", true},
		{"wildcard only", "*/*", false},
		{"json only", "application/json", false},
		{"browser default", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", false},
		{"empty", "", false},
		{"suffix trap", "text/event-stream-v2", false},
		{"second range matches", "*/*, text/event-stream", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, AcceptsEventStream([]byte(tc.accept)))
		})
	}
}

// TestMatchersZeroAlloc pins the zero-allocation contract: both matchers
// walk the input bytes in place (AGENTS.md §7 hot-path discipline — the
// Accept matcher runs on every non-hit request). Not parallel:
// AllocsPerRun must run on the main test goroutine.
func TestMatchersZeroAlloc(t *testing.T) {
	accept := []byte("application/json, text/event-stream, */*")
	ct := []byte("text/event-stream; charset=utf-8")
	allocs := testing.AllocsPerRun(100, func() {
		_ = AcceptsEventStream(accept)
		_ = IsEventStreamContentType(ct)
	})
	require.Zero(t, allocs)
}
