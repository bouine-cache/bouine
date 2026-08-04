package observability

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_DefaultsToInfoJSON(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := New(Options{Output: &buf})

	log.Debug("hidden")
	log.Info("hello", "k", "v")

	out := strings.TrimSpace(buf.String())
	require.False(t, strings.Contains(out, "hidden"))

	// JSON parse the info line.
	var rec map[string]any
	{
		err := json.Unmarshal([]byte(out), &rec)
		require.NoErrorf(t, err, "expected JSON output, got %q: %v", out, err)
	}
	if rec["msg"] != "hello" || rec["k"] != "v" {
		t.Fatalf("unexpected record: %v", rec)
	}
}

func TestNew_TextFormat(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := New(Options{Format: "text", Output: &buf})
	log.Info("plain")

	require.Contains(t, buf.String(), "msg=plain")
}

func TestParseLevel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want string
	}{
		{"", "INFO"},
		{"info", "INFO"},
		{" info ", "INFO"}, // trimmed
		{"DEBUG", "DEBUG"},
		{"warning", "WARN"},
		{"warn", "WARN"},
		{"error", "ERROR"},
		{"bogus", "INFO"},
	}
	for _, tc := range cases {
		got := parseLevel(tc.in).String()
		assert.Equal(t, tc.want, got)
	}
}
