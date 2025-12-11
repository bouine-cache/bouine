package observability

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNew_DefaultsToInfoJSON(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Output: &buf})

	log.Debug("hidden")
	log.Info("hello", "k", "v")

	out := strings.TrimSpace(buf.String())
	if strings.Contains(out, "hidden") {
		t.Fatalf("debug should not appear at default level: %q", out)
	}

	// JSON parse the info line.
	var rec map[string]any
	if err := json.Unmarshal([]byte(out), &rec); err != nil {
		t.Fatalf("expected JSON output, got %q: %v", out, err)
	}
	if rec["msg"] != "hello" || rec["k"] != "v" {
		t.Fatalf("unexpected record: %v", rec)
	}
}

func TestNew_TextFormat(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Format: "text", Output: &buf})
	log.Info("plain")

	if !strings.Contains(buf.String(), "msg=plain") {
		t.Fatalf("expected text format, got %q", buf.String())
	}
}

func TestParseLevel(t *testing.T) {
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
		if got != tc.want {
			t.Errorf("parseLevel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
