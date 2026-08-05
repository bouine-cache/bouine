package cluster

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
)

// captureLogger builds a SampledLogger (sampleRate=0 → no sampling) writing
// JSON to a thread-safe buffer, mirroring the pattern in
// internal/storage/hot_logging_test.go. Returns the logger and a helper to
// drain and parse the emitted records.
func captureLogger(t *testing.T) (observability.Logger, *sync.Mutex, *bytes.Buffer) {
	t.Helper()
	var mu sync.Mutex
	var buf bytes.Buffer
	l := observability.NewSampledLogger(
		slog.New(slog.NewJSONHandler(&muWriter{mu: &mu, buf: &buf}, &slog.HandlerOptions{Level: slog.LevelDebug})),
		0,
	)
	return l, &mu, &buf
}

type muWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w *muWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func parseAdapterRecords(t *testing.T, mu *sync.Mutex, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	var records []map[string]any
	for _, line := range bytes.Split(buf.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		err := json.Unmarshal(line, &rec)
		require.NoErrorf(t, err, "unmarshal log line: %v\nline: %s", err, line)
		records = append(records, rec)
	}
	return records
}

func TestParseMemberlistLine(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		line    string
		wantLvl string
		wantMsg string
	}{
		{
			name:    "warn with timestamp and memberlist prefix",
			line:    "2026/07/05 09:37:34 [WARN] memberlist: Was able to connect to bouine-3 over TCP but UDP probes failed, network may be misconfigured",
			wantLvl: "WARN",
			wantMsg: "Was able to connect to bouine-3 over TCP but UDP probes failed, network may be misconfigured",
		},
		{
			name:    "err with prefix",
			line:    "2026/07/05 09:37:35 [ERR] memberlist: Failed to encode message for broadcast: eof",
			wantLvl: "ERR",
			wantMsg: "Failed to encode message for broadcast: eof",
		},
		{
			name:    "debug with prefix",
			line:    "2026/07/05 09:37:36 [DEBUG] memberlist: Using dynamic bind port 42321",
			wantLvl: "DEBUG",
			wantMsg: "Using dynamic bind port 42321",
		},
		{
			name:    "info with prefix",
			line:    "2026/07/05 09:37:37 [INFO] memberlist: Marking node bouine-2 as failed",
			wantLvl: "INFO",
			wantMsg: "Marking node bouine-2 as failed",
		},
		{
			name:    "no level token falls back to info",
			line:    "2026/07/05 09:37:38 Err: Could not set the deadline: timeout",
			wantLvl: "INFO",
			wantMsg: "Err: Could not set the deadline: timeout",
		},
		{
			name:    "bracket in message without level token",
			line:    "2026/07/05 09:37:38 Err: Failed to dial [::1]:7946: connection refused",
			wantLvl: "INFO",
			wantMsg: "Err: Failed to dial [::1]:7946: connection refused",
		},
		{
			name:    "empty line",
			line:    "",
			wantLvl: "INFO",
			wantMsg: "",
		},
		{
			name:    "no memberlist prefix preserved",
			line:    "2026/07/05 09:37:39 [WARN] some other prefix: thing happened",
			wantLvl: "WARN",
			wantMsg: "some other prefix: thing happened",
		},
		// Representative memberlist log patterns (synthetic).
		{
			name:    "UDP probe failure warning",
			line:    "2026/01/15 10:15:30 [WARN] memberlist: Was able to connect to node-4 over TCP but UDP probes failed, network may be misconfigured",
			wantLvl: "WARN",
			wantMsg: "Was able to connect to node-4 over TCP but UDP probes failed, network may be misconfigured",
		},
		{
			name:    "suspect node failure",
			line:    "2026/01/15 10:15:27 [INFO] memberlist: Suspect node-2 has failed, no acks received",
			wantLvl: "INFO",
			wantMsg: "Suspect node-2 has failed, no acks received",
		},
		{
			name:    "marking node failed with peer confirmations",
			line:    "2026/01/15 10:15:30 [INFO] memberlist: Marking node-3 as failed, suspect timeout reached (2 peer confirmations)",
			wantLvl: "INFO",
			wantMsg: "Marking node-3 as failed, suspect timeout reached (2 peer confirmations)",
		},
		{
			name:    "refuting suspect message",
			line:    "2026/01/15 10:15:24 [WARN] memberlist: Refuting a suspect message (from: node-3)",
			wantLvl: "WARN",
			wantMsg: "Refuting a suspect message (from: node-3)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lvl, msg := parseMemberlistLine(tc.line)
			if lvl != tc.wantLvl {
				t.Errorf("level = %q, want %q", lvl, tc.wantLvl)
			}
			if msg != tc.wantMsg {
				t.Errorf("msg = %q, want %q", msg, tc.wantMsg)
			}
		})
	}
}

func TestSlogAdapter_EmitsStructuredRecords(t *testing.T) {
	t.Parallel()
	logger, mu, buf := captureLogger(t)
	a := newSlogAdapter(logger)

	lines := []string{
		"2026/07/05 09:37:34 [WARN] memberlist: Was able to connect to bouine-3 over TCP but UDP probes failed, network may be misconfigured\n",
		"2026/07/05 09:37:35 [ERR] memberlist: Failed to encode message for broadcast: eof\n",
		"2026/07/05 09:37:36 [DEBUG] memberlist: Using dynamic bind port 42321\n",
		"2026/07/05 09:37:37 [INFO] memberlist: Marking node bouine-2 as failed\n",
	}
	for _, l := range lines {
		_, err := a.Write([]byte(l))
		require.NoError(t, err, "Write")
	}

	records := parseAdapterRecords(t, mu, buf)
	require.Len(t, records, len(lines))

	wantLevels := []string{"WARN", "ERROR", "DEBUG", "INFO"}
	wantMsgs := []string{
		"Was able to connect to bouine-3 over TCP but UDP probes failed, network may be misconfigured",
		"Failed to encode message for broadcast: eof",
		"Using dynamic bind port 42321",
		"Marking node bouine-2 as failed",
	}
	for i, rec := range records {
		assert.Equal(t, wantLevels[i], rec["level"])
		assert.Equal(t, wantMsgs[i], rec["msg"])
		assert.Equal(t, "memberlist", rec["component"])
	}
}

func TestSlogAdapter_BuffersPartialLines(t *testing.T) {
	t.Parallel()
	logger, mu, buf := captureLogger(t)
	a := newSlogAdapter(logger)

	// Write a line in two chunks; no record should be emitted until the
	// newline arrives.
	_, err := a.Write([]byte("2026/07/05 09:37:34 [WARN] memberlist: partial"))
	require.NoError(t, err, "Write 1")
	got := parseAdapterRecords(t, mu, buf)
	require.Len(t, got, 0)
	_, err = a.Write([]byte(" message\n"))
	require.NoError(t, err, "Write 2")
	records := parseAdapterRecords(t, mu, buf)
	require.Len(t, records, 1)
	assert.Equal(t, "partial message", records[0]["msg"])
	assert.Equal(t, "WARN", records[0]["level"])
}

func TestSlogAdapter_MultipleLinesInOneWrite(t *testing.T) {
	t.Parallel()
	logger, mu, buf := captureLogger(t)
	a := newSlogAdapter(logger)

	blob := strings.Join([]string{
		"2026/07/05 09:37:34 [WARN] memberlist: first\n",
		"2026/07/05 09:37:35 [ERR] memberlist: second\n",
	}, "")
	_, err := a.Write([]byte(blob))
	require.NoError(t, err, "Write")
	records := parseAdapterRecords(t, mu, buf)
	require.Len(t, records, 2)
	if records[0]["msg"] != "first" || records[1]["msg"] != "second" {
		t.Errorf("msgs = %v, %v; want first, second", records[0]["msg"], records[1]["msg"])
	}
}

func TestSlogAdapter_EmptyLinesDropped(t *testing.T) {
	t.Parallel()
	logger, mu, buf := captureLogger(t)
	a := newSlogAdapter(logger)

	_, err := a.Write([]byte("\n\n"))
	require.NoError(t, err, "Write")
	got := parseAdapterRecords(t, mu, buf)
	require.Len(t, got, 0)
}

func TestSlogAdapter_HandlerQueueFullIncrementsCounter(t *testing.T) {
	t.Parallel()
	logger, _, _ := captureLogger(t)
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	a := newSlogAdapter(logger)
	a.setMetrics(m)

	_, err := a.Write([]byte(
		"2026/07/03 23:15:00 [WARN] memberlist: handler queue full, dropping message 8\n"))
	require.NoError(t, err, "Write")

	families, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, f := range families {
		if f.GetName() != "bouine_cluster_gossip_drops_total" {
			continue
		}
		require.Len(t, f.GetMetric(), 1)
		require.Equal(t, 1.0, f.GetMetric()[0].GetCounter().GetValue())
		return
	}
	t.Fatal("bouine_cluster_gossip_drops_total not registered")
}

func TestSlogAdapter_NonDropWarningDoesNotIncrement(t *testing.T) {
	t.Parallel()
	logger, _, _ := captureLogger(t)
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	a := newSlogAdapter(logger)
	a.setMetrics(m)

	_, err := a.Write([]byte(
		"2026/07/03 23:15:00 [WARN] memberlist: Refuting a suspect message (from: node-3)\n"))
	require.NoError(t, err, "Write")

	families, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, f := range families {
		if f.GetName() != "bouine_cluster_gossip_drops_total" {
			continue
		}
		require.Len(t, f.GetMetric(), 1)
		require.Equal(t, 0.0, f.GetMetric()[0].GetCounter().GetValue())
		return
	}
	t.Fatal("bouine_cluster_gossip_drops_total not registered")
}

func TestSlogAdapter_NoMetricsNoPanic(t *testing.T) {
	t.Parallel()
	logger, _, _ := captureLogger(t)
	a := newSlogAdapter(logger)

	// No setMetrics call — metrics pointer is nil. Must not panic.
	_, err := a.Write([]byte(
		"2026/07/03 23:15:00 [WARN] memberlist: handler queue full, dropping message 8\n"))
	require.NoError(t, err, "Write")
}

func TestSlogAdapter_InfoLevelDropDoesNotIncrement(t *testing.T) {
	t.Parallel()
	logger, _, _ := captureLogger(t)
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	a := newSlogAdapter(logger)
	a.setMetrics(m)

	// A hypothetical INFO-level log containing the substring should not
	// increment the counter — only WARN counts.
	_, err := a.Write([]byte(
		"2026/07/03 23:15:00 [INFO] memberlist: handler queue full (debug info)\n"))
	require.NoError(t, err, "Write")

	families, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, f := range families {
		if f.GetName() != "bouine_cluster_gossip_drops_total" {
			continue
		}
		require.Len(t, f.GetMetric(), 1)
		require.Equal(t, 0.0, f.GetMetric()[0].GetCounter().GetValue())
		return
	}
	t.Fatal("bouine_cluster_gossip_drops_total not registered")
}
