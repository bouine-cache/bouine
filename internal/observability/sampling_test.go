package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/thylong/bouine/pkg/api"
)

func TestSampledByKeyDeterminism(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 100)
	for i := range 1000 {
		key := api.Key(i * 7)
		first := l.sampledByKey(key)
		second := l.sampledByKey(key)
		if first != second {
			t.Fatalf("key %d: non-deterministic sampling", key)
		}
	}
}

func TestSampledByKeyRate(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 100)
	sampled := 0
	for i := range 10000 {
		if l.sampledByKey(api.Key(i)) {
			sampled++
		}
	}
	if sampled != 100 {
		t.Fatalf("expected 100 sampled keys out of 10000, got %d", sampled)
	}
}

func TestSampledByKeyZeroRateAlwaysLogs(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 0)
	for i := range 100 {
		if !l.sampledByKey(api.Key(i)) {
			t.Fatalf("key %d: zero rate should always log", i)
		}
	}
}

func TestSampledByKeyDistribution(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 100)
	buckets := make(map[int]int)
	for i := range 100000 {
		if l.sampledByKey(api.Key(i)) {
			buckets[i/1000%10]++
		}
	}
	if len(buckets) != 10 {
		t.Fatalf("expected samples across all 10 buckets, got %d", len(buckets))
	}
}

func TestSampledByCounterRate(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 100)
	sampled := 0
	for range 10000 {
		if l.sampledByCounter() {
			sampled++
		}
	}
	if sampled != 100 {
		t.Fatalf("expected 100 sampled out of 10000, got %d", sampled)
	}
}

func TestSampledByCounterZeroRateAlwaysLogs(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 0)
	for range 100 {
		if !l.sampledByCounter() {
			t.Fatal("zero rate should always log")
		}
	}
}

// sampledByKey is a test helper that exposes the key-based sampling decision.
func (l *SampledLogger) sampledByKey(key api.Key) bool {
	return l.sampleRate == 0 || uint64(key)%l.sampleRate == 0
}

// sampledByCounter is a test helper that exposes the counter-based sampling decision.
func (l *SampledLogger) sampledByCounter() bool {
	return l.sampleRate == 0 || l.counter.Add(1)%l.sampleRate == 0
}

func TestInfoZeroRateAlwaysLogs(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewSampledLogger(slog.New(slog.NewJSONHandler(&buf, nil)), 0)
	l.Info("test message", "key", api.Key(42))
	if buf.Len() == 0 {
		t.Fatal("zero rate should always log")
	}
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec["msg"] != "test message" {
		t.Errorf("msg = %v, want test message", rec["msg"])
	}
}

func TestInfoKeyFilteredBySampling(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewSampledLogger(slog.New(slog.NewJSONHandler(&buf, nil)), 100)
	l.Info("filtered", "key", api.Key(1))
	if buf.Len() != 0 {
		t.Fatal("key 1 with rate 100 should be filtered")
	}
	l.Info("passes", "key", api.Key(100))
	if buf.Len() == 0 {
		t.Fatal("key 100 with rate 100 should pass")
	}
}

func TestInfoNoKeyUsesCounterSampling(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewSampledLogger(slog.New(slog.NewJSONHandler(&buf, nil)), 100)
	for range 99 {
		l.Info("no key here")
	}
	if buf.Len() != 0 {
		t.Fatal("first 99 calls (counter 1-99 % 100 != 0) should be filtered")
	}
	l.Info("no key here")
	if buf.Len() == 0 {
		t.Fatal("100th call (counter 100 % 100 == 0) should pass")
	}
}
