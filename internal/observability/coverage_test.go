package observability

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/internal/observability/responsewriter"
	"github.com/bouine-cache/bouine/pkg/api"
)

// --- pure function tests ---

func TestMethodIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, methodIndex("GET"))
	assert.Equal(t, 1, methodIndex("HEAD"))
	assert.Equal(t, 2, methodIndex("POST"))
	assert.Equal(t, 2, methodIndex(""))
}

func TestStatusIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, statusIndex(200))
	assert.Equal(t, 1, statusIndex(206))
	assert.Equal(t, 2, statusIndex(304))
	assert.Equal(t, 3, statusIndex(301))
	assert.Equal(t, 4, statusIndex(302))
	assert.Equal(t, 5, statusIndex(404))
	assert.Equal(t, 6, statusIndex(500))
	assert.Equal(t, -1, statusIndex(999))
	assert.Equal(t, -1, statusIndex(0))
}

func TestCacheResultIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, cacheResultIndex("HIT"))
	assert.Equal(t, 1, cacheResultIndex("MISS"))
	assert.Equal(t, 2, cacheResultIndex("STALE"))
	assert.Equal(t, 3, cacheResultIndex("REVALIDATED"))
	assert.Equal(t, 4, cacheResultIndex("BYPASS"))
	assert.Equal(t, -1, cacheResultIndex("UNKNOWN"))
}

func TestSourceIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, sourceIndex(string(api.SourceHot)))
	assert.Equal(t, 1, sourceIndex(string(api.SourceWarm)))
	assert.Equal(t, 2, sourceIndex(string(api.SourcePeer)))
	assert.Equal(t, 3, sourceIndex(string(api.SourceOrigin)))
	assert.Equal(t, 4, sourceIndex(""))
	assert.Equal(t, -1, sourceIndex("unknown"))
}

func TestAccessLogMessage(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "served cache hit", accessLogMessage("HIT", 200))
	assert.Equal(t, "served cache miss", accessLogMessage("MISS", 200))
	assert.Equal(t, "bypassed cache", accessLogMessage("BYPASS", 200))
	assert.Equal(t, "served stale response", accessLogMessage("STALE", 200))
	assert.Equal(t, "served revalidated response", accessLogMessage("REVALIDATED", 200))
	assert.Equal(t, "served uncached response", accessLogMessage("", 200))
	assert.Equal(t, "request completed with error", accessLogMessage("HIT", 500))
	assert.Contains(t, accessLogMessage("UNKNOWN", 200), "unknown")
}

// --- PeerRing tests ---

func TestPeerRing_RecordAndHealth(t *testing.T) {
	t.Parallel()
	r := &PeerRing{}
	r.Record("node1", true)
	r.Record("node1", true)
	r.Record("node1", false)
	r.Record("node2", true)

	health := r.PeerHealth()
	assert.Equal(t, 2, len(health))
	assert.InDelta(t, 66.67, health["node1"], 0.1)
	assert.Equal(t, 100.0, health["node2"])
}

func TestPeerRing_Empty(t *testing.T) {
	t.Parallel()
	r := &PeerRing{}
	health := r.PeerHealth()
	assert.Empty(t, health)
}

// --- truncate test ---

func TestTruncate(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "short", truncate("short", 256))
	assert.Equal(t, "ab", truncate("abcdef", 2))
	assert.Equal(t, "", truncate("", 10))
}

// --- ResolveLogger / NoopLogger tests ---

func TestResolveLogger_Nil(t *testing.T) {
	t.Parallel()
	l := ResolveLogger(nil)
	assert.NotNil(t, l)
	assert.IsType(t, NoopLogger{}, l)
}

func TestResolveLogger_NonNil(t *testing.T) {
	t.Parallel()
	nl := NoopLogger{}
	l := ResolveLogger(nl)
	assert.Equal(t, nl, l)
}

func TestNoopLogger_AllMethods(t *testing.T) {
	t.Parallel()
	l := NoopLogger{}
	l.Info("test")
	l.Warn("test")
	l.Error("test")
	l.Debug("test")
}

// --- shouldLogAccess tests ---

func TestShouldLogAccess_ZeroRate(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{accessSampleRate: 0}
	assert.True(t, m.shouldLogAccess(api.Key{}))
}

func TestShouldLogAccess_WithKey(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{accessSampleRate: 100}
	key := api.Key{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	result := m.shouldLogAccess(key)
	_ = result
}

// --- buildAccessLogAttrs test ---

func TestBuildAccessLogAttrs(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{}
	r := httptest.NewRequest("GET", "http://example.com/path", nil)
	r.RemoteAddr = "1.2.3.4:5678"
	sw := &responsewriter.ResponseWriter{Status: 200, Bytes: 1024}
	attrs := m.buildAccessLogAttrs(r, sw, "HIT", 50*time.Millisecond)
	assert.NotEmpty(t, attrs)
	assert.Contains(t, attrs, "method")
	assert.Contains(t, attrs, "GET")
	assert.Contains(t, attrs, "host")
	assert.Contains(t, attrs, "path")
	assert.Contains(t, attrs, "status")
}
