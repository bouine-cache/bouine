package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cannedKeyHex is the find fixture's key hex: a valid 32-char hex
// with deliberately low entropy (a repeating pattern) so the secret
// scanner does not read the fixture as a credential literal.
func cannedKeyHex() string { return strings.Repeat("01", 16) }

func keysFindStub(t *testing.T, captured *string, peerSeen *bool) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/keys/find", func(w http.ResponseWriter, r *http.Request) {
		if captured != nil {
			*captured = r.URL.Query().Get("url")
		}
		if peerSeen != nil {
			*peerSeen = r.URL.Query().Get("peer") == "1"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{
			"url": "http://example.com/pages/p?x=1",
			"key_hex": %q,
			"route": "pp",
			"host": "example.com",
			"path": "/pages/p",
			"query_policy": "all",
			"local": {"present": true, "tier": "hot", "size_bytes": 128,
				"stored_at": "2026-10-25T12:00:00Z", "ttl_seconds": 60,
				"status_code": 200, "window_hits": 3, "pool": "echo",
				"backed_to_warm": true},
			"variants": 2
		}`, cannedKeyHex())
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://")
}

func TestKeysFindTextOutput(t *testing.T) {
	t.Parallel()
	var url string
	server := keysFindStub(t, &url, nil)

	stdout, _, err := run(t, "keys", "find", "http://example.com/pages/p?x=1", "--server", server)
	require.NoError(t, err)
	assert.Equal(t, "http://example.com/pages/p?x=1", url)

	for _, want := range []string{
		"key:          " + cannedKeyHex(),
		"route:        pp",
		"host/path:    example.com/pages/p",
		"query policy: all",
		"local: hot, 128 bytes, ttl 1m0s",
		"backed to warm",
		"2 tracked variants",
	} {
		assert.Contains(t, stdout, want)
	}
}

func TestKeysFindPeerFlag(t *testing.T) {
	t.Parallel()
	var peer bool
	server := keysFindStub(t, nil, &peer)

	_, _, err := run(t, "keys", "find", "http://example.com/pages/p?x=1",
		"--server", server, "--peer")
	require.NoError(t, err)
	assert.True(t, peer, "--peer must reach the wire as peer=1")

	_, _, err = run(t, "keys", "find", "http://example.com/pages/p?x=1", "--server", server)
	require.NoError(t, err)
	assert.False(t, peer, "without --peer the query must not ask for the owner")
}

func TestKeysFindJSONOutput(t *testing.T) {
	t.Parallel()
	server := keysFindStub(t, nil, nil)

	stdout, _, err := run(t, "keys", "find", "http://example.com/pages/p?x=1",
		"--server", server, "--format", "json")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &doc))
	assert.Equal(t, cannedKeyHex(), doc["key_hex"])
	assert.Equal(t, "pp", doc["route"])
	local := doc["local"].(map[string]any)
	assert.Equal(t, "hot", local["tier"])
	assert.Equal(t, float64(2), doc["variants"])
}

func keysTopStub(t *testing.T, capturedPath *string, capturedQuery *string) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/keys/top", func(w http.ResponseWriter, r *http.Request) {
		*capturedPath = "top"
		*capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"kind": "top", "class": "miss", "limit": 2, "sample_rate": 100,
			"entries": [
				{"prefix": "/api/users", "route": "api", "requests": 42,
				 "misses": 40, "stale": 1, "revalidated": 1, "bypass": 0,
				 "last_seen": "2026-10-25T12:00:00Z"},
				{"prefix": "/static", "requests": 7, "misses": 7}
			]
		}`))
	})
	mux.HandleFunc("GET /v1/keys/recent", func(w http.ResponseWriter, r *http.Request) {
		*capturedPath = "recent"
		*capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind": "recent", "limit": 5, "sample_rate": 0,
			"entries": [{"prefix": "/hot/now", "requests": 1, "misses": 1,
			"last_seen": "2026-10-25T12:05:00Z"}]}`))
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://")
}

func TestKeysTopTextOutput(t *testing.T) {
	t.Parallel()
	var path, query string
	server := keysTopStub(t, &path, &query)

	stdout, _, err := run(t, "keys", "top", "--limit", "2", "--class", "miss", "--server", server)
	require.NoError(t, err)
	assert.Equal(t, "top", path)
	assert.Contains(t, query, "limit=2")
	assert.Contains(t, query, "class=miss")

	for _, want := range []string{
		"top: 2 prefixes (class miss), ring sample rate 1-in-100, non-HIT traffic view",
		"/api/users",
		"42",
		"/static",
		"2026-10-25T12:00:00Z",
	} {
		assert.Contains(t, stdout, want)
	}
}

func TestKeysRecentTextOutput(t *testing.T) {
	t.Parallel()
	var path, query string
	server := keysTopStub(t, &path, &query)

	stdout, _, err := run(t, "keys", "recent", "--server", server)
	require.NoError(t, err)
	assert.Equal(t, "recent", path)
	assert.Contains(t, query, "limit=20")
	assert.Contains(t, stdout, "recent: 1 prefixes, ring sample rate: every non-HIT request")
	assert.Contains(t, stdout, "/hot/now")
}

func TestKeysTopErrorSurfaces(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/keys/top", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"limit must be between 1 and 512"}`, http.StatusBadRequest)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	_, _, err := run(t, "keys", "top", "--limit", "999", "--server", strings.TrimPrefix(ts.URL, "http://"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit must be between 1 and 512")
}
