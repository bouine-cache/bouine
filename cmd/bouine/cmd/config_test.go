package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// hotStub serves the hot-config surface: a mutable knob document and
// a POST endpoint that records requests and echoes a canned result.
// The returned probe reports what the CLI sent to POST.
func hotStub(t *testing.T, knobs *[]bouineapi.HotKnob) (string, func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var posts []map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "GET" && r.URL.Path == "/v1/config/hot":
			mu.Lock()
			defer mu.Unlock()
			_ = json.NewEncoder(w).Encode(*knobs)
		case r.Method == "POST" && r.URL.Path == "/v1/config/hot":
			var req map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			applied, _ := req["dry_run"].(bool)
			mu.Lock()
			posts = append(posts, req)
			mu.Unlock()
			res := bouineapi.HotSetResult{
				Key:      req["key"].(string),
				OldValue: "24h0m0s",
				NewValue: "1h30m0s",
				Applied:  !applied,
				Warning:  "not persisted; add to your YAML before the next deploy",
			}
			if res.Applied {
				res.Peers = 2
			}
			_ = json.NewEncoder(w).Encode(res)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://"), func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]any(nil), posts...)
	}
}

func hotKnobDoc() []bouineapi.HotKnob {
	return []bouineapi.HotKnob{
		{Key: "cluster.ban_ttl", Value: "24h0m0s", Source: "default",
			FileValue: "24h0m0s", Default: "24h0m0s", Description: "ban lifetime"},
		{Key: "url_ring_sample_rate", Value: "100", Source: "runtime",
			FileValue: "0", Default: "0", Description: "URL ring sampling"},
	}
}

func TestConfigGetTable(t *testing.T) {
	knobs := hotKnobDoc()
	server, _ := hotStub(t, &knobs)
	stdout, _, err := run(t, "config", "get", "--server", server)
	require.NoError(t, err)
	for _, want := range []string{"KEY", "VALUE", "SOURCE",
		"cluster.ban_ttl", "24h0m0s", "default",
		"url_ring_sample_rate", "100", "runtime"} {
		assert.Contains(t, stdout, want)
	}
}

func TestConfigGetSingleKeyIsBareValue(t *testing.T) {
	knobs := hotKnobDoc()
	server, _ := hotStub(t, &knobs)
	stdout, _, err := run(t, "config", "get", "url_ring_sample_rate", "--server", server)
	require.NoError(t, err)
	assert.Equal(t, "100\n", stdout, "single-key get is script-friendly")
}

func TestConfigGetUnknownKey(t *testing.T) {
	knobs := hotKnobDoc()
	server, _ := hotStub(t, &knobs)
	_, _, err := run(t, "config", "get", "nope.key", "--server", server)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown hot config key")
}

func TestConfigGetJSON(t *testing.T) {
	knobs := hotKnobDoc()
	server, _ := hotStub(t, &knobs)
	stdout, _, err := run(t, "config", "get", "--server", server, "--format", "json")
	require.NoError(t, err)
	var got []bouineapi.HotKnob
	require.NoError(t, json.Unmarshal([]byte(stdout), &got))
	require.Len(t, got, 2)
	assert.Equal(t, "24h0m0s", got[0].FileValue)
}

func TestConfigSetPrintsWarning(t *testing.T) {
	knobs := hotKnobDoc()
	server, posts := hotStub(t, &knobs)
	stdout, _, err := run(t, "config", "set", "cluster.ban_ttl", "90m", "--server", server)
	require.NoError(t, err)
	assert.Contains(t, stdout, "cluster.ban_ttl: 24h0m0s -> 1h30m0s (broadcast to 2 peers)")
	assert.Contains(t, stdout, "warning: not persisted; add to your YAML before the next deploy")
	sent := posts()
	require.Len(t, sent, 1)
	assert.Equal(t, "cluster.ban_ttl", sent[0]["key"])
	assert.Equal(t, "90m", sent[0]["value"])
	assert.Equal(t, false, sent[0]["dry_run"], "plain set must not send dry_run")
}

func TestConfigSetDryRun(t *testing.T) {
	knobs := hotKnobDoc()
	server, posts := hotStub(t, &knobs)
	stdout, _, err := run(t, "config", "set", "cluster.ban_ttl", "90m", "--server", server, "--dry-run")
	require.NoError(t, err)
	assert.Contains(t, stdout, "dry-run: would set cluster.ban_ttl: 24h0m0s -> 1h30m0s (not applied)")
	assert.NotContains(t, stdout, "warning:", "dry-run output does not repeat the persist warning as if applied")
	sent := posts()
	require.Len(t, sent, 1)
	assert.Equal(t, true, sent[0]["dry_run"])
}

func TestConfigSetJSON(t *testing.T) {
	knobs := hotKnobDoc()
	server, _ := hotStub(t, &knobs)
	stdout, _, err := run(t, "config", "set", "cluster.ban_ttl", "90m", "--server", server, "--format", "json")
	require.NoError(t, err)
	var res bouineapi.HotSetResult
	require.NoError(t, json.Unmarshal([]byte(stdout), &res))
	assert.True(t, res.Applied)
	assert.Equal(t, "not persisted; add to your YAML before the next deploy", res.Warning)
}

func TestConfigSetServerErrorFails(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"unknown hot config key \"nope\""}`))
	}))
	t.Cleanup(ts.Close)
	_, _, err := run(t, "config", "set", "nope", "1", "--server", strings.TrimPrefix(ts.URL, "http://"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown hot config key")
}

func TestConfigDiffNoDrift(t *testing.T) {
	knobs := []bouineapi.HotKnob{
		{Key: "cluster.ban_ttl", Value: "24h0m0s", Source: "default", FileValue: "24h0m0s"},
		{Key: "url_ring_sample_rate", Value: "0", Source: "file", FileValue: "0"},
	}
	server, _ := hotStub(t, &knobs)
	stdout, _, err := run(t, "config", "diff", "--server", server)
	require.NoError(t, err)
	assert.Equal(t, "no runtime drift\n", stdout)
}

func TestConfigDiffRuntimeDrift(t *testing.T) {
	knobs := hotKnobDoc() // url_ring_sample_rate is Source: runtime
	server, _ := hotStub(t, &knobs)
	stdout, _, err := run(t, "config", "diff", "--server", server)
	require.NoError(t, err)
	assert.Contains(t, stdout, "url_ring_sample_rate  file=0  runtime=100")
	assert.NotContains(t, stdout, "cluster.ban_ttl", "non-drifted knobs are not listed")
}

func TestConfigDiffJSON(t *testing.T) {
	knobs := hotKnobDoc()
	server, _ := hotStub(t, &knobs)
	stdout, _, err := run(t, "config", "diff", "--server", server, "--format", "json")
	require.NoError(t, err)
	var rows []hotDriftRow
	require.NoError(t, json.Unmarshal([]byte(stdout), &rows))
	require.Len(t, rows, 1)
	assert.Equal(t, "url_ring_sample_rate", rows[0].Key)
}

func writeConfigFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bouine.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestConfigDiffFiles(t *testing.T) {
	a := writeConfigFile(t, "listen:\n  http: 127.0.0.1:18080\ncluster:\n  ban_ttl: 24h\nurl_ring_sample_rate: 0\n")
	b := writeConfigFile(t, "listen:\n  http: 127.0.0.1:18081\ncluster:\n  ban_ttl: 90m\nurl_ring_sample_rate: 100\n")
	stdout, _, err := run(t, "config", "diff", a, b)
	require.NoError(t, err)
	assert.Contains(t, stdout, "cluster.ban_ttl: 24h0m0s -> 1h30m0s")
	assert.Contains(t, stdout, "listen.http: 127.0.0.1:18080 -> 127.0.0.1:18081")
	assert.Contains(t, stdout, "url_ring_sample_rate: 0 -> 100")
}

func TestConfigDiffFilesIdentical(t *testing.T) {
	a := writeConfigFile(t, "listen:\n  http: 127.0.0.1:18080\n")
	b := writeConfigFile(t, "listen:\n  http: 127.0.0.1:18080\n")
	stdout, _, err := run(t, "config", "diff", a, b)
	require.NoError(t, err)
	assert.Equal(t, "no differences\n", stdout)
}

func TestConfigDiffFilesJSON(t *testing.T) {
	a := writeConfigFile(t, "url_ring_sample_rate: 0\n")
	b := writeConfigFile(t, "url_ring_sample_rate: 100\n")
	stdout, _, err := run(t, "config", "diff", a, b, "--format", "json")
	require.NoError(t, err)
	var rows []configDiffRow
	require.NoError(t, json.Unmarshal([]byte(stdout), &rows))
	require.Len(t, rows, 1)
	assert.Equal(t, "url_ring_sample_rate", rows[0].Path)
	assert.Equal(t, "0", rows[0].A)
	assert.Equal(t, "100", rows[0].B)
}

func TestConfigDiffRejectsSingleFile(t *testing.T) {
	a := writeConfigFile(t, "listen:\n  http: 127.0.0.1:18080\n")
	_, _, err := run(t, "config", "diff", a)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "two config files")
}

func TestConfigDiffInvalidFileFails(t *testing.T) {
	a := writeConfigFile(t, "listen:\n  http: 127.0.0.1:18080\n")
	bad := writeConfigFile(t, "not_a_key: 1\n")
	_, _, err := run(t, "config", "diff", a, bad)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not_a_key", "both files must pass config validation")
}

// reloadStub serves POST /v1/config/reload with a canned report and
// records the request body.
func reloadStub(t *testing.T) (string, func() map[string]any) {
	t.Helper()
	var body map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		require.Equal(t, "POST", r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		_ = json.NewEncoder(w).Encode(bouineapi.ConfigReloadResult{
			Path:         "/etc/bouine/config.yaml",
			Applied:      []bouineapi.ConfigReloadChange{{Key: "cluster.ban_ttl", Old: "24h0m0s", New: "1h30m0s"}},
			Unchanged:    []string{"url_ring_sample_rate"},
			NeedsRestart: []string{"listen.http: 127.0.0.1:1 -> 127.0.0.1:2"},
		})
	}))
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://"), func() map[string]any {
		return body
	}
}

func TestConfigReloadTextOutput(t *testing.T) {
	server, body := reloadStub(t)
	stdout, _, err := run(t, "config", "reload", "--server", server)
	require.NoError(t, err)
	assert.Contains(t, stdout, "reloaded /etc/bouine/config.yaml")
	assert.Contains(t, stdout, "applied   cluster.ban_ttl: 24h0m0s -> 1h30m0s")
	assert.Contains(t, stdout, "unchanged url_ring_sample_rate")
	assert.Contains(t, stdout, "NEEDS RESTART (restart required to activate):")
	assert.Contains(t, stdout, "listen.http: 127.0.0.1:1 -> 127.0.0.1:2")
	assert.Empty(t, body()["path"], "no --file means an empty path (daemon default)")
}

func TestConfigReloadFileFlag(t *testing.T) {
	server, body := reloadStub(t)
	_, _, err := run(t, "config", "reload", "--server", server, "--file", "/tmp/new.yaml")
	require.NoError(t, err)
	assert.Equal(t, "/tmp/new.yaml", body()["path"])
}

func TestConfigReloadJSONOutput(t *testing.T) {
	server, _ := reloadStub(t)
	stdout, _, err := run(t, "config", "reload", "--server", server, "--format", "json")
	require.NoError(t, err)
	var res bouineapi.ConfigReloadResult
	require.NoError(t, json.Unmarshal([]byte(stdout), &res))
	assert.Equal(t, "/etc/bouine/config.yaml", res.Path)
	require.Len(t, res.Applied, 1)
	require.Len(t, res.NeedsRestart, 1)
}

func TestConfigReloadDisabledSurfacesServerMessage(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"config reload is disabled: set experimental.config_reload to enable it"}`))
	}))
	t.Cleanup(ts.Close)
	_, _, err := run(t, "config", "reload", "--server", strings.TrimPrefix(ts.URL, "http://"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "experimental.config_reload")
}
