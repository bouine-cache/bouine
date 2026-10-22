package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubAdmin serves canned admin API responses so command tests can run
// end-to-end through the cobra tree.
func stubAdmin(t *testing.T) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/purge", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"purged"}`))
	})
	mux.HandleFunc("POST /v1/ban", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"banned","count":3}`))
	})
	mux.HandleFunc("POST /v1/refresh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"refreshed"}`))
	})
	mux.HandleFunc("GET /v1/cluster/peers", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"joined_at":"2026-10-23T09:00:00Z","name":"bouine-0",` +
			`"addr":"10.0.0.1:9010","admin_addr":"10.0.0.1:9000","data_addr":"10.0.0.1:8080",` +
			`"version":"0.5.30"}]`))
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://")
}

// run executes the root command with args and captured output.
func run(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	root := Root()
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs(args)
	err := root.Execute()
	return stdout.String(), stderr.String(), err
}

func TestFlagOrEnvPrecedence(t *testing.T) {
	root := Root()
	require.NoError(t, root.PersistentFlags().Set("server", "flag.example:9000"))
	t.Setenv("BOUINE_SERVER", "env.example:9000")

	got := flagOrEnv(root, flagServer, envServer, defaultServer)
	assert.Equal(t, "flag.example:9000", got, "flag must beat env")
}

func TestFlagOrEnvEmptyFlagWins(t *testing.T) {
	root := Root()
	require.NoError(t, root.PersistentFlags().Set("token", ""))
	t.Setenv("BOUINE_TOKEN", "env-secret")

	got := flagOrEnv(root, flagToken, envToken, "")
	assert.Equal(t, "", got, "explicitly cleared flag must beat env")
}

func TestFlagOrEnvEnvBeatsDefault(t *testing.T) {
	root := Root()
	t.Setenv("BOUINE_SERVER", "env.example:9000")

	assert.Equal(t, "env.example:9000", flagOrEnv(root, flagServer, envServer, defaultServer))
}

func TestFlagOrEnvDefault(t *testing.T) {
	root := Root()
	assert.Equal(t, defaultServer, flagOrEnv(root, flagServer, envServer, defaultServer))
	assert.Equal(t, "", flagOrEnv(root, flagToken, envToken, ""))
}

func TestAdminClientEnvWiring(t *testing.T) {
	t.Setenv("BOUINE_SERVER", "env.example:9000")
	t.Setenv("BOUINE_TOKEN", "env-secret")

	root := Root()
	client := adminClient(root)
	assert.Equal(t, "http://env.example:9000", client.BaseURL)
	assert.Equal(t, "env-secret", client.Token)
}

func TestAdminClientFlagOverridesEnv(t *testing.T) {
	t.Setenv("BOUINE_SERVER", "env.example:9000")
	t.Setenv("BOUINE_TOKEN", "env-secret")

	root := Root()
	purge, _, ferr := root.Find([]string{"purge"})
	require.NoError(t, ferr)
	// Same flag objects the subcommand inherits after cobra merges
	// persistent flags during Execute.
	require.NoError(t, root.PersistentFlags().Set("server", "flag.example:9000"))
	require.NoError(t, root.PersistentFlags().Set("token", "flag-secret"))
	client := adminClient(purge)
	assert.Equal(t, "http://flag.example:9000", client.BaseURL)
	assert.Equal(t, "flag-secret", client.Token)
}

func TestOutputFormat(t *testing.T) {
	root := Root()
	format, err := outputFormat(root)
	require.NoError(t, err)
	assert.Equal(t, formatText, format)

	root = Root()
	require.NoError(t, root.PersistentFlags().Set("format", formatJSON))
	format, err = outputFormat(root)
	require.NoError(t, err)
	assert.Equal(t, formatJSON, format)

	root = Root()
	t.Setenv("BOUINE_FORMAT", formatJSON)
	format, err = outputFormat(root)
	require.NoError(t, err)
	assert.Equal(t, formatJSON, format)

	root = Root()
	require.NoError(t, root.PersistentFlags().Set("format", "xml"))
	_, err = outputFormat(root)
	assert.Error(t, err)
}

func TestRootRejectsInvalidFormatEarly(t *testing.T) {
	t.Setenv("BOUINE_TOKEN", "irrelevant")
	_, _, err := run(t, "version", "--format", "xml")
	require.Error(t, err, "invalid format must fail even for offline commands")
	assert.Contains(t, err.Error(), "invalid --format")
}

func TestTokenNeverPrinted(t *testing.T) {
	t.Setenv("BOUINE_SERVER", "127.0.0.1:1")
	t.Setenv("BOUINE_TOKEN", "super-secret-token-value")
	stdout, stderr, err := run(t, "purge", "http://example.com/img.png")
	require.Error(t, err, "dead admin port must fail")
	combined := stdout + stderr + err.Error()
	assert.NotContains(t, combined, "super-secret-token-value",
		"the token must never leak into output or errors")
}

func TestPurgeTextOutput(t *testing.T) {
	server := stubAdmin(t)
	stdout, _, err := run(t, "purge", "http://example.com/img.png", "--server", server)
	require.NoError(t, err)
	assert.Equal(t, "purged\n", stdout)
}

func TestPurgeJSONOutput(t *testing.T) {
	server := stubAdmin(t)
	stdout, _, err := run(t, "purge", "http://example.com/img.png", "--server", server, "--format", "json")
	require.NoError(t, err)
	var res map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &res))
	assert.Equal(t, "purged", res["status"])
}

func TestPurgeServerFromEnv(t *testing.T) {
	server := stubAdmin(t)
	t.Setenv("BOUINE_SERVER", server)
	stdout, _, err := run(t, "purge", "http://example.com/img.png")
	require.NoError(t, err)
	assert.Equal(t, "purged\n", stdout)
}

func TestBanOutput(t *testing.T) {
	server := stubAdmin(t)
	stdout, _, err := run(t, "ban", "host_regex=example.com", "--server", server)
	require.NoError(t, err)
	assert.Equal(t, "banned (count: 3)\n", stdout)

	stdout, _, err = run(t, "ban", "host_regex=example.com", "--server", server, "--format", "json")
	require.NoError(t, err)
	var res struct {
		Status string `json:"status"`
		Count  int    `json:"count"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &res))
	assert.Equal(t, "banned", res.Status)
	assert.Equal(t, 3, res.Count)
}

func TestRefreshOutput(t *testing.T) {
	server := stubAdmin(t)
	stdout, _, err := run(t, "refresh", "http://example.com/img.png", "--server", server)
	require.NoError(t, err)
	assert.Equal(t, "refreshed\n", stdout)

	stdout, _, err = run(t, "refresh", "http://example.com/img.png", "--server", server, "--format", "json")
	require.NoError(t, err)
	var res map[string]string
	require.NoError(t, json.Unmarshal([]byte(stdout), &res))
	assert.Equal(t, "refreshed", res["status"])
}

func TestClusterPeersOutput(t *testing.T) {
	server := stubAdmin(t)
	stdout, _, err := run(t, "cluster", "peers", "--server", server)
	require.NoError(t, err)
	assert.Contains(t, stdout, "bouine-0")
	assert.Contains(t, stdout, "10.0.0.1:9010")
	assert.Contains(t, stdout, "0.5.30")

	stdout, _, err = run(t, "cluster", "peers", "--server", server, "--format", "json")
	require.NoError(t, err)
	var peers []map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &peers))
	require.Len(t, peers, 1)
	assert.Equal(t, "bouine-0", peers[0]["name"])
	assert.Equal(t, "10.0.0.1:9010", peers[0]["addr"])
}
