package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeRemotes(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "remotes.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func TestLoadRemotesFile_Validation(t *testing.T) {
	t.Parallel()
	base := `
remotes:
  - name: edge
    server: edge.example.net:9000
    token-env: BOUINE_TOKEN_EDGE
    labels: {tier: edge}
`
	t.Run("valid", func(t *testing.T) {
		rf, err := loadRemotesFile(writeRemotes(t, base))
		require.NoError(t, err)
		require.Len(t, rf.Remotes, 1)
		assert.Equal(t, "edge", rf.Remotes[0].Name)
	})
	t.Run("missing file is fine", func(t *testing.T) {
		rf, err := loadRemotesFile(filepath.Join(t.TempDir(), "nope.yaml"))
		require.NoError(t, err)
		assert.Empty(t, rf.Remotes)
	})
	t.Run("malformed", func(t *testing.T) {
		_, err := loadRemotesFile(writeRemotes(t, "remotes: [not yaml"))
		require.Error(t, err)
	})
	t.Run("duplicate names", func(t *testing.T) {
		_, err := loadRemotesFile(writeRemotes(t, `
remotes:
  - {name: a, server: a:1}
  - {name: a, server: b:1}
`))
		require.ErrorContains(t, err, "duplicate")
	})
	t.Run("both credential kinds", func(t *testing.T) {
		_, err := loadRemotesFile(writeRemotes(t, `
remotes:
  - {name: a, server: a:1, token-env: X, token-file: /tmp/x}
`))
		require.ErrorContains(t, err, "both token-env and token-file")
	})
}

func TestResolveToken_TokenFilePerms(t *testing.T) {
	dir := t.TempDir()
	loose := filepath.Join(dir, "loose.token")
	require.NoError(t, os.WriteFile(loose, []byte("secret"), 0o644))
	_, err := remoteEntry{Name: "x", TokenFile: loose}.resolveToken()
	require.ErrorContains(t, err, "0600")

	tight := filepath.Join(dir, "tight.token")
	require.NoError(t, os.WriteFile(tight, []byte("secret\n"), 0o600))
	tok, err := remoteEntry{Name: "x", TokenFile: tight}.resolveToken()
	require.NoError(t, err)
	assert.Equal(t, "secret", tok)

	t.Setenv("MY_TOK", "envtok")
	tok, err = remoteEntry{Name: "x", TokenEnv: "MY_TOK"}.resolveToken()
	require.NoError(t, err)
	assert.Equal(t, "envtok", tok)

	_, err = remoteEntry{Name: "x", TokenEnv: "MISSING_VAR_XYZ"}.resolveToken()
	require.ErrorContains(t, err, "not set")
}

func TestResolveRemotes_Conflicts(t *testing.T) {
	rf := writeRemotes(t, `
current-remote: edge
remotes:
  - {name: edge, server: edge:9000, labels: {tier: edge}}
  - {name: shield, server: shield:9000, labels: {tier: shield}}
`)
	t.Setenv("BOUINE_SERVER", "")
	t.Setenv("BOUINE_REMOTES_FILE", rf)

	_, _, err := run(t, "purge", "http://x/", "--server", "a:1", "--remote", "edge")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")

	_, _, err = run(t, "purge", "http://x/", "--remote", "nope")
	require.ErrorContains(t, err, "unknown remote")

	_, _, err = run(t, "purge", "http://x/", "-l", "tier=nope")
	require.ErrorContains(t, err, "matched no remotes")
}

func TestResolveRemotes_SelectorFanout(t *testing.T) {
	t.Setenv("BOUINE_SERVER", "")
	rf := writeRemotes(t, `
remotes:
  - {name: edge-eu, server: e1:9000, labels: {tier: edge, region: eu}}
  - {name: edge-us, server: e2:9000, labels: {tier: edge, region: us}}
  - {name: shield, server: s:9000, labels: {tier: shield}}
`)
	t.Setenv("BOUINE_SERVER", "")
	t.Setenv("BOUINE_REMOTES_FILE", rf)

	srv := stubAdmin(t)
	_ = srv
	root := Root()
	root.SetArgs([]string{"purge", "http://x/", "-l", "tier=edge", "--dry-run"})
	var stdout strings.Builder
	root.SetOut(&stdout)
	require.NoError(t, root.Execute())
	got := stdout.String()
	assert.Contains(t, got, "edge-eu\tpurge http://x/")
	assert.Contains(t, got, "edge-us\tpurge http://x/")
	assert.NotContains(t, got, "shield")
}

func TestFanout_MiddleFailureExit1(t *testing.T) {
	good := stubAdmin(t)
	rf := writeRemotes(t, "remotes:\n"+
		"  - {name: a, server: "+good+", labels: {g: one}}\n"+
		"  - {name: b, server: 127.0.0.1:1, labels: {g: one}}\n"+
		"  - {name: c, server: "+good+", labels: {g: one}}\n")
	t.Setenv("BOUINE_SERVER", "")
	t.Setenv("BOUINE_REMOTES_FILE", rf)

	root := Root()
	var stdout, stderr strings.Builder
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{"purge", "http://x/", "-l", "g=one"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 3")
	assert.Contains(t, stdout.String(), "a\tpurged")
	assert.Contains(t, stdout.String(), "b\tERROR")
	assert.Contains(t, stdout.String(), "c\tpurged")
	assert.Contains(t, stderr.String(), "b:")
}

func TestFanout_SingleRemoteOutputUnchanged(t *testing.T) {
	srv := stubAdmin(t)
	root := Root()
	var stdout strings.Builder
	root.SetOut(&stdout)
	root.SetArgs([]string{"purge", "http://x/", "--server", srv})
	require.NoError(t, root.Execute())
	assert.Equal(t, "purged\n", stdout.String())

	root = Root()
	stdout.Reset()
	root.SetOut(&stdout)
	root.SetArgs([]string{"purge", "http://x/", "--server", srv, "--format", "json"})
	require.NoError(t, root.Execute())
	assert.JSONEq(t, `{"status":"purged"}`, stdout.String())
}

func TestCheckWriteTransport(t *testing.T) {
	t.Parallel()
	require.NoError(t, checkWriteTransport("127.0.0.1:9000", false))
	require.NoError(t, checkWriteTransport("[::1]:9000", false))
	require.NoError(t, checkWriteTransport("https://edge:9000", false))
	require.Error(t, checkWriteTransport("edge.example.net:9000", false))
	require.NoError(t, checkWriteTransport("edge.example.net:9000", true))
}

func TestRemoteCmd_ListUseDelete(t *testing.T) {
	rf := writeRemotes(t, `# my remotes
current-remote: ""
remotes:
  - {name: edge, server: edge:9000, labels: {tier: edge}}
  - {name: shield, server: shield:9000}
`)
	t.Setenv("BOUINE_REMOTES_FILE", rf)

	out, _, err := run(t, "remote")
	require.NoError(t, err)
	assert.Contains(t, out, "edge")
	assert.Contains(t, out, "shield")

	_, _, err = run(t, "remote", "use", "edge")
	require.NoError(t, err)
	data, _ := os.ReadFile(rf)
	assert.Contains(t, string(data), `current-remote: "edge"`)
	assert.Contains(t, string(data), "# my remotes", "comments must survive the rewrite")

	_, _, err = run(t, "remote", "delete", "edge")
	require.NoError(t, err)
	rf2, err := loadRemotesFile(rf)
	require.NoError(t, err)
	assert.Empty(t, rf2.CurrentRemote)
	require.Len(t, rf2.Remotes, 1)
	assert.Equal(t, "shield", rf2.Remotes[0].Name)
}

func TestParseKubeServer(t *testing.T) {
	t.Parallel()
	ns, svc, port, err := parseKubeServer("kube://cache/bouine-admin:9000")
	require.NoError(t, err)
	assert.Equal(t, "cache", ns)
	assert.Equal(t, "bouine-admin", svc)
	assert.Equal(t, 9000, port)

	_, _, _, err = parseKubeServer("http://x:1")
	require.Error(t, err)
	_, _, _, err = parseKubeServer("kube://cache/svc")
	require.Error(t, err)
	_, _, _, err = parseKubeServer("kube://cache/svc:abc")
	require.Error(t, err)
}

func TestVersionAtLeast(t *testing.T) {
	t.Parallel()
	assert.True(t, versionAtLeast("dev"))
	assert.True(t, versionAtLeast("0.7.0"))
	assert.True(t, versionAtLeast("v0.7.1"))
	assert.True(t, versionAtLeast("0.8.0"))
	assert.False(t, versionAtLeast("0.6.7"))
	assert.False(t, versionAtLeast("v0.6.7"))
}
