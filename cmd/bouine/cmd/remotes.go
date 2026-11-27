package cmd

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

const (
	flagRemotesFile = "remotes-file"
	flagRemote      = "remote"
	flagSelector    = "selector"

	envRemotesFile = "BOUINE_REMOTES_FILE" //nolint:gosec // env var name, not a credential value
)

type remotesFile struct {
	path          string
	CurrentRemote string        `yaml:"current-remote"`
	Remotes       []remoteEntry `yaml:"remotes"`
}

type remoteEntry struct {
	Labels        map[string]string `yaml:"labels"`
	Name          string            `yaml:"name"`
	Server        string            `yaml:"server"`
	TokenEnv      string            `yaml:"token-env"`
	TokenFile     string            `yaml:"token-file"`
	AllowInsecure bool              `yaml:"allow-insecure"`
}

// remotesFilePath resolves --remotes-file > $BOUINE_REMOTES_FILE >
// $XDG_CONFIG_HOME/bouine/remotes.yaml (~/.config/bouine/remotes.yaml).
func remotesFilePath(cmd *cobra.Command) string {
	if f := flagOrEnv(cmd, flagRemotesFile, envRemotesFile, ""); f != "" {
		return f
	}
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "bouine", "remotes.yaml")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "bouine", "remotes.yaml")
}

// loadRemotesFile reads and validates the remotes file. A missing or
// empty file is not an error; it yields an empty file.
func loadRemotesFile(path string) (*remotesFile, error) {
	rf := &remotesFile{path: path}
	if path == "" {
		return rf, nil
	}
	data, err := os.ReadFile(path) //nolint:gosec // operator-configured remotes file path
	if errors.Is(err, os.ErrNotExist) {
		return rf, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read remotes file: %w", err)
	}
	if len(data) == 0 {
		return rf, nil
	}
	if err := yaml.Unmarshal(data, rf); err != nil {
		return nil, fmt.Errorf("parse remotes file %s: %w", path, err)
	}
	seen := map[string]bool{}
	for _, r := range rf.Remotes {
		if r.Name == "" {
			return nil, fmt.Errorf("remotes file %s: entry without a name", path)
		}
		if seen[r.Name] {
			return nil, fmt.Errorf("remotes file %s: duplicate remote name %q", path, r.Name)
		}
		seen[r.Name] = true
		if r.Server == "" {
			return nil, fmt.Errorf("remotes file %s: remote %q has no server", path, r.Name)
		}
		if r.TokenEnv != "" && r.TokenFile != "" {
			return nil, fmt.Errorf("remotes file %s: remote %q sets both token-env and token-file", path, r.Name)
		}
	}
	return rf, nil
}

// resolveToken returns the credential for a remote entry. A token-file
// must be mode 0600. A literal token in the file is unsupported.
func (r remoteEntry) resolveToken() (string, error) {
	switch {
	case r.TokenEnv != "":
		v := os.Getenv(r.TokenEnv)
		if v == "" {
			return "", fmt.Errorf("remote %q: environment variable %s is not set", r.Name, r.TokenEnv)
		}
		return v, nil
	case r.TokenFile != "":
		p := os.ExpandEnv(r.TokenFile)
		fi, err := os.Stat(p)
		if err != nil {
			return "", fmt.Errorf("remote %q: stat token file: %w", r.Name, err)
		}
		if fi.Mode().Perm() != 0o600 {
			return "", fmt.Errorf("remote %q: token file %s must be mode 0600, got %o", r.Name, p, fi.Mode().Perm())
		}
		data, err := os.ReadFile(p) //nolint:gosec // operator-configured path, 0600 enforced above
		if err != nil {
			return "", fmt.Errorf("remote %q: read token file: %w", r.Name, err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	return "", nil
}

// toRemote converts an entry to a bouineapi.Remote.
func (r remoteEntry) toRemote() (bouineapi.Remote, error) {
	token, err := r.resolveToken()
	if err != nil {
		return bouineapi.Remote{}, err
	}
	return bouineapi.Remote{Name: r.Name, Server: r.Server, Token: token, Labels: r.Labels}, nil
}

// resolveRemotes applies the resolution matrix from issue #953:
// --server/$BOUINE_SERVER, --remote and --selector are mutually
// exclusive; without any of them the file's current-remote is used,
// falling back to the default server.
func resolveRemotes(cmd *cobra.Command) ([]bouineapi.Remote, error) {
	serverSet := lookupFlag(cmd, flagServer) != nil && lookupFlag(cmd, flagServer).Changed || os.Getenv(envServer) != ""
	remoteFlag := flagOrEnv(cmd, flagRemote, "", "")
	selector := flagOrEnv(cmd, flagSelector, "", "")

	conflicts := 0
	for _, set := range []bool{serverSet, remoteFlag != "", selector != ""} {
		if set {
			conflicts++
		}
	}
	if conflicts > 1 {
		return nil, errors.New("--server, --remote and --selector are mutually exclusive")
	}

	if serverSet {
		server := flagOrEnv(cmd, flagServer, envServer, defaultServer)
		token := flagOrEnv(cmd, flagToken, envToken, "")
		return []bouineapi.Remote{{Name: "adhoc", Server: server, Token: token}}, nil
	}

	rf, err := loadRemotesFile(remotesFilePath(cmd))
	if err != nil {
		return nil, err
	}

	if remoteFlag != "" {
		return findRemote(remoteFlag, rf)
	}

	if selector != "" {
		return selectByLabels(selector, rf)
	}
	if rf.CurrentRemote != "" {
		return findRemote(rf.CurrentRemote, rf)
	}
	return []bouineapi.Remote{{Name: "default", Server: defaultServer, Token: flagOrEnv(cmd, flagToken, envToken, "")}}, nil
}

func selectByLabels(selector string, rf *remotesFile) ([]bouineapi.Remote, error) {
	pairs := strings.Split(selector, ",")
	var matched []bouineapi.Remote
	for _, e := range rf.Remotes {
		r := bouineapi.Remote{Name: e.Name, Server: e.Server, Labels: e.Labels}
		if r.MatchesSelector(pairs) {
			matched = append(matched, mustRemote(e))
		}
	}
	if len(matched) == 0 {
		return nil, fmt.Errorf("selector %q matched no remotes", selector)
	}
	return matched, nil
}

func findRemote(name string, rf *remotesFile) ([]bouineapi.Remote, error) {
	for _, e := range rf.Remotes {
		if e.Name == name {
			return []bouineapi.Remote{mustRemote(e)}, nil
		}
	}
	kind := "unknown remote"
	if name == rf.CurrentRemote {
		kind = "current-remote"
	}
	return nil, fmt.Errorf("%s %q not found in %s", kind, name, rf.path)
}

// mustRemote converts an entry, returning a zero Remote on error; the
// caller surfaces the error via warnDuplicateTokens or the fan-out.
func mustRemote(e remoteEntry) bouineapi.Remote {
	r, err := e.toRemote()
	if err != nil {
		r = bouineapi.Remote{Name: e.Name, Server: e.Server, Labels: e.Labels}
	}
	return r
}

// isLoopbackHost reports whether host is a loopback literal (127.0.0.0/8
// or ::1). DNS names are intentionally out of scope for v1.
func isLoopbackHost(host string) bool {
	if host == "::1" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// checkWriteTransport refuses plaintext write calls to non-loopback
// remotes unless the remote opts in with allow-insecure.
func checkWriteTransport(server string, allowInsecure bool) error {
	scheme, rest, ok := strings.Cut(server, "://")
	if !ok {
		scheme, rest = "http", server
	}
	if scheme != "http" {
		return nil
	}
	host, _, _ := net.SplitHostPort(rest)
	if host == "" {
		host = rest
	}
	if isLoopbackHost(host) || allowInsecure {
		return nil
	}
	return fmt.Errorf("refusing plaintext write to non-loopback remote %q: use https:// or a kube:// remote, or set allow-insecure: true on the remote", server)
}

// warnDuplicateTokens warns when several remotes resolve to the same
// token value: one leaked credential would then fan out fleet-wide.
func warnDuplicateTokens(cmd *cobra.Command, remotes []bouineapi.Remote) {
	byToken := map[string][]string{}
	for _, r := range remotes {
		if r.Token != "" {
			byToken[r.Token] = append(byToken[r.Token], r.Name)
		}
	}
	for _, names := range byToken {
		if len(names) > 1 {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "warning: remotes %s share the same credential value; a single leak fans out to all of them\n", strings.Join(names, ", "))
		}
	}
}
