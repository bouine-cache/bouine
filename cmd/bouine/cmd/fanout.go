package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// clientForRemote builds the SDK client for one remote, attaching the
// kube-apiserver-proxy transport for kube:// servers and enforcing the
// version gate (daemons older than kubeProxyMinVersion do not strip the
// /proxy prefix).
func clientForRemote(ctx context.Context, r bouineapi.Remote) (*bouineapi.Client, error) {
	c := bouineapi.New(r.Server).WithToken(r.Token)
	if !strings.HasPrefix(r.Server, "kube://") {
		return c, nil
	}
	tr, err := newKubeTransport(r.Server)
	if err != nil {
		return nil, err
	}
	c.Transport = tr
	c.BaseURL = r.Server
	ver, err := c.Version(ctx)
	if err != nil {
		return nil, fmt.Errorf("remote %q: version probe via kube proxy: %w", r.Name, err)
	}
	if !versionAtLeast(ver.Version) {
		return nil, fmt.Errorf("remote %q: daemon %s predates the services/proxy support (needs >= %s)", r.Name, ver.Version, kubeProxyMinVersion)
	}
	return c, nil
}

// versionAtLeast reports whether v >= min by semver-ish comparison of
// dot-separated numeric components; "dev" always passes.
func versionAtLeast(v string) bool {
	if v == "dev" {
		return true
	}
	vp := strings.Split(strings.TrimPrefix(v, "v"), ".")
	mp := strings.Split(kubeProxyMinVersion, ".")
	for i := 0; i < len(mp) && i < len(vp); i++ {
		var a, b int
		_, _ = fmt.Sscanf(vp[i], "%d", &a)
		_, _ = fmt.Sscanf(mp[i], "%d", &b)
		if a != b {
			return a > b
		}
	}
	return len(vp) >= len(mp)
}

// closureResult pairs the human-readable line with the JSON payload so
// single-remote output stays byte-for-byte identical to the pre-fan-out
// CLI.
type closureResult struct {
	payload any
	text    string
}

// outcomeJSON is the multi-remote JSON shape: a bare array of
// {remote, ok, error?, result?}.
type outcomeJSON struct {
	Result any    `json:"result,omitempty"`
	Remote string `json:"remote"`
	Error  string `json:"error,omitempty"`
	OK     bool   `json:"ok"`
}

// runInvalidation executes one admin write command against the resolved
// remotes. Single-remote invocations keep the exact legacy output; when
// a selector matched several remotes it prints one line per remote (text)
// or a JSON array, and returns an aggregate error so the exit code is 1.
func runInvalidation(cmd *cobra.Command, plan string, fn func(*bouineapi.Client) (closureResult, error)) error {
	remotes, err := resolveRemotes(cmd)
	if err != nil {
		return err
	}
	dryRun, _ := cmd.Flags().GetBool(flagDryRun)
	if dryRun {
		for _, r := range remotes {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\n", r.Name, plan)
		}
		return nil
	}
	if err := checkWriteRemotes(cmd, remotes); err != nil {
		return err
	}
	if len(remotes) == 1 {
		return runSingle(cmd, remotes[0], fn)
	}
	return runFanout(cmd, remotes, fn)
}

func checkWriteRemotes(cmd *cobra.Command, remotes []bouineapi.Remote) error {
	if len(remotes) > 1 {
		warnDuplicateTokens(cmd, remotes)
	}
	for _, r := range remotes {
		if err := checkWriteTransport(r.Server, r.AllowInsecure); err != nil {
			return err
		}
	}
	return nil
}

func runSingle(cmd *cobra.Command, r bouineapi.Remote, fn func(*bouineapi.Client) (closureResult, error)) error {
	client, err := clientForRemote(cmd.Context(), r)
	if err != nil {
		return err
	}
	res, err := fn(client)
	if err != nil {
		return err
	}
	return printResult(cmd, res.text, res.payload)
}

func runFanout(cmd *cobra.Command, remotes []bouineapi.Remote, fn func(*bouineapi.Client) (closureResult, error)) error {
	format, err := outputFormat(cmd)
	if err != nil {
		return err
	}
	outcomes := make([]outcomeJSON, 0, len(remotes))
	failed := 0
	for _, r := range remotes {
		o := runOneRemote(cmd, format, r, fn)
		if !o.OK {
			failed++
		}
		outcomes = append(outcomes, o)
	}
	if format == formatJSON {
		out, err := json.MarshalIndent(outcomes, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal outcomes: %w", err)
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(out))
	}
	if failed > 0 {
		return fmt.Errorf("fan-out: %d of %d remotes failed", failed, len(remotes))
	}
	return nil
}

func runOneRemote(cmd *cobra.Command, format string, r bouineapi.Remote, fn func(*bouineapi.Client) (closureResult, error)) outcomeJSON {
	client, err := clientForRemote(cmd.Context(), r)
	if err != nil {
		reportRemoteError(cmd, format, r.Name, err)
		return outcomeJSON{Remote: r.Name, OK: false, Error: err.Error()}
	}
	res, err := fn(client)
	if err != nil {
		reportRemoteError(cmd, format, r.Name, err)
		return outcomeJSON{Remote: r.Name, OK: false, Error: err.Error()}
	}
	if format == formatText {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\n", r.Name, res.text)
	}
	return outcomeJSON{Remote: r.Name, OK: true, Result: res.payload}
}

func reportRemoteError(cmd *cobra.Command, format, name string, err error) {
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%s: %v\n", name, err)
	if format == formatText {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\tERROR: %v\n", name, err)
	}
}
