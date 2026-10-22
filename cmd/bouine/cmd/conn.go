package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// Global connection and output settings shared by every admin-plane
// subcommand (issue #812). Values resolve with flag > environment >
// default precedence. The token is never written to output (AGENTS.md
// §6: no secrets in logs); the environment is the preferred source so
// it does not land in shell history.
const (
	flagServer = "server"
	flagToken  = "token"
	flagFormat = "format"

	envServer = "BOUINE_SERVER"
	envToken  = "BOUINE_TOKEN" //nolint:gosec // env var name, not a credential value
	envFormat = "BOUINE_FORMAT"

	defaultServer = "127.0.0.1:9000"

	formatText = "text"
	formatJSON = "json"
)

// addGlobalFlags registers the persistent flags every subcommand
// inherits. Offline subcommands (serve, config) accept them in help
// output but ignore them.
func addGlobalFlags(root *cobra.Command) {
	p := root.PersistentFlags()
	p.String(flagServer, defaultServer, "admin server address")
	p.String(flagToken, "", "admin bearer token (prefer "+envToken+")")
	p.String(flagFormat, formatText, "output format: "+formatText+" or "+formatJSON)
}

// lookupFlag finds a flag anywhere in the command's flag surface:
// its own flags, its persistent flags, then the ancestors'. Cobra
// merges persistent flags into Flags() during Execute, but callers
// may resolve before execution, so the walk is explicit.
func lookupFlag(cmd *cobra.Command, name string) *pflag.Flag {
	if f := cmd.Flags().Lookup(name); f != nil {
		return f
	}
	if f := cmd.PersistentFlags().Lookup(name); f != nil {
		return f
	}
	for p := cmd.Parent(); p != nil; p = p.Parent() {
		if f := p.PersistentFlags().Lookup(name); f != nil {
			return f
		}
	}
	return nil
}

// flagOrEnv resolves a string flag with flag > env > default
// precedence. An explicitly set flag wins even when empty; an empty
// environment variable falls through to the default.
func flagOrEnv(cmd *cobra.Command, name, env, def string) string {
	if f := lookupFlag(cmd, name); f != nil && f.Changed {
		return f.Value.String()
	}
	if v := os.Getenv(env); v != "" {
		return v
	}
	return def
}

// adminClient builds the admin API client from the resolved connection
// settings. The token is attached as the bearer credential only.
func adminClient(cmd *cobra.Command) *bouineapi.Client {
	client := bouineapi.New("http://" + flagOrEnv(cmd, flagServer, envServer, defaultServer))
	if token := flagOrEnv(cmd, flagToken, envToken, ""); token != "" {
		client = client.WithToken(token)
	}
	return client
}

// outputFormat returns the requested output format, rejecting
// unsupported values.
func outputFormat(cmd *cobra.Command) (string, error) {
	f := flagOrEnv(cmd, flagFormat, envFormat, formatText)
	switch f {
	case formatText, formatJSON:
		return f, nil
	default:
		return "", fmt.Errorf("invalid --format %q (want %q or %q)", f, formatText, formatJSON)
	}
}

// printResult writes a command result: a human line in text mode, the
// marshalled payload in JSON mode.
func printResult(cmd *cobra.Command, text string, payload any) error {
	format, err := outputFormat(cmd)
	if err != nil {
		return err
	}
	if format == formatJSON {
		out, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal result: %w", err)
		}
		_, err = fmt.Fprintln(cmd.OutOrStdout(), string(out))
		return err
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), text)
	return err
}
