package cmd

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// flagDryRun marks commands that validate and report without
// applying (bouine config set --dry-run).
const flagDryRun = "dry-run"

// flagFile points a subcommand at a specific config file
// (bouine config reload --file).
const flagFile = "file"

func newConfigCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "config",
		Short: "Configuration utilities",
		Long: "config validates and inspects bouine configuration files, and reads and " +
			"changes hot-config knobs on a running daemon without a restart. Runtime " +
			"changes are ephemeral: they are lost at the next restart, so persist them " +
			"in your YAML before the next deploy (config diff shows what is at risk).",
	}

	c.AddCommand(newConfigValidateCmd())
	c.AddCommand(newConfigSchemaCmd())
	c.AddCommand(newConfigGetCmd())
	c.AddCommand(newConfigSetCmd())
	c.AddCommand(newConfigDiffCmd())
	c.AddCommand(newConfigReloadCmd())
	return c
}

func newConfigValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate <file>",
		Short: "Validate a bouine config file",
		Long: "validate loads and validates a bouine YAML config file. " +
			"It reports errors but does not start the daemon.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := config.Load(args[0]); err != nil {
				return fmt.Errorf("validation failed: %w", err)
			}
			_, err := fmt.Fprintln(cmd.OutOrStdout(), "config is valid")
			return err
		},
	}
}

func newConfigSchemaCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "schema",
		Short: "Print the bouine Helm chart values JSON schema",
		Long: "schema prints the JSON schema for the bouine Helm chart values. " +
			"This includes both deployment fields (replicaCount, resources, " +
			"ingress, etc.) and the bouine config sub-tree (config.listen, " +
			"config.routes, etc.). Useful for editor autocomplete and CI validation.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := cmd.OutOrStdout().Write(helmSchemaJSON)
			return err
		},
	}
}

func newConfigGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get [key]",
		Short: "Show live hot-config knob values",
		Long: "get lists every runtime-tunable knob with its live value and source " +
			"(file, default, or runtime). With a key it prints just that knob's " +
			"value, so scripts can read it: $(bouine config get cluster.ban_ttl).",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			knobs, err := adminClient(cmd).HotConfig(cmd.Context())
			if err != nil {
				return err
			}
			if len(args) == 1 {
				return showHotKnob(cmd, knobs, args[0])
			}
			return printResult(cmd, renderHotKnobs(knobs), knobs)
		},
	}
}

func showHotKnob(cmd *cobra.Command, knobs []bouineapi.HotKnob, key string) error {
	for _, k := range knobs {
		if k.Key == key {
			return printResult(cmd, k.Value, k)
		}
	}
	return fmt.Errorf("unknown hot config key %q; run 'bouine config get' to list keys", key)
}

// renderHotKnobs writes the aligned key/value/source table. Aligned
// with lipgloss.Width rather than len so multi-byte values line up.
func renderHotKnobs(knobs []bouineapi.HotKnob) string {
	kw, vw := lipgloss.Width("key"), lipgloss.Width("value")
	for _, k := range knobs {
		kw = max(kw, lipgloss.Width(k.Key))
		vw = max(vw, lipgloss.Width(k.Value))
	}
	lines := make([]string, 0, len(knobs)+1)
	lines = append(lines, fmt.Sprintf("%-*s  %-*s  %s", kw, "KEY", vw, "VALUE", "SOURCE"))
	for _, k := range knobs {
		lines = append(lines, fmt.Sprintf("%-*s  %-*s  %s", kw, k.Key, vw, k.Value, k.Source))
	}
	return strings.Join(lines, "\n")
}

func newConfigSetCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Change a hot-config knob at runtime",
		Long: "set applies a validated point mutation to a running bouine without a restart. " +
			"The change is ephemeral: it is lost at the next restart, so add it to your " +
			"YAML before the next deploy. --dry-run validates the value and reports the " +
			"would-be change without applying it.",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dryRun, err := cmd.Flags().GetBool(flagDryRun)
			if err != nil {
				return err
			}
			res, err := adminClient(cmd).HotSet(cmd.Context(), args[0], args[1], dryRun)
			if err != nil {
				return err
			}
			return printResult(cmd, renderHotSet(res), res)
		},
	}
	c.Flags().Bool(flagDryRun, false, "validate the change and report it without applying")
	return c
}

// renderHotSet renders from the wire result, not the request flag, so
// the output can never claim an application the server did not make.
// Applied sets report their cluster fan-out coverage.
func renderHotSet(res *bouineapi.HotSetResult) string {
	if !res.Applied {
		return fmt.Sprintf("dry-run: would set %s: %s -> %s (not applied)",
			res.Key, res.OldValue, res.NewValue)
	}
	line := fmt.Sprintf("%s: %s -> %s", res.Key, res.OldValue, res.NewValue)
	if res.Peers > 0 {
		if res.Peers == 1 {
			line += " (broadcast to 1 peer)"
		} else {
			line += fmt.Sprintf(" (broadcast to %d peers)", res.Peers)
		}
	}
	if res.Warning != "" {
		line += "\nwarning: " + res.Warning
	}
	return line
}

// hotDriftRow is one knob whose runtime value diverges from the boot
// config and will be lost at the next restart.
type hotDriftRow struct {
	Key     string `json:"key"`
	File    string `json:"file"`
	Runtime string `json:"runtime"`
}

func newConfigDiffCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "diff [<a.yaml> <b.yaml>]",
		Short: "Show config drift",
		Long: "diff reports hot-config drift. With no arguments it asks the running daemon " +
			"which knobs were changed at runtime and are lost at the next restart. " +
			"With two config files it previews what deploying the second file would " +
			"change, comparing effective configs (defaults filled).",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 0 && len(args) != 2 {
				return fmt.Errorf("diff takes no arguments (runtime drift) or two config files (deploy preview)")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 2 {
				return runConfigFileDiff(cmd, args[0], args[1])
			}
			return runConfigRuntimeDiff(cmd)
		},
	}
}

func newConfigReloadCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "reload [--file <path>]",
		Short: "Apply a config file to the running daemon without a restart",
		Long: "reload re-parses and validates a config file, applies its hot-config " +
			"knob values to the running daemon, and reports everything that needs a " +
			"restart. A parse or validation failure changes nothing. Requires the " +
			"daemon to run with experimental.config_reload. Rollback is operational: " +
			"reload the previous (git-tracked) file.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			file, err := cmd.Flags().GetString(flagFile)
			if err != nil {
				return err
			}
			res, err := adminClient(cmd).ConfigReload(cmd.Context(), file)
			if err != nil {
				return err
			}
			return printResult(cmd, renderConfigReload(res), res)
		},
	}
	c.Flags().String(flagFile, "", "config file to reload (default: the daemon's startup file)")
	return c
}

// renderConfigReload prints the three-section report; needs_restart
// is the section the operator must never miss, so it gets its own
// shouting header.
func renderConfigReload(res *bouineapi.ConfigReloadResult) string {
	if len(res.Applied) == 0 && len(res.Unchanged) == 0 && len(res.NeedsRestart) == 0 {
		return fmt.Sprintf("reloaded %s: no differences", res.Path)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "reloaded %s", res.Path)
	for _, ch := range res.Applied {
		fmt.Fprintf(&b, "\napplied   %s: %s -> %s", ch.Key, ch.Old, ch.New)
	}
	for _, key := range res.Unchanged {
		fmt.Fprintf(&b, "\nunchanged %s", key)
	}
	if len(res.NeedsRestart) > 0 {
		b.WriteString("\nNEEDS RESTART (restart required to activate):")
		for _, item := range res.NeedsRestart {
			fmt.Fprintf(&b, "\n  %s", item)
		}
	}
	return b.String()
}

func runConfigRuntimeDiff(cmd *cobra.Command) error {
	knobs, err := adminClient(cmd).HotConfig(cmd.Context())
	if err != nil {
		return err
	}
	rows := make([]hotDriftRow, 0)
	for _, k := range knobs {
		if k.Source == "runtime" {
			rows = append(rows, hotDriftRow{Key: k.Key, File: k.FileValue, Runtime: k.Value})
		}
	}
	return printResult(cmd, renderHotDrift(rows), rows)
}

func renderHotDrift(rows []hotDriftRow) string {
	if len(rows) == 0 {
		return "no runtime drift"
	}
	lines := make([]string, 0, len(rows))
	for _, r := range rows {
		lines = append(lines, fmt.Sprintf("%s  file=%s  runtime=%s", r.Key, r.File, r.Runtime))
	}
	return strings.Join(lines, "\n")
}
