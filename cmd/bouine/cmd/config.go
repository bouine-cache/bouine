package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/internal/config"
)

func newConfigCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "config",
		Short: "Configuration utilities",
		Long:  "config provides subcommands to validate and inspect bouine configuration files.",
	}

	c.AddCommand(newConfigValidateCmd())
	c.AddCommand(newConfigSchemaCmd())
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
