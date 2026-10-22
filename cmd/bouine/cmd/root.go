// Package cmd defines the Cobra command tree for the bouine CLI.
package cmd

import (
	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/internal/buildinfo"
)

// Root returns the root Cobra command. It is the only exported entry
// point of this package; subcommands wire themselves into it.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:           "bouine",
		Short:         "bouine — a horizontally-scalable HTTP cache",
		Long:          "bouine is an observability-first HTTP/1.1+2 cache designed for Kubernetes.",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       buildinfo.Version,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// Fail fast on a bad --format before any command runs, so
			// offline commands reject it too instead of silently
			// ignoring it (issue #812).
			_, err := outputFormat(cmd)
			return err
		},
	}

	root.SetVersionTemplate(versionTemplate())

	root.CompletionOptions.DisableDefaultCmd = true

	addGlobalFlags(root)

	root.AddCommand(newVersionCmd())
	root.AddCommand(newServeCmd())
	root.AddCommand(newClusterCmd())
	root.AddCommand(newPurgeCmd())
	root.AddCommand(newBanCmd())
	root.AddCommand(newRefreshCmd())
	root.AddCommand(newConfigCmd())
	root.AddCommand(newCompletionCmd())
	return root
}

func versionTemplate() string {
	return "{{.Use}} {{.Version}}\n"
}
