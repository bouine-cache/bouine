// Package cmd defines the Cobra command tree for the bouine CLI.
package cmd

import (
	"github.com/spf13/cobra"

	"github.com/thylong/bouine/internal/buildinfo"
)

// Root returns the root Cobra command. It is the only exported entry
// point of this package; subcommands wire themselves into it.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:           "bouine",
		Short:         "bouine — a horizontally-scalable HTTP reverse-proxy cache",
		Long:          "bouine is an observability-first HTTP/1.1+2+3 reverse-proxy cache designed for Kubernetes. See PLAN.md for the roadmap.",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       buildinfo.Version,
	}

	root.SetVersionTemplate(versionTemplate())

	root.AddCommand(newVersionCmd())
	root.AddCommand(newServeCmd())
	root.AddCommand(newClusterCmd())
	root.AddCommand(newPurgeCmd())
	root.AddCommand(newBanCmd())
	root.AddCommand(newRefreshCmd())
	return root
}

func versionTemplate() string {
	return "{{.Use}} {{.Version}}\n"
}
