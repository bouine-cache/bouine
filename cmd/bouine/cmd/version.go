package cmd

import (
	"github.com/spf13/cobra"

	"github.com/thylong/bouine/internal/buildinfo"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the bouine version, commit, and build date",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := cmd.OutOrStdout().Write([]byte(
				"bouine " + buildinfo.Version +
					" (commit " + buildinfo.Commit +
					", built " + buildinfo.Date + ")\n",
			))
			return err
		},
	}
}
