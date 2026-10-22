package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

func newClusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Inspect and manage the cluster",
	}
	cmd.AddCommand(newClusterPeersCmd())
	return cmd
}

func newClusterPeersCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "peers",
		Short: "List live cluster peers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			peers, err := adminClient(cmd).Peers(cmd.Context())
			if err != nil {
				return fmt.Errorf("cluster peers: %w", err)
			}
			format, err := outputFormat(cmd)
			if err != nil {
				return err
			}
			if format == formatJSON {
				out, err := json.MarshalIndent(peers, "", "  ")
				if err != nil {
					return fmt.Errorf("marshal peers: %w", err)
				}
				_, err = fmt.Fprintln(cmd.OutOrStdout(), string(out))
				return err
			}
			for _, p := range peers {
				line := fmt.Sprintf("%s\t%s\t%s\t%s\n",
					p.Name, p.Addr, p.Version, p.JoinedAt.Format("2006-01-02 15:04:05"))
				if _, err := fmt.Fprint(cmd.OutOrStdout(), line); err != nil {
					return err
				}
			}
			return nil
		},
	}
	return c
}
