package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
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
	var server, token string

	c := &cobra.Command{
		Use:   "peers",
		Short: "List live cluster peers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			baseURL := "http://" + server
			client := bouineapi.New(baseURL).WithToken(token)
			peers, err := client.Peers(cmd.Context())
			if err != nil {
				return fmt.Errorf("cluster peers: %w", err)
			}
			out, _ := json.MarshalIndent(peers, "", "  ")
			_, err = fmt.Fprintln(cmd.OutOrStdout(), string(out))
			return err
		},
	}
	c.Flags().StringVar(&server, "server", "127.0.0.1:9000", "admin server address")
	c.Flags().StringVar(&token, "token", "", "admin bearer token")
	return c
}
