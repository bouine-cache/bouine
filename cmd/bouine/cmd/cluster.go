package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"

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
	var server, token string

	c := &cobra.Command{
		Use:   "peers",
		Short: "List live cluster peers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			url := "http://" + server + "/v1/cluster/peers"
			req, err := http.NewRequestWithContext(cmd.Context(), "GET", url, nil)
			if err != nil {
				return err
			}
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("cluster peers: %w", err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("cluster peers: status %d", resp.StatusCode)
			}
			var peers any
			if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
				return err
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
