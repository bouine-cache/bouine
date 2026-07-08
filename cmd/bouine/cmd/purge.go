package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

func newPurgeCmd() *cobra.Command {
	var server, token string

	c := &cobra.Command{
		Use:   "purge <url>",
		Short: "Purge a URL from the cache",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := bouineapi.New("http://" + server)
			if token != "" {
				client = client.WithToken(token)
			}
			res, err := client.Purge(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), res.Status)
			return err
		},
	}
	c.Flags().StringVar(&server, "server", "127.0.0.1:9000", "admin server address")
	c.Flags().StringVar(&token, "token", "", "admin bearer token")
	return c
}

func newBanCmd() *cobra.Command {
	var server, token string

	c := &cobra.Command{
		Use:   "ban <predicate>",
		Short: "Ban cache entries matching a predicate",
		Long: "ban issues a predicate-based invalidation. " +
			"Example: bouine ban host_regex=example.com path_regex=^/api",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			expr := api.BanExpr{}
			for _, a := range args {
				k, v, ok := strings.Cut(a, "=")
				if !ok {
					return fmt.Errorf("invalid predicate %q (expected key=value)", a)
				}
				switch k {
				case "host_regex":
					expr.HostRegex = v
				case "path_regex":
					expr.PathRegex = v
				default:
					return fmt.Errorf("unknown predicate key %q", k)
				}
			}
			client := bouineapi.New("http://" + server)
			if token != "" {
				client = client.WithToken(token)
			}
			res, err := client.Ban(cmd.Context(), expr)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s (count: %d)\n", res.Status, res.Count)
			return err
		},
	}
	c.Flags().StringVar(&server, "server", "127.0.0.1:9000", "admin server address")
	c.Flags().StringVar(&token, "token", "", "admin bearer token")
	return c
}

func newRefreshCmd() *cobra.Command {
	var server, token string

	c := &cobra.Command{
		Use:   "refresh <url>",
		Short: "Soft-purge a URL (mark stale, revalidate on next request)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := bouineapi.New("http://" + server)
			if token != "" {
				client = client.WithToken(token)
			}
			res, err := client.Refresh(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), res.Status)
			return err
		},
	}
	c.Flags().StringVar(&server, "server", "127.0.0.1:9000", "admin server address")
	c.Flags().StringVar(&token, "token", "", "admin bearer token")
	return c
}
