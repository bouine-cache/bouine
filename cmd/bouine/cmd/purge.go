package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/pkg/api"
)

func newPurgeCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "purge <url>",
		Short: "Purge a URL from the cache",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := adminClient(cmd).Purge(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return printResult(cmd, res.Status, res)
		},
	}
	return c
}

func newBanCmd() *cobra.Command {
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
				case "surrogate_key":
					expr.SurrogateKey = v
				default:
					return fmt.Errorf("unknown predicate key %q", k)
				}
			}
			res, err := adminClient(cmd).Ban(cmd.Context(), expr)
			if err != nil {
				return err
			}
			return printResult(cmd, fmt.Sprintf("%s (count: %d)", res.Status, res.Count), res)
		},
	}
	return c
}

func newRefreshCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "refresh <url>",
		Short: "Soft-purge a URL (mark stale, revalidate on next request)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := adminClient(cmd).Refresh(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return printResult(cmd, res.Status, res)
		},
	}
	return c
}
