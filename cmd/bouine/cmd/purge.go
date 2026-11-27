package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

func newPurgeCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "purge <url>",
		Short: "Purge a URL from the cache",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInvalidation(cmd, "purge "+args[0], func(c *bouineapi.Client) (closureResult, error) {
				res, err := c.Purge(cmd.Context(), args[0])
				if err != nil {
					return closureResult{}, err
				}
				return closureResult{text: res.Status, payload: res}, nil
			})
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
			return runInvalidation(cmd, "ban "+strings.Join(args, " "), func(c *bouineapi.Client) (closureResult, error) {
				res, err := c.Ban(cmd.Context(), expr)
				if err != nil {
					return closureResult{}, err
				}
				return closureResult{text: fmt.Sprintf("%s (count: %d)", res.Status, res.Count), payload: res}, nil
			})
		},
	}
	return c
}

func newRefreshCmd() *cobra.Command {
	var surrogateKey string
	c := &cobra.Command{
		Use:   "refresh <url>",
		Short: "Soft-purge a URL or surrogate key (mark stale, revalidate on next request)",
		Long: "refresh soft-purges one URL, or every cached object carrying a " +
			"surrogate key with --surrogate-key (e.g. a Surrogate-Key / Cache-Tag value). " +
			"Exactly one of the positional URL or --surrogate-key must be given.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			plan := "refresh " + surrogateKey
			if surrogateKey == "" {
				if len(args) != 1 {
					return fmt.Errorf("provide a URL or --surrogate-key")
				}
				plan = "refresh " + args[0]
				if len(args) > 0 && surrogateKey != "" {
					return fmt.Errorf("url and --surrogate-key are mutually exclusive")
				}
			}
			return runInvalidation(cmd, plan, func(c *bouineapi.Client) (closureResult, error) {
				var (
					res closureResult
					err error
				)
				if surrogateKey != "" {
					r, e := c.RefreshSurrogate(cmd.Context(), surrogateKey)
					res, err = closureResult{text: fmt.Sprintf("%s (count: %d)", r.Status, r.Count), payload: r}, e
				} else {
					r, e := c.Refresh(cmd.Context(), args[0])
					res, err = closureResult{text: r.Status, payload: r}, e
				}
				if err != nil {
					return closureResult{}, err
				}
				return res, nil
			})
		},
	}
	c.Flags().StringVar(&surrogateKey, "surrogate-key", "", "soft-purge every object carrying this surrogate key")
	return c
}
