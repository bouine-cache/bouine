package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

func newPurgeCmd() *cobra.Command {
	var server, token string

	c := &cobra.Command{
		Use:   "purge <url>",
		Short: "Purge a URL from the cache",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body, _ := json.Marshal(map[string]string{"url": args[0]})
			return adminPost(cmd, server, token, "/v1/purge", body)
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
			expr := map[string]string{}
			for _, a := range args {
				k, v, ok := strings.Cut(a, "=")
				if !ok {
					return fmt.Errorf("invalid predicate %q (expected key=value)", a)
				}
				expr[k] = v
			}
			body, _ := json.Marshal(expr)
			return adminPost(cmd, server, token, "/v1/ban", body)
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
			body, _ := json.Marshal(map[string]string{"url": args[0]})
			return adminPost(cmd, server, token, "/v1/refresh", body)
		},
	}
	c.Flags().StringVar(&server, "server", "127.0.0.1:9000", "admin server address")
	c.Flags().StringVar(&token, "token", "", "admin bearer token")
	return c
}

func adminPost(cmd *cobra.Command, server, token, path string, body []byte) error {
	url := "http://" + server + path
	req, err := http.NewRequestWithContext(cmd.Context(), "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d: %s", path, resp.StatusCode, respBody)
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), string(respBody))
	return err
}
