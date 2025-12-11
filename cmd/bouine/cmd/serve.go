package cmd

import (
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/thylong/bouine/internal/admin"
	"github.com/thylong/bouine/internal/observability"
)

func newServeCmd() *cobra.Command {
	var (
		adminAddr string
		logLevel  string
		logFormat string
	)

	c := &cobra.Command{
		Use:   "serve",
		Short: "Run the bouine daemon",
		Long: "serve starts the bouine daemon. In phase 0 only the admin " +
			"listener (Fiber, /healthz, /readyz, /version) is wired. " +
			"Data-plane listeners and the cache engine land in later phases.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(),
				syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			logger := observability.New(observability.Options{
				Level:  logLevel,
				Format: logFormat,
			})

			srv := admin.New(admin.Config{
				Addr:   adminAddr,
				Logger: logger,
			})

			return srv.Serve(ctx)
		},
	}

	c.Flags().StringVar(&adminAddr, "admin-addr", ":9000",
		"address the admin Fiber app listens on")
	c.Flags().StringVar(&logLevel, "log-level", "info",
		"log level (debug, info, warn, error)")
	c.Flags().StringVar(&logFormat, "log-format", "json",
		"log format (json, text)")

	return c
}
