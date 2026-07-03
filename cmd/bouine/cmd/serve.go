package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/observability"
)

func newServeCmd() *cobra.Command {
	var (
		configPath string
		logLevel   string
		logFormat  string
	)

	c := &cobra.Command{
		Use:   "serve",
		Short: "Run the bouine daemon",
		Long: "serve starts the bouine daemon. It boots every configured " +
			"listener (HTTP/1.1, HTTP/2, admin), wires the pipeline " +
			"router to the upstream pools, and blocks until " +
			"SIGINT/SIGTERM.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(),
				syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			logger := observability.New(observability.Options{
				Level:  logLevel,
				Format: logFormat,
			})

			cfg, err := loadConfig(configPath)
			if err != nil {
				return err
			}

			return newEngine(cfg, configPath, logger).run(ctx)
		},
	}

	c.Flags().StringVar(&configPath, "config", "",
		"path to bouine config YAML file")
	c.Flags().StringVar(&logLevel, "log-level", "info",
		"log level (debug, info, warn, error)")
	c.Flags().StringVar(&logFormat, "log-format", "json",
		"log format (json, text)")

	return c
}

func loadConfig(path string) (*config.Config, error) {
	if path != "" {
		cfg, err := config.Load(path)
		if err != nil {
			return nil, fmt.Errorf("config: %w", err)
		}
		return cfg, nil
	}
	d := config.Defaults()
	// Resolve derived values for the no-config-file path too (Load does
	// this via Parse). Without it, a container with GOMEMLIMIT set but
	// no config file would run the hot store without an eviction budget.
	d.Storage.ResolveHotMaxBytes(os.Getenv("GOMEMLIMIT"))
	return &d, nil
}
