/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	xxhash "github.com/cespare/xxhash/v2"
	memory "github.com/gofiber/storage/memory/v2"
	"github.com/thylong/bouine/pkg/middleware/core"
	"github.com/thylong/bouine/pkg/middleware/upstream"
	fiber "github.com/thylong/fiber/v2"
	"github.com/thylong/fiber/v2/middleware/cache"
	"github.com/thylong/fiber/v2/middleware/compress"
	"github.com/thylong/fiber/v2/middleware/logger"
	"github.com/thylong/fiber/v2/middleware/proxy"
	"github.com/thylong/fiber/v2/middleware/recover"
	"github.com/thylong/fiber/v2/middleware/timeout"
	"go.uber.org/zap"

	"github.com/spf13/cobra"
)

var (
	httpTimeout  int64
	port         string
	prod         bool
	upstreamAddr string
	loggingLevel string
)

func init() {
	rootCmd.AddCommand(startCmd)

	startCmd.Flags().BoolVarP(&prod, "prod", "", false, "Enable prefork & set logLevel to INFO")
	startCmd.Flags().StringVarP(&port, "port", "p", ":8080", "Port to listen on")
	startCmd.Flags().StringVarP(&loggingLevel, "logging_level", "l", "info", "The minimum enabled logging level")
	startCmd.Flags().StringVarP(&upstreamAddr, "upstream", "u", "", "Proxied upstream host")
	startCmd.Flags().Int64VarP(&httpTimeout, "timeout", "t", 3000, "HTTP request timeout in milliseconds")
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a bouine instance",
	Long: `Start a bouine instance.
	The instance can either be a single standalone node or join an Bouine cluster.

	- To use as a single cluster, set the raft_bootstrap to true.
	- To start a leader node in a new cluster, set the raft_bootstrap to true.
	- To start a follower node in a new cluster, starts the node and use bouinectl to add the instance to any cluster.
	`,
	Run: func(cmd *cobra.Command, args []string) {
		flag.Parse()

		// Create fiber app
		app, store := createApp(httpTimeout, prod, upstreamAddr, loggingLevel)
		defer store.Close()

		// Listen for HTTP requests
		go func() {
			if err := app.Listen(port); err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				os.Exit(1)
			}
		}()

		signalChan := make(chan os.Signal, 1)
		signal.Notify(
			signalChan,
			syscall.SIGHUP,
			syscall.SIGINT, // includes Ctrl+c
			syscall.SIGQUIT,
		)

		<-signalChan
		log.Print("os.Interrupt - shutting down...\n")

		go func() {
			<-signalChan
			log.Fatal("os.Kill - terminating...\n")
		}()

		_ = app.Shutdown()
	},
}

func createApp(httpTimeout int64, prod bool, upstreamAddr string, loggingLevel string) (*fiber.App, *memory.Storage) {
	u, err := url.Parse(upstreamAddr)
	if err != nil {
		log.Fatalf("invalid upstream format: %s", upstreamAddr)
	}

	app := fiber.New(fiber.Config{
		Prefork:               prod,
		DisableStartupMessage: prod,
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		},
	})

	// init store logger
	logCfg := zap.NewProductionConfig()
	if loggingLevel == "debug" {
		logCfg.Development = true
		logCfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	// storeLogger, _ := logCfg.Build()

	// Initialize custom config
	store := memory.New(memory.Config{
		GCInterval: 10 * time.Second,
	})

	// Middlewares
	app.Use(recover.New())
	app.Use(logger.New())
	app.Use(compress.New())
	// cache middleware with distributed K/V store
	app.Use(cache.New(cache.Config{
		Next:                core.CacheSkippable,
		ExpirationGenerator: core.CustomExpirationGenerator,
		KeyGenerator: func(c *fiber.Ctx) string {
			digest := xxhash.New()
			digest.WriteString(c.Hostname() + "_" + c.Path())

			return fmt.Sprintf("%d", digest.Sum64())
		},
		// Storage:              store,
		StoreResponseHeaders: true,
	}))
	// active healthcheck middleware
	app.Use(upstream.SmartHealthcheckMiddleware(upstream.Config{
		Upstreams: []string{u.Host},
	}))
	// proxy request to upstream
	app.Use(upstream.ProxyMiddleware(proxy.Config{
		Servers: []string{
			upstreamAddr,
		},
		Timeout: 3000 * time.Millisecond,
		ModifyRequest: func(c *fiber.Ctx) error {
			c.Request().Header.Add("X-Real-IP", c.IP())
			return nil // Replace anonymous function by Request sanitizer
		},
		ModifyResponse: func(c *fiber.Ctx) error {
			return nil // Replace anonymous function by Response sanitizer
		},
	}))

	// Catch all handler
	app.Use(timeout.NewWithContext(
		func(c *fiber.Ctx) error {
			return c.SendStatus(500)
		},
		time.Duration(httpTimeout)*time.Millisecond),
	)
	return app, store
}
