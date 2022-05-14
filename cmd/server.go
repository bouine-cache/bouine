/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cache"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/proxy"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/timeout"
	"github.com/thylong/bouine/pkg/middlewares"
	"github.com/thylong/bouine/pkg/storage"
	"go.uber.org/zap"

	"github.com/spf13/cobra"
)

var (
	httpTimeout   int64
	raftAddress   string
	port          string
	prod          bool
	upstream      string
	raftID        string
	raftDir       string
	raftBootstrap bool
	loggingLevel  string
)

func init() {
	rootCmd.AddCommand(startCmd)

	startCmd.Flags().BoolVarP(&prod, "prod", "", false, "Enable prefork & set logLevel to INFO")
	startCmd.Flags().BoolVarP(&raftBootstrap, "raft_bootstrap", "b", false, "Whether to bootstrap the Raft cluster")
	startCmd.Flags().StringVarP(&raftAddress, "raft_address", "a", "0.0.0.0:50051", "TCP host+port for this node")
	startCmd.Flags().StringVarP(&port, "port", "p", ":8080", "Port to listen on")
	startCmd.Flags().StringVarP(&loggingLevel, "logging_level", "l", "info", "The minimum enabled logging level")
	startCmd.Flags().StringVarP(&raftDir, "raft_dir", "d", "/tmp/bouine", "Raft data dir")
	startCmd.Flags().StringVarP(&raftID, "raft_id", "i", ":8080", "Node id used by Raft")
	startCmd.Flags().StringVarP(&upstream, "upstream", "u", "http://mockingjay:8084", "Proxied upstream host")
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
		app, store := createFiberApp(httpTimeout, raftAddress, prod, upstream, raftID, raftDir, raftBootstrap, loggingLevel)
		defer store.Close()

		// Listen for gRPC requests
		// (coming from other nodes & clients)
		go func() {
			if err := store.ListengRPCServer(raftAddress); err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				os.Exit(1)
			}
		}()

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

func createFiberApp(httpTimeout int64, raftAddress string, prod bool, upstream string, raftID string, raftDir string, raftBootstrap bool, loggingLevel string) (*fiber.App, *storage.Storage) {
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
	storeLogger, _ := logCfg.Build()

	// create distributed K/V store
	store := storage.New(storage.Config{
		RaftID:        raftID,
		RaftDir:       raftDir,
		RaftBootstrap: raftBootstrap,
		RaftAddress:   raftAddress,
		Logger:        storeLogger,
	})

	// Middlewares
	app.Use(recover.New())
	app.Use(logger.New())
	// cache middleware with distributed K/V store
	app.Use(cache.New(cache.Config{
		Next:                 middlewares.CacheSkippable,
		ExpirationGenerator:  middlewares.CustomExpirationGenerator,
		Storage:              store,
		StoreResponseHeaders: true,
	}))
	// active healthcheck middleware
	app.Use(middlewares.SmartHealthcheckMiddleware())
	// proxy request to upstream
	app.Use(middlewares.ProxyMiddleware(proxy.Config{
		Servers: []string{
			upstream,
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
	app.Use(timeout.New(
		func(c *fiber.Ctx) error {
			return c.SendStatus(500)
		},
		time.Duration(httpTimeout)*time.Millisecond),
	)
	return app, store
}
