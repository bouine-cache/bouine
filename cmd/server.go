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
	rootCmd.AddCommand(serverCmd)

	serverCmd.Flags().BoolVarP(&prod, "prod", "", false, "Enable prefork & set logLevel to INFO")
	serverCmd.Flags().BoolVarP(&raftBootstrap, "raft_bootstrap", "b", false, "Whether to bootstrap the Raft cluster")
	serverCmd.Flags().StringVarP(&raftAddress, "raft_address", "a", "0.0.0.0:50051", "TCP host+port for this node")
	serverCmd.Flags().StringVarP(&port, "port", "p", ":8080", "Port to listen on")
	serverCmd.Flags().StringVarP(&loggingLevel, "logging_level", "l", "info", "The minimum enabled logging level")
	serverCmd.Flags().StringVarP(&raftDir, "raft_dir", "d", "/tmp/bouine", "Raft data dir")
	serverCmd.Flags().StringVarP(&raftID, "raft_id", "i", ":8080", "Node id used by Raft")
	serverCmd.Flags().StringVarP(&upstream, "upstream", "u", "http://mockingjay:8084", "Proxied upstream host")
	serverCmd.Flags().Int64VarP(&httpTimeout, "timeout", "t", 3000, "HTTP request timeout in milliseconds")
}

// serverCmd represents the server command.
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("server called")
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
	// cache middleware serves cache otherwise Next to proxy
	app.Use(cache.New(cache.Config{
		Next:                 middlewares.CacheSkippable,
		ExpirationGenerator:  middlewares.CustomExpirationGenerator,
		Storage:              store,
		StoreResponseHeaders: true,
	}))
	// proxyMiddleware forwards requests to upstream
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
