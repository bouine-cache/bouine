// Copyright 2022 Théotime Levêque
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
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
	"github.com/thylong/bouine/api/middlewares"
	"github.com/thylong/bouine/pkg/storage"
	"go.uber.org/zap"
)

var (
	httpTimeout   = flag.Int64("timeout", 3000, "HTTP request timeout in milliseconds")
	raftAddress   = flag.String("address", "0.0.0.0:50051", "TCP host+port for this node")
	port          = flag.String("port", ":8080", "Port to listen on")
	prod          = flag.Bool("prod", false, "Enable prefork in Production")
	upstream      = flag.String("upstream", "http://mockingjay:8084", "Proxied upstream host")
	raftID        = flag.String("raft_id", "1", "Node id used by Raft")
	raftDir       = flag.String("raft_data_dir", "/tmp/bouine", "Raft data dir")
	raftBootstrap = flag.Bool("raft_bootstrap", false, "Whether to bootstrap the Raft cluster")
	loggingLevel  = flag.String("logging_level", "info", "The minimum enabled logging level")
)

func main() {
	flag.Parse()

	// Create fiber app
	app, store := createFiberApp(*httpTimeout, *raftAddress, *prod, *upstream, *raftID, *raftDir, *raftBootstrap, *loggingLevel)
	defer store.Close()

	// Listen for gRPC requests
	// (coming from other nodes & clients)
	go func() {
		if err := store.ListengRPCServer(*raftAddress); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
	}()

	// Listen for HTTP requests
	go func() {
		if err := app.Listen(*port); err != nil {
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
