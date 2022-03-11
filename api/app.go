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
	"github.com/gofiber/fiber/v2/middleware/proxy"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/timeout"
	"github.com/gofiber/storage/ristretto"
	"github.com/thylong/bouine/api/handlers"
	"github.com/thylong/bouine/api/middlewares"
	// "github.com/thylong/bouine/internal/backend"
	// "google.golang.org/grpc"
	// "google.golang.org/grpc/reflection".
)

var (
	httpTimeout = flag.Int64("timeout", 3000, "HTTP request timeout in milliseconds")
	// myAddr        = flag.String("address", "0.0.0.0:50051", "TCP host+port for this node").
	port     = flag.String("port", ":8080", "Port to listen on")
	prod     = flag.Bool("prod", false, "Enable prefork in Production")
	upstream = flag.String("upstream", "http://mockingjay:8084", "Proxied upstream host")
	// raftId   = flag.String("raft_id", "", "Node id used by Raft")
	// raftDir       = flag.String("raft_data_dir", "data/", "Raft data dir")
	// raftBootstrap = flag.Bool("raft_bootstrap", false, "Whether to bootstrap the Raft cluster").
)

func main() {
	// Parse command-line flags
	flag.Parse()

	// if *raftId == "" {
	// 	log.Fatalf("flag --raft_id is required")
	// }

	// Start raft
	// ctx := context.Background()
	// r, tm, err := backend.NewRaft(ctx, *raftDir, *raftId, *myAddr, *raftBootstrap)
	// if err != nil {
	// 	log.Fatalf("failed to start raft: %v", err)
	// }
	// s := grpc.NewServer()
	// pb.RegisterExampleServer(s, &rpcInterface{
	// 	wordTracker: wt,
	// 	raft:        r,
	// })
	// tm.Register(s)
	// leaderhealth.Setup(r, s, []string{"Example"})
	// raftadmin.Register(s, r)
	// reflection.Register(s)
	// if err := s.Serve(sock); err != nil {
	// 	log.Fatalf("failed to serve: %v", err)
	// }

	// Create fiber app
	app := fiber.New(fiber.Config{
		Prefork:               *prod, // go run app.go -prod
		DisableStartupMessage: *prod,
	})

	// Middlewares
	app.Use(recover.New())
	app.Use(cache.New(cache.Config{
		Next:                middlewares.CacheSkippable,
		ExpirationGenerator: middlewares.CustomExpirationGenerator,
		Storage: ristretto.New(ristretto.Config{
			NumCounters: 1e7,     // number of keys to track frequency of (10M).
			MaxCost:     1 << 30, // maximum cost of cache (1GB).
			BufferItems: 64,      // number of keys per Get buffer.
		}),
	}))
	app.Use(middlewares.ProxyMiddleware(proxy.Config{
		Servers: []string{
			*upstream,
		},
		Timeout: 5000 * time.Millisecond,
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
		handlers.DefaultHandler,
		time.Duration(*httpTimeout)*time.Millisecond),
	)

	go func() {
		if err := app.Listen(*port); err != nil { // go run app.go -port=:8080
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

	// fmt.Println("Closing connections...")
}

// TODO: Create a run function to simplify main func()
// TODO: Create a server struct holding pointers to dependencies (no global variables)
