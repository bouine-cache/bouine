package main

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	memory "github.com/gofiber/storage/memory/v2"
	"github.com/gofiber/utils"
)

var (
	testUpstream *fiber.App
	upstreamAddr string
	testBouine   *fiber.App
	store        *memory.Storage
)

func createUpstreamTestServer(t *testing.T) *fiber.App {
	t.Helper()

	target := fiber.New(fiber.Config{DisableStartupMessage: true})

	return target
}

func listenUpstreamTestServer(t *testing.T, target *fiber.App) string {
	t.Helper()

	ln, err := net.Listen(fiber.NetworkTCP4, "127.0.0.1:0")
	utils.AssertEqual(t, nil, err)

	go func() {
		utils.AssertEqual(t, nil, target.Listener(ln))
	}()

	time.Sleep(2 * time.Second)
	addr := ln.Addr().String()

	return addr
}

func createBouineTestServer(t *testing.T, upstreamAddr string) (*fiber.App, *memory.Storage) {
	t.Helper()

	target, store := createApp(int64(500), false, fmt.Sprintf("http://%s", upstreamAddr), "info")

	return target, store
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
