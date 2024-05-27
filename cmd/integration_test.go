package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	memory "github.com/gofiber/storage/memory/v2"
	"github.com/gofiber/utils"
	"github.com/thylong/bouine/pkg/middleware"
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

	target, store := createApp(int64(500), false, fmt.Sprintf("http://%s", upstreamAddr), "debug")

	return target, store
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestCacheW3CStandards(t *testing.T) {
	tests := []struct {
		name              string
		endpoint          string
		reqHeaders        http.Header
		upstreamRes       http.Response
		expectedFirstRes  http.Response
		expectedSecondRes http.Response
	}{
		{
			name:              "An optimal HTTP cache reuses a response with positive Cache-Control: max-age",
			endpoint:          "/max-age",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}}},
		},
	}

	testUpstream = createUpstreamTestServer(t)

	// register testUpstream handlers
	for _, tt := range tests {
		testUpstream.Get(tt.endpoint, func(c *fiber.Ctx) error {
			response := &http.Response{
				StatusCode: tt.upstreamRes.StatusCode,
				Header:     tt.upstreamRes.Header,
				Body:       io.NopCloser(bytes.NewBufferString(`{"message": "Hello, World!"}`)),
			}

			// Set the response headers
			for key, values := range response.Header {
				for _, value := range values {
					c.Set(key, value)
				}
			}

			// Read the response body
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}

			// Send the response with the appropriate status code
			return c.Status(response.StatusCode).Send(body)
		})
	}
	upstreamAddr = listenUpstreamTestServer(t, testUpstream)
	testBouine, store = createBouineTestServer(t, upstreamAddr)
	defer store.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// first request
			req := httptest.NewRequest("GET", tt.endpoint, nil)
			req.Header = tt.reqHeaders
			resp, err := testBouine.Test(req)
			utils.AssertEqual(t, nil, err)
			defer resp.Body.Close()
			utils.AssertEqual(t, tt.expectedFirstRes.StatusCode, resp.StatusCode)
			fmt.Printf("%v \n", resp.Header)
			utils.AssertEqual(t, tt.expectedFirstRes.Header.Get("Cache-Control"), resp.Header.Get("Cache-Control"))
			utils.AssertEqual(t, resp.Header.Get("X-Cache"), middleware.StatusMiss)

			// wait between the 2 requests (for freshness & stale checks)
			time.Sleep(2 * time.Second)

			// second request
			req = httptest.NewRequest("GET", tt.endpoint, nil)
			req.Header = tt.reqHeaders
			resp, err = testBouine.Test(req)
			utils.AssertEqual(t, nil, err)
			defer resp.Body.Close()
			utils.AssertEqual(t, tt.expectedSecondRes.StatusCode, resp.StatusCode)
			fmt.Printf("%v \n", resp.Header)
			utils.AssertEqual(t, tt.expectedSecondRes.Header.Get("Cache-Control"), resp.Header.Get("Cache-Control"))
			utils.AssertEqual(t, resp.Header.Get("X-Cache"), middleware.StatusHit)
		})
	}
}
