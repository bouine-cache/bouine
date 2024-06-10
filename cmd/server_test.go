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
package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/utils"
	"github.com/thylong/bouine/pkg/middleware/core"
	fiber "github.com/thylong/fiber/v2"
)

func Test_createApp(t *testing.T) {
	t.Parallel()

	// create testUpstream & add simple handler
	testUpstream := createUpstreamTestServer(t)
	testUpstream.Get("/", func(c *fiber.Ctx) error {
		response := &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Cache-Control": []string{"max-age=3600"}, "Content-Type": []string{"application/json"}},
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

	upstreamAddr := listenUpstreamTestServer(t, testUpstream)
	app, store := createBouineTestServer(t, upstreamAddr)
	defer store.Close()

	expectedFirstRes := http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss}}}
	expectedSecondRes := http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit}}}
	// first request
	req := httptest.NewRequest("GET", "/", nil)
	resp, err := app.Test(req)
	utils.AssertEqual(t, nil, err)
	defer resp.Body.Close()
	utils.AssertEqual(t, expectedFirstRes.StatusCode, resp.StatusCode)
	utils.AssertEqual(t, expectedFirstRes.Header.Get("Cache-Control"), resp.Header.Get("Cache-Control"))
	utils.AssertEqual(t, expectedFirstRes.Header.Get("X-Cache"), resp.Header.Get("X-Cache"))

	// wait between the 2 requests (for freshness & stale checks)
	time.Sleep(1 * time.Second)

	// second request
	req = httptest.NewRequest("GET", "/", nil)
	resp, err = app.Test(req)
	utils.AssertEqual(t, nil, err)
	defer resp.Body.Close()
	utils.AssertEqual(t, expectedSecondRes.StatusCode, resp.StatusCode)
	utils.AssertEqual(t, expectedSecondRes.Header.Get("Cache-Control"), resp.Header.Get("Cache-Control"))
	utils.AssertEqual(t, expectedSecondRes.Header.Get("X-Cache"), resp.Header.Get("X-Cache"))
}
