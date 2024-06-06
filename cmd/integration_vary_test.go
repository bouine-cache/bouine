package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	memory "github.com/gofiber/storage/memory/v2"
	"github.com/gofiber/utils"
	"github.com/thylong/bouine/pkg/middleware/core"
)

func TestCacheVary(t *testing.T) {
	t.Parallel()

	var (
		testUpstream *fiber.App
		upstreamAddr string
		testBouine   *fiber.App
		store        *memory.Storage
		now          time.Time
		nowAsGMT     string
	)

	now = time.Now().UTC()
	nowAsGMT = asGMT(now)

	tests := []struct {
		name              string
		endpoint          string
		firstReqHeaders   http.Header
		SecondReqHeaders  http.Header
		upstreamRes       http.Response
		expectedFirstRes  http.Response
		expectedSecondRes http.Response
	}{
		{
			name:             "An optimal HTTP cache reuses a Vary response when the request matches",
			endpoint:         "/vary-match",
			firstReqHeaders:  http.Header{"Content-Type": []string{"application/json"}, "Foo": []string{"1"}},
			SecondReqHeaders: http.Header{"Content-Type": []string{"application/json"}, "Foo": []string{"1"}},
			upstreamRes: http.Response{
				StatusCode: 200, Header: http.Header{
					"Cache-Control": []string{"max-age=5000"},
					"Last-Modified": []string{asGMT(now.Add(time.Second * 3000))},
					"Vary":          []string{"Foo"},
					"Date":          []string{nowAsGMT},
				},
			},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusHit}}},
		},
		{
			name:             "HTTP cache must not reuse Vary response when request doesn't match",
			endpoint:         "/vary-no-match",
			firstReqHeaders:  http.Header{"Content-Type": []string{"application/json"}, "Foo": []string{"1"}},
			SecondReqHeaders: http.Header{"Content-Type": []string{"application/json"}, "Foo": []string{"2"}},
			upstreamRes: http.Response{
				StatusCode: 200, Header: http.Header{
					"Cache-Control": []string{"max-age=5000"},
					"Last-Modified": []string{asGMT(now.Add(time.Second * 3000))},
					"Vary":          []string{"Foo"},
					"Date":          []string{nowAsGMT},
				},
			},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
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
			req.Header = tt.firstReqHeaders
			resp, err := testBouine.Test(req)
			utils.AssertEqual(t, nil, err)
			defer resp.Body.Close()
			utils.AssertEqual(t, tt.expectedFirstRes.StatusCode, resp.StatusCode)
			utils.AssertEqual(t, tt.expectedFirstRes.Header.Get("Cache-Control"), resp.Header.Get("Cache-Control"))
			utils.AssertEqual(t, tt.expectedFirstRes.Header.Get("X-Cache"), resp.Header.Get("X-Cache"))

			// wait between the 2 requests (for freshness & stale checks)
			time.Sleep(2 * time.Second)

			// second request
			req = httptest.NewRequest("GET", tt.endpoint, nil)
			req.Header = tt.SecondReqHeaders
			resp, err = testBouine.Test(req)
			utils.AssertEqual(t, nil, err)
			defer resp.Body.Close()
			utils.AssertEqual(t, tt.expectedSecondRes.StatusCode, resp.StatusCode)
			utils.AssertEqual(t, tt.expectedSecondRes.Header.Get("Cache-Control"), resp.Header.Get("Cache-Control"))
			utils.AssertEqual(t, tt.expectedSecondRes.Header.Get("X-Cache"), resp.Header.Get("X-Cache"))
		})
	}
}
