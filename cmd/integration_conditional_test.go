package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/utils"
	"github.com/thylong/bouine/pkg/middleware/core"
)

func TestCacheConditionalRequests(t *testing.T) {
	t.Parallel()

	var (
		now      time.Time
		nowAsGMT string
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
			name:             "An optimal HTTP cache responds to If-Modified-Since with a 304 when holding a fresh response with a matching Last-Modified",
			endpoint:         "/conditional-lm-fresh",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{"If-Modified-Since": []string{asGMT(now.Truncate(time.Second * 3000))}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
				"Date":          []string{nowAsGMT}, "Cache-Control": []string{"max-age=100000"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
				"Date":          []string{nowAsGMT}, "Cache-Control": []string{"max-age=100000"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: fiber.StatusNotModified, Header: http.Header{
				"Date": []string{nowAsGMT}, "Cache-Control": []string{"max-age=100000"}, "X-Cache": []string{core.StatusHit},
			}},
		},
		{
			name:             "An optimal HTTP cache responds to If-Modified-Since with a 304 when holding a fresh response with an earlier Last-Modified",
			endpoint:         "/conditional-lm-fresh-earlier",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{"If-Modified-Since": []string{asGMT(now.Truncate(time.Second * 2000))}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
				"Date":          []string{nowAsGMT}, "Cache-Control": []string{"max-age=100000"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
				"Date":          []string{nowAsGMT}, "Cache-Control": []string{"max-age=100000"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: fiber.StatusNotModified, Header: http.Header{
				"Date": []string{nowAsGMT}, "Cache-Control": []string{"max-age=100000"}, "X-Cache": []string{core.StatusHit},
			}},
		},
	}

	testUpstream := createUpstreamTestServer(t)

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
	upstreamAddr := listenUpstreamTestServer(t, testUpstream)
	testBouine, store := createBouineTestServer(t, upstreamAddr)
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
			// Read the response body
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("The expected resp: %#v\n", tt.expectedFirstRes)
			fmt.Printf("The resp: %s\n", body)
			fmt.Printf("The resp: %#v\n", resp.Header.Get("Cache-Control"))
			utils.AssertEqual(t, tt.expectedFirstRes.StatusCode, resp.StatusCode)
			utils.AssertEqual(t, tt.expectedFirstRes.Header.Get("Cache-Control"), resp.Header.Get("Cache-Control"))
			utils.AssertEqual(t, tt.expectedFirstRes.Header.Get("X-Cache"), resp.Header.Get("X-Cache"))

			// wait between the 2 requests (for freshness & stale checks)
			time.Sleep(3 * time.Second)

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
