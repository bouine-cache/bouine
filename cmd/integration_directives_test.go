package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/utils"
	"github.com/thylong/bouine/pkg/middleware/core"
	fiber "github.com/thylong/fiber/v2"
)

func TestCacheDirectives(t *testing.T) {
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
		ThirdReqHeaders   http.Header
		upstreamRes       http.Response
		expectedFirstRes  http.Response
		expectedSecondRes http.Response
		expectedThirdRes  http.Response
	}{
		{
			name:             "Shared HTTP cache must not store a response with Cache-Control: private",
			endpoint:         "/cc-resp-private-shared",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{"If-Modified-Since": []string{asGMT(now.Truncate(time.Second * 3000))}},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Cache-Control": []string{"private, max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Cache-Control": []string{"private, max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Cache-Control": []string{"private, max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store a response with Cache-Control: no-store",
			endpoint:         "/cc-resp-no-store",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Cache-Control": []string{"no-store"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"no-store"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"no-store"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store a response with Cache-Control: nO-StOrE",
			endpoint:         "/cc-resp-no-store-case-insensitive",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"No-StOrE"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"No-StOrE"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"No-StOrE"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store a response with Cache-Control: no-store, even with max-age and Expires",
			endpoint:         "/cc-resp-no-store-fresh",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
				"Cache-Control": []string{"max-age=10000, no-store"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
				"Cache-Control": []string{"max-age=10000, no-store"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
				"Cache-Control": []string{"max-age=10000, no-store"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		// TODO: supported, but not tested for now
		// {
		// 	name:             "Does HTTP cache use older stored response when newer one came with Cache-Control: no-store?",
		// 	endpoint:         "/cc-resp-no-store-old-new",
		// 	firstReqHeaders:  http.Header{},
		// 	SecondReqHeaders: http.Header{},
		// 	ThirdReqHeaders:  http.Header{},
		// 	upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
		// 		"Cache-Control": []string{"max-age=10000, no-store"},
		// 	}},
		// 	expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
		// 		"Cache-Control": []string{"max-age=10000, no-store"}, "X-Cache": []string{core.StatusUnreachable},
		// 	}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
		// 		"Cache-Control": []string{"max-age=10000, no-store"}, "X-Cache": []string{core.StatusUnreachable},
		// 	}},
		// 	expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// TODO: supported, but not tested for now
		// {
		// 	name:             "Does HTTP cache use older stored response when newer one came with Cache-Control: no-store, max-age=0?",
		// 	endpoint:         "/cc-resp-no-store-old-max-age",
		// 	firstReqHeaders:  http.Header{},
		// 	SecondReqHeaders: http.Header{},
		// 	ThirdReqHeaders:  http.Header{},
		// 	upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
		// 		"Cache-Control": []string{"max-age=10000, no-store"},
		// 	}},
		// 	expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
		// 		"Cache-Control": []string{"max-age=10000, no-store"}, "X-Cache": []string{core.StatusUnreachable},
		// 	}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
		// 		"Cache-Control": []string{"max-age=10000, no-store"}, "X-Cache": []string{core.StatusUnreachable},
		// 	}},
		// 	expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		{
			name:             "HTTP cache must not use a cached response with Cache-Control: no-cache, even with max-age and Expires",
			endpoint:         "/cc-resp-no-cache",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
				"Cache-Control": []string{"max-age=10000, no-cache"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
				"Cache-Control": []string{"max-age=10000, no-cache"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
				"Cache-Control": []string{"max-age=10000, no-cache"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not use a cached response with Cache-Control: No-CaChE, even with max-age and Expires",
			endpoint:         "/cc-resp-no-cache-case-insensitive",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
				"Cache-Control": []string{"max-age=10000, No-CaChE"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
				"Cache-Control": []string{"max-age=10000, No-CaChE"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Expires": []string{asGMT(now.Add(time.Second * 10000))},
				"Cache-Control": []string{"max-age=10000, No-CaChE"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "An optimal HTTP cache reuses a response with positive Cache-Control: max-age, must-revalidate",
			endpoint:         "/cc-resp-must-revalidate-fresh",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"ETag":          []string{"abcd"},
				"Cache-Control": []string{"max-age=10000, must-revalidate"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"ETag":          []string{"abcd"},
				"Cache-Control": []string{"max-age=10000, must-revalidate"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"ETag":          []string{"abcd"},
				"Cache-Control": []string{"max-age=10000, must-revalidate"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
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

			// third request
			if tt.expectedThirdRes.StatusCode != fiber.StatusNotImplemented {
				req = httptest.NewRequest("GET", tt.endpoint, nil)
				req.Header = tt.ThirdReqHeaders
				resp, err = testBouine.Test(req)
				utils.AssertEqual(t, nil, err)
				defer resp.Body.Close()
				utils.AssertEqual(t, tt.expectedThirdRes.StatusCode, resp.StatusCode)
				utils.AssertEqual(t, tt.expectedThirdRes.Header.Get("Cache-Control"), resp.Header.Get("Cache-Control"))
				utils.AssertEqual(t, tt.expectedThirdRes.Header.Get("X-Cache"), resp.Header.Get("X-Cache"))
			}
		})
	}
}
