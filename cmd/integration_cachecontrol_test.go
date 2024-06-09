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

func TestCacheControl(t *testing.T) {
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
		reqHeaders        http.Header
		upstreamRes       http.Response
		expectedFirstRes  http.Response
		expectedSecondRes http.Response
	}{
		{
			name:              "An optimal HTTP cache reuses a response with positive Cache-Control: max-age",
			endpoint:          "/freshness-max-age",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit}}},
		},
		{
			name:              "HTTP cache must not reuse a response with Cache-Control: max-age after it becomes stale",
			endpoint:          "/freshness-max-age-stale",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2"}, "X-Cache": []string{core.StatusMiss}}},
		},
		{
			name:              "HTTP cache must not reuse a response with Cache-Control: max-age=0",
			endpoint:          "/freshness-max-age-0",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=0"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=0"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=0"}, "X-Cache": []string{core.StatusUnreachable}}},
		},
		{
			name:              "An optimal HTTP cache reuses a response with Cache-Control: max-age: 2147483647",
			endpoint:          "/freshness-max-age-max-minus-1",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2147483647"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2147483647"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2147483647"}, "X-Cache": []string{core.StatusHit}}},
		},
		{
			name:              "An optimal HTTP cache reuses a response with Cache-Control: max-age: 2147483648",
			endpoint:          "/freshness-max-age-max",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2147483648"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2147483648"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2147483648"}, "X-Cache": []string{core.StatusHit}}},
		},
		{
			name:              "An optimal HTTP cache reuses a response with Cache-Control: max-age: 2147483649",
			endpoint:          "/freshness-max-age-max-plus-1",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2147483649"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2147483649"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=2147483649"}, "X-Cache": []string{core.StatusHit}}},
		},
		{
			name:              "An optimal HTTP cache reuses a response with Cache-Control: max-age: 99999999999",
			endpoint:          "/freshness-max-age-max-plus",
			reqHeaders:        http.Header{},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=99999999999"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=99999999999"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=99999999999"}, "X-Cache": []string{core.StatusHit}}},
		},
		{
			name:              "HTTP cache must not reuse a response when the Age header is greater than its Cache-Control: max-age freshness lifetime",
			endpoint:          "/freshness-max-age-age",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}, "Age": []string{"7200"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}, "Age": []string{"7200"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600"}, "Age": []string{"7200"}, "X-Cache": []string{core.StatusUnreachable}}},
		},
		{
			name:       "An optimal HTTP cache reuses a response with positive Cache-Control: max-age and a past Expires",
			endpoint:   "/freshness-max-age-expires",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=3600"},
				"Expires":       []string{now.Format("Wed, 21 Oct 2015 07:28:00 GMT")},
				"Date":          []string{nowAsGMT},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=3600"},
				"Expires":       []string{now.Format("wed, 21 oct 2015 07:28:00 GMT")},
				"Date":          []string{nowAsGMT},
				"X-Cache":       []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=3600"},
				"Expires":       []string{now.Format("wed, 21 oct 2015 07:28:00 GMT")},
				"Date":          []string{nowAsGMT},
				"X-Cache":       []string{core.StatusHit},
			}},
		},
		{
			name:       "An optimal HTTP cache reuses a response with positive Cache-Control: max-age and an invalid Expires",
			endpoint:   "/freshness-max-age-expires-invalid",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=3600"},
				"Expires":       []string{"0"},
				"Date":          []string{now.Add(time.Duration(7200) * time.Second).Format("wed, 21 oct 2015 07:28:00 gmt")},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=3600"},
				"Expires":       []string{"0"},
				"Date":          []string{now.Add(time.Duration(7200) * time.Second).Format("wed, 21 oct 2015 07:28:00 gmt")},
				"X-Cache":       []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=3600"},
				"Expires":       []string{"0"},
				"Date":          []string{now.Add(time.Duration(7200) * time.Second).Format("wed, 21 oct 2015 07:28:00 gmt")},
				"X-Cache":       []string{core.StatusHit},
			}},
		},
		{
			name:       "HTTP cache must not reuse a response with Cache-Control: max-age=0 and a future Expires",
			endpoint:   "/freshness-max-age-0-expires",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=0"},
				"Expires":       []string{now.Add(time.Duration(3600) * time.Second).Format("wed, 21 oct 2015 07:28:00 gmt")},
				"Date":          []string{now.Format("wed, 21 oct 2015 07:28:00 gmt")},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=0"},
				"Expires":       []string{now.Add(time.Duration(3600) * time.Second).Format("wed, 21 oct 2015 07:28:00 gmt")},
				"Date":          []string{now.Format("wed, 21 oct 2015 07:28:00 gmt")},
				"X-Cache":       []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=0"},
				"Expires":       []string{now.Add(time.Duration(3600) * time.Second).Format("wed, 21 oct 2015 07:28:00 gmt")},
				"Date":          []string{now.Format("wed, 21 oct 2015 07:28:00 gmt")},
				"X-Cache":       []string{core.StatusUnreachable},
			}},
		},
		{
			name:              "An optimal HTTP cache reuses a response with positive Cache-Control: max-age and a CC extension present",
			endpoint:          "/freshness-max-age-extension",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"foobar, max-age=3600"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"foobar, max-age=3600"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"foobar, max-age=3600"}, "X-Cache": []string{core.StatusHit}}},
		},
		{
			name:              "An optimal HTTP cache reuses a response with positive Cache-Control: MaX-AgE",
			endpoint:          "/freshness-max-age-case-insenstive",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"MaX-aGe=3600"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"MaX-aGe=3600"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"MaX-aGe=3600"}, "X-Cache": []string{core.StatusHit}}},
		},
		{
			name:              "HTTP cache must not reuse a response with negative Cache-Control: max-age",
			endpoint:          "/freshness-max-age-negative",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=-3600"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=-3600"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=-3600"}, "X-Cache": []string{core.StatusUnreachable}}},
		},
		{
			name:              "An optimal shared HTTP cache reuses a response with positive Cache-Control: s-maxage",
			endpoint:          "/freshness-s-maxage-shared",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"s-maxage=3600"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"s-maxage=3600"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"s-maxage=3600"}, "X-Cache": []string{core.StatusHit}}},
		},
		{
			name:              "Shared HTTP cache must prefer short Cache-Control: s-maxage over a longer Cache-Control: max-age",
			endpoint:          "/freshness-max-age-s-maxage-shared-longer",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600, s-maxage=1"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600, s-maxage=1"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=3600, s-maxage=1"}, "X-Cache": []string{core.StatusMiss}}},
		},
		{
			name:              "Shared HTTP cache must prefer short Cache-Control: s-maxage over a longer Cache-Control: max-age (reversed)",
			endpoint:          "/freshness-max-age-s-maxage-shared-longer-reversed",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"s-maxage=1, max-age=3600"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"s-maxage=1, max-age=3600"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"s-maxage=1, max-age=3600"}, "X-Cache": []string{core.StatusMiss}}},
		},
		{
			name:              "An optimal shared HTTP cache prefers long Cache-Control: s-maxage over a shorter Cache-Control: max-age",
			endpoint:          "/freshness-max-age-s-maxage-shared-shorter",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=1, s-maxage=3600"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=1, s-maxage=3600"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=1, s-maxage=3600"}, "X-Cache": []string{core.StatusHit}}},
		},
		// FIXME: check max-age + s-maxage when expired
		// {
		// 	name:       "An optimal shared HTTP cache prefers long Cache-Control: s-maxage over Cache-Control: max-age=0, even with a past Expires",
		// 	endpoint:   "/freshness-max-age-s-maxage-shared-shorter-expires",
		// 	reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
		// 	upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Cache-Control": []string{"max-age=0, s-maxage=3600"},
		// 		"Expires":       []string{now.Truncate(time.Duration(10) * time.Second).Format("wed, 21 oct 2015 07:28:00 gmt")},
		// 	}},
		// 	expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Cache-Control": []string{"max-age=0, s-maxage=3600"},
		// 		"Expires":       []string{now.Truncate(time.Duration(10) * time.Second).Format("wed, 21 oct 2015 07:28:00 gmt")}, "X-Cache": []string{core.StatusMiss},
		// 	}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Cache-Control": []string{"max-age=0, s-maxage=3600"},
		// 		"Expires":       []string{now.Truncate(time.Duration(10) * time.Second).Format("wed, 21 oct 2015 07:28:00 gmt")}, "X-Cache": []string{core.StatusHit},
		// 	}},
		// },
		{
			name:              "HTTP cache must not reuse a response with max-age in a quoted string (before the \"real\" max-age)",
			endpoint:          "/freshness-max-age-ignore-quoted",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"extension=\"max-age=3600\", max-age=1"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"extension=\"max-age=3600\", max-age=1"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"extension=\"max-age=3600\", max-age=1"}, "X-Cache": []string{core.StatusUnreachable}}},
		},
		{
			name:              "HTTP cache must not reuse a response with max-age in a quoted string (before the \"real\" max-age)",
			endpoint:          "/freshness-max-age-ignore-quoted-2",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=1, extension=\"max-age=3600\""}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=1, extension=\"max-age=3600\""}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=1, extension=\"max-age=3600\""}, "X-Cache": []string{core.StatusUnreachable}}},
		},
		{
			name:              "An optimal HTTP cache reuses max-age with the value 003600",
			endpoint:          "/freshness-max-age-leading-zero",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=003600"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=003600"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=003600"}, "X-Cache": []string{core.StatusHit}}},
		},
		{
			name:              "HTTP cache must not reuse a response with a single-quoted Cache-Control: max-age",
			endpoint:          "/freshness-max-age-single-quoted",
			reqHeaders:        http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes:       http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age='3600'"}}},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age='3600'"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age='3600'"}, "X-Cache": []string{core.StatusUnreachable}}},
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
			utils.AssertEqual(t, tt.expectedFirstRes.Header.Get("Cache-Control"), resp.Header.Get("Cache-Control"))
			utils.AssertEqual(t, tt.expectedFirstRes.Header.Get("X-Cache"), resp.Header.Get("X-Cache"))

			// wait between the 2 requests (for freshness & stale checks)
			time.Sleep(2 * time.Second)

			// second request
			req = httptest.NewRequest("GET", tt.endpoint, nil)
			req.Header = tt.reqHeaders
			resp, err = testBouine.Test(req)
			utils.AssertEqual(t, nil, err)
			defer resp.Body.Close()
			utils.AssertEqual(t, tt.expectedSecondRes.StatusCode, resp.StatusCode)
			utils.AssertEqual(t, tt.expectedSecondRes.Header.Get("Cache-Control"), resp.Header.Get("Cache-Control"))
			utils.AssertEqual(t, tt.expectedSecondRes.Header.Get("X-Cache"), resp.Header.Get("X-Cache"))
		})
	}
}
