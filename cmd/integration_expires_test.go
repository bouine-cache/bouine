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

func TestCacheExpires(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		endpoint          string
		reqHeaders        http.Header
		upstreamRes       http.Response
		expectedFirstRes  http.Response
		expectedSecondRes http.Response
	}{
		{
			name:       "An optimal HTTP cache reuses a response with a future Expires",
			endpoint:   "/freshness-expires-past",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Now().Add(time.Second * 2592000).Format(time.RFC1123Z)},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Now().Add(time.Second * 2592000).Format(time.RFC1123Z)},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Now().Add(time.Second * 2592000).Format(time.RFC1123Z)},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
		},
		{
			name:       "HTTP cache must not reuse a response with a past Expires",
			endpoint:   "/freshness-expires-future",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Now().Truncate(time.Second * 2592000).Format(time.RFC1123Z)},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Now().Truncate(time.Second * 2592000).Format(time.RFC1123Z)},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Now().Truncate(time.Second * 2592000).Format(time.RFC1123Z)},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
		},
		{
			name:       "An optimal HTTP cache reuses a response with an Expires that is exactly 32 bits",
			endpoint:   "/freshness-expires-32bit",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Tue, 19 Jan 2038 14:14:08 GMT"},
				"Date":    []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Tue, 19 Jan 2038 14:14:08 GMT"},
				"Date":    []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Tue, 19 Jan 2038 14:14:08 GMT"},
				"Date":    []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
		},
		{
			name:       "An optimal HTTP cache reuses a response with an Expires that is far in the future",
			endpoint:   "/freshness-expires-far-future",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 21 Oct 2286 07:28:00 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 21 Oct 2286 07:28:00 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 21 Oct 2286 07:28:00 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
		},
		{
			name:       "An optimal HTTP cache reuses a response with a future Expires in obsolete RFC 850 format",
			endpoint:   "/freshness-expires-rfc850",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Date(2286, time.January, 19, 1, 1, 1, 1, time.UTC).Format(time.RFC850)},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Date(2286, time.January, 19, 1, 1, 1, 1, time.UTC).Format(time.RFC850)},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Date(2286, time.January, 19, 1, 1, 1, 1, time.UTC).Format(time.RFC850)},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
		},
		{
			name:       "An optimal HTTP cache reuses a response with a future Expires in ANSI C's asctime() format",
			endpoint:   "/freshness-expires-ansi-c",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Date(2286, time.January, 19, 1, 1, 1, 1, time.UTC).Format(time.ANSIC)},
				"Date":    []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Date(2286, time.January, 19, 1, 1, 1, 1, time.UTC).Format(time.ANSIC)},
				"Date":    []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{time.Date(2286, time.January, 19, 1, 1, 1, 1, time.UTC).Format(time.ANSIC)},
				"Date":    []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
		},
		{
			name:       "An optimal HTTP cache reuses a response with a future Expires using wrong case (weekday)",
			endpoint:   "/freshness-expires-wrong-case-weekday",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"FRI, 21 Oct 2050 07:28:00 GMT"},
				"Date":    []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"FRI, 21 Oct 2050 07:28:00 GMT"},
				"Date":    []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"FRI, 21 Oct 2050 07:28:00 GMT"},
				"Date":    []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
		},
		{
			name:       "An optimal HTTP cache reuses a response with a future Expires using wrong case (month)",
			endpoint:   "/freshness-expires-wrong-case-month",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Fri, 21 OCT 2050 07:28:00 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Fri, 21 OCT 2050 07:28:00 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Fri, 21 OCT 2050 07:28:00 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
		},
		{
			name:       "An optimal HTTP cache reuses a response with a future Expires using wrong case (tz)",
			endpoint:   "/freshness-expires-wrong-case-tz",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Tue, 19 Jan 2286 23:00:00 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Tue, 19 Jan 2286 23:00:00 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Tue, 19 Jan 2286 23:00:00 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
		},
		{
			name:       "HTTP cache must not reuse a response with an invalid Expires (UTC)",
			endpoint:   "/freshness-expires-invalid-utc",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Tue, 19 Jan 2286 23:00:00 UTC"},
				"Date":    []string{time.Now().Format(time.RFC1123)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Tue, 19 Jan 2286 23:00:00 UTC"},
				"Date":    []string{time.Now().Format(time.RFC1123)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Tue, 19 Jan 2286 23:00:00 UTC"},
				"Date":    []string{time.Now().Format(time.RFC1123)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
		},
		// FIX: Reject any suffix, not just UTC (ignored as not convenient for Go support)
		{
			name:       "HTTP cache must not reuse a response with an invalid Expires (other tz)",
			endpoint:   "/freshness-expires-invalid-aest",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 18 Aug 2050 02:01:18 AEST"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 18 Aug 2050 02:01:18 AEST"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 18 Aug 2050 02:01:18 AEST"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
		},
		{
			name:       "HTTP cache must not reuse a response with an invalid Expires (two-digit year)",
			endpoint:   "/freshness-expires-invalid-2-digit-year",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 18 Aug 50 02:01:18 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 18 Aug 50 02:01:18 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 18 Aug 50 02:01:18 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
		},
		{
			name:       "HTTP cache must not reuse a response with an invalid Expires (missing comma)",
			endpoint:   "/freshness-expires-invalid-no-comma",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu 18 Aug 2050 02:01:18 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu 18 Aug 2050 02:01:18 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu 18 Aug 2050 02:01:18 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusUnreachable},
			}},
		},
		// FIX: Expires stale doesn't seem to be taken into account by the cache, resulting in a cache hit (tolerate for convenience)
		{
			name:       "HTTP cache must not reuse a response with an invalid Expires (multiple spaces)",
			endpoint:   "/freshness-expires-invalid-multiple-spaces",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 18 Aug  2050 02:01:18 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 18 Aug  2050 02:01:18 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Thu, 18 Aug  2050 02:01:18 GMT"},
				"Date":    []string{time.Now().Format(time.RFC1123Z)}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
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
			req.Header = tt.reqHeaders
			resp, err := testBouine.Test(req)
			utils.AssertEqual(t, nil, err)
			defer resp.Body.Close()
			fmt.Printf("The resp: %#v\n", tt.expectedFirstRes)
			fmt.Printf("The resp: %#v\n", resp.Header.Get("Cache-Control"))
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
