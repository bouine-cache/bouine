package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/utils"
	"github.com/thylong/bouine/pkg/middleware"
)

func TestCacheAgeW3CStandards(t *testing.T) {
	tests := []struct {
		name              string
		endpoint          string
		reqHeaders        http.Header
		upstreamRes       http.Response
		expectedFirstRes  http.Response
		expectedSecondRes http.Response
	}{
		{
			name:       "HTTP cache should ignore an Age header with a non-numeric value",
			endpoint:   "/age-parse-nonnumeric",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"abc"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"abc"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"abc"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusHit},
			}},
		},
		{
			name:       "HTTP cache should ignore an Age header with a negative value",
			endpoint:   "/age-parse-negative",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"-7200"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"-7200"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"-7200"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusHit},
			}},
		},
		{
			name:       "HTTP cache should ignore an Age header with a float value",
			endpoint:   "/age-parse-float",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"7200.0"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"7200.0"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"7200.0"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusHit},
			}},
		},
		// TODO: Age stale doesn't seem to be taken into account by the cache, resulting in a cache hit
		// {
		// 	name:       "HTTP cache should consider a response with a Age value of 2147483647 to be stale",
		// 	endpoint:   "/age-parse-large-minus-one",
		// 	reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
		// 	upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"2147483647"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
		// 	}},
		// 	expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"2147483647"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		// 	}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"2147483647"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		// 	}},
		// },
		// TODO: Age stale doesn't seem to be taken into account by the cache, resulting in a cache hit
		// {
		// 	name:       "HTTP cache should consider a response with a Age value of 2147483648 to be stale",
		// 	endpoint:   "/age-parse-large-minus-one",
		// 	reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
		// 	upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"2147483648"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
		// 	}},
		// 	expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"2147483648"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		// 	}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"2147483648"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		// 	}},
		// },
		// TODO: Age stale doesn't seem to be taken into account by the cache, resulting in a cache hit
		// {
		// 	name:       "HTTP cache should consider a response with a Age value of 2147483649 to be stale",
		// 	endpoint:   "/age-parse-large-minus-one",
		// 	reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
		// 	upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"2147483649"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
		// 	}},
		// 	expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"2147483649"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		// 	}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"2147483649"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		// 	}},
		// },
		// TODO: Age stale doesn't seem to be taken into account by the cache, resulting in a cache hit
		// {
		// 	name:       "HTTP cache should consider a response with a single Age header line old, 0 to be stale",
		// 	endpoint:   "/age-parse-suffix",
		// 	reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
		// 	upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"7200, 0"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
		// 	}},
		// 	expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"7200, 0"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		// 	}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"7200, 0"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		// 	}},
		// },
		{
			name:       "HTTP cache should consider a response with a single Age header line 0, old to be fresh",
			endpoint:   "/age-parse-prefix",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"0, 7200"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"0, 7200"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"0, 7200"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusHit},
			}},
		},
		{
			name:       "HTTP cache should consider a response with a single line Age: 0, 0 to be fresh",
			endpoint:   "/age-parse-dup-0",
			reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"0, 0"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"0, 0"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Age": []string{"0, 0"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusHit},
			}},
		},
		// TODO: Age stale doesn't seem to be taken into account by the cache, resulting in a cache hit
		//  {
		//  	name:       "Does HTTP cache consider an alphabetic parameter on Age header to be valid?",
		//  	endpoint:   "/age-parse-parameter",
		//  	reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
		//  	upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
		//  		"Age": []string{"7200;foo=bar"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
		//  	}},
		//  	expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
		//  		"Age": []string{"7200;foo=bar"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		//  	}},
		//  	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
		//  		"Age": []string{"7200;foo=bar"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		//  	}},
		//  },
		// TODO: Age stale doesn't seem to be taken into account by the cache, resulting in a cache hit
		// {
		// 	name:       "Does HTTP cache should consider a numeric parameter on Age header to be valid?",
		// 	endpoint:   "/age-parse-numeric-parameter",
		// 	reqHeaders: http.Header{"Content-Type": []string{"application/json"}},
		// 	upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"7200;foo=111"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"},
		// 	}},
		// 	expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"7200;foo=111"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		// 	}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
		// 		"Age": []string{"7200;foo=111"}, "Date": []string{time.Now().Format("wed, 21 oct 2015 07:28:00 gmt")}, "Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{middleware.StatusMiss},
		// 	}},
		// },
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
