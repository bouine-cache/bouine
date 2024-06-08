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
		ThirdReqHeaders   http.Header
		upstreamRes       http.Response
		expectedFirstRes  http.Response
		expectedSecondRes http.Response
		expectedThirdRes  http.Response
	}{
		// {
		// 	name:             "An optimal HTTP cache reuses a Vary response when the request matches",
		// 	endpoint:         "/vary-match",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}},
		// 	SecondReqHeaders: http.Header{"Foo": []string{"1"}},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Add(time.Second * 3000))},
		// 			"Vary":          []string{"Foo"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusHit}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// {
		// 	name:             "HTTP cache must not reuse Vary response when request doesn't match",
		// 	endpoint:         "/vary-no-match",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}},
		// 	SecondReqHeaders: http.Header{"Foo": []string{"2"}},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Add(time.Second * 3000))},
		// 			"Vary":          []string{"Foo"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// {
		// 	name:             "HTTP cache must not reuse Vary response when stored request omits variant request header",
		// 	endpoint:         "/vary-omit-stored",
		// 	firstReqHeaders:  http.Header{},
		// 	SecondReqHeaders: http.Header{"Foo": []string{"1"}},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
		// 			"Vary":          []string{"Foo"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// {
		// 	name:             "HTTP cache must not reuse Vary response when presented request omits variant request header",
		// 	endpoint:         "/vary-omit",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}},
		// 	SecondReqHeaders: http.Header{},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
		// 			"Vary":          []string{"Foo"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// // TODO: ensure body content matches 1st variant (and not 2nd variant)
		// {
		// 	name:             "An optimal HTTP cache can store two different variants",
		// 	endpoint:         "/vary-invalidate",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}},
		// 	SecondReqHeaders: http.Header{"Foo": []string{"2"}},
		// 	ThirdReqHeaders:  http.Header{"Foo": []string{"1"}},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
		// 			"Vary":          []string{"Foo"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedThirdRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusHit}}},
		// },
		// {
		// 	name:             "An optimal HTTP cache should not include headers not listed in Vary in the cache key",
		// 	endpoint:         "/vary-cache-key",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Other": []string{"2"}},
		// 	SecondReqHeaders: http.Header{"Foo": []string{"1"}, "Other": []string{"3"}},
		// 	ThirdReqHeaders:  http.Header{},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
		// 			"Vary":          []string{"Foo"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusHit}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// {
		// 	name:             "An optimal HTTP cache reuses a two-way Vary response when request matches",
		// 	endpoint:         "/vary-2-match",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Bar": []string{"abc"}},
		// 	SecondReqHeaders: http.Header{"Foo": []string{"1"}, "Bar": []string{"abc"}},
		// 	ThirdReqHeaders:  http.Header{},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
		// 			"Vary":          []string{"Foo, Bar"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusHit}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// {
		// 	name:             "HTTP cache must not reuse two-way Vary response when request doesn't match",
		// 	endpoint:         "/vary-2-match",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Bar": []string{"abc"}},
		// 	SecondReqHeaders: http.Header{"Foo": []string{"2"}, "Bar": []string{"abc"}},
		// 	ThirdReqHeaders:  http.Header{},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
		// 			"Vary":          []string{"Foo, Bar"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// {
		// 	name:             "HTTP cache must not reuse two-way Vary response when request omits variant request header",
		// 	endpoint:         "/vary-2-match-omit",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Bar": []string{"abc"}},
		// 	SecondReqHeaders: http.Header{},
		// 	ThirdReqHeaders:  http.Header{},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
		// 			"Vary":          []string{"Foo, Bar"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// {
		// 	name:             "An optimal HTTP cache reuses a three-way Vary response when request matches",
		// 	endpoint:         "/vary-3-match",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Bar": []string{"abc"}, "Baz": []string{"789"}},
		// 	SecondReqHeaders: http.Header{"Foo": []string{"1"}, "Bar": []string{"abc"}, "Baz": []string{"789"}},
		// 	ThirdReqHeaders:  http.Header{},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
		// 			"Vary":          []string{"Foo, Bar, Baz"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusHit}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// {
		// 	name:             "HTTP cache must not reuse three-way Vary response when request doesn't match",
		// 	endpoint:         "/vary-3-no-match",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Bar": []string{"abc"}, "Baz": []string{"789"}},
		// 	SecondReqHeaders: http.Header{"Foo": []string{"2"}, "Bar": []string{"abc"}, "Baz": []string{"789"}},
		// 	ThirdReqHeaders:  http.Header{},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
		// 			"Vary":          []string{"Foo, Bar, Baz"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		// {
		// 	name:             "HTTP cache must not reuse three-way Vary response when request doesn't match, regardless of header order",
		// 	endpoint:         "/vary-3-order",
		// 	firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Bar": []string{"abc"}, "Baz": []string{"789"}},
		// 	SecondReqHeaders: http.Header{"Foo": []string{"2"}, "Baz": []string{"789"}, "Bar": []string{"abcde"}},
		// 	ThirdReqHeaders:  http.Header{},
		// 	upstreamRes: http.Response{
		// 		StatusCode: 200, Header: http.Header{
		// 			"Cache-Control": []string{"max-age=5000"},
		// 			"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
		// 			"Vary":          []string{"Foo, Bar, Baz"},
		// 			"Date":          []string{nowAsGMT},
		// 		},
		// 	},
		// 	expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
		// 	expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		// },
		{
			name:             "An optimal HTTP cache reuses a three-way Vary response when both request and the original request omitted a variant header",
			endpoint:         "/vary-3-omit",
			firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Baz": []string{"789"}},
			SecondReqHeaders: http.Header{"Foo": []string{"1"}, "Baz": []string{"789"}},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{
				StatusCode: 200, Header: http.Header{
					"Cache-Control": []string{"max-age=5000"},
					"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
					"Vary":          []string{"Foo, Bar, Baz"},
					"Date":          []string{nowAsGMT},
				},
			},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusMiss}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusHit}}},
			expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not reuse Vary response with a value of *",
			endpoint:         "/vary-star",
			firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Baz": []string{"789"}},
			SecondReqHeaders: http.Header{"Foo": []string{"1"}, "Baz": []string{"789"}},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{
				StatusCode: 200, Header: http.Header{
					"Cache-Control": []string{"max-age=5000"},
					"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
					"Vary":          []string{"*"},
					"Date":          []string{nowAsGMT},
				},
			},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not reuse Vary response with a value of *",
			endpoint:         "/vary-syntax-star",
			firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Baz": []string{"789"}},
			SecondReqHeaders: http.Header{"Foo": []string{"1"}, "Baz": []string{"789"}},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{
				StatusCode: 200, Header: http.Header{
					"Cache-Control": []string{"max-age=5000"},
					"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
					"Vary":          []string{"*"},
					"Date":          []string{nowAsGMT},
				},
			},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not reuse Vary response with a value of *, *",
			endpoint:         "/vary-syntax-star-star",
			firstReqHeaders:  http.Header{"Foo": []string{"1"}, "Baz": []string{"789"}},
			SecondReqHeaders: http.Header{"Foo": []string{"1"}, "Baz": []string{"789"}},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{
				StatusCode: 200, Header: http.Header{
					"Cache-Control": []string{"max-age=5000"},
					"Last-Modified": []string{asGMT(now.Truncate(time.Second * 3000))},
					"Vary":          []string{"*, *"},
					"Date":          []string{nowAsGMT},
				},
			},
			expectedFirstRes:  http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": []string{"max-age=5000"}, "X-Cache": []string{core.StatusUnreachable}}},
			expectedThirdRes:  http.Response{StatusCode: fiber.StatusNotImplemented},
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
