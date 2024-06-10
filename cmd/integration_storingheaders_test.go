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

func TestCacheStoringHeaders(t *testing.T) {
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
			name:             "HTTP cache must store Test-Header header field",
			endpoint:         "/headers-store-Test-Header",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Test-Header": []string{"aywusqomkigecay"},
				"Date":        []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Test-Header":   []string{"aywusqomkigecay"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Test-Header":   []string{"aywusqomkigecay"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store X-Test-Header header field",
			endpoint:         "/headers-store-X-Test-Header",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-Test-Header": []string{"abcdefghijklmno"},
				"Date":          []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-Test-Header": []string{"abcdefghijklmno"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-Test-Header": []string{"abcdefghijklmno"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Content-Foo header field",
			endpoint:         "/headers-store-Content-Foo",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Foo": []string{"auoicwqkeysmgau"},
				"Date":        []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Foo":   []string{"auoicwqkeysmgau"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Foo":   []string{"auoicwqkeysmgau"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store X-Content-Foo header field",
			endpoint:         "/headers-store-X-Content-Foo",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-Content-Foo": []string{"axurolifczwtqnk"},
				"Date":          []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-Content-Foo": []string{"axurolifczwtqnk"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-Content-Foo": []string{"axurolifczwtqnk"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Cache-Control header field",
			endpoint:         "/headers-store-Cache-Control",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Date": []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store Connection header field",
			endpoint:         "/headers-store-Connection",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Connection": []string{"askcumewogyqias"},
				"Date":       []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Connection":    []string{""},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Connection":    []string{""},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Content-Encoding header field",
			endpoint:         "/headers-store-Content-Encoding",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Encoding": []string{"apetixmbqfujync"},
				"Date":             []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Encoding": []string{"apetixmbqfujync"},
				"Cache-Control":    []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Encoding": []string{"apetixmbqfujync"},
				"Cache-Control":    []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Content-Length header field",
			endpoint:         "/headers-store-Content-Length",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Length": []string{"28"},
				"Date":           []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Length": []string{"28"},
				"Cache-Control":  []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Length": []string{"28"},
				"Cache-Control":  []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Content-Location header field",
			endpoint:         "/headers-store-Content-Location",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Location": []string{"/bar"},
				"Date":             []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Location": []string{"/bar"},
				"Cache-Control":    []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Location": []string{"/bar"},
				"Cache-Control":    []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Content-MD5 header field",
			endpoint:         "/headers-store-Content-MD5",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-MD5": []string{"N7UdGUp1E+RbVvZSTy1R8g=="},
				"Date":        []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-MD5":   []string{"N7UdGUp1E+RbVvZSTy1R8g=="},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-MD5":   []string{"N7UdGUp1E+RbVvZSTy1R8g=="},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Content-Range header field",
			endpoint:         "/headers-store-Content-Range",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Range": []string{"ananananananana"},
				"Date":          []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Range": []string{"ananananananana"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Range": []string{"ananananananana"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Content-Security-Policy header field",
			endpoint:         "/headers-store-Content-Security-Policy",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Security-Policy": []string{"default-src 'self' cdn.example.com"},
				"Date":                    []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Security-Policy": []string{"default-src 'self' cdn.example.com"},
				"Cache-Control":           []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Security-Policy": []string{"default-src 'self' cdn.example.com"},
				"Cache-Control":           []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Content-Type header field",
			endpoint:         "/headers-store-Content-Type",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Type": []string{"text/plain;charset=utf-8"},
				"Date":         []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Type":  []string{"text/plain;charset=utf-8"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Content-Type":  []string{"text/plain;charset=utf-8"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Clear-Site-Data header field",
			endpoint:         "/headers-store-Clear-Site-Data",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Clear-Site-Data": []string{"cookies"},
				"Date":            []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Clear-Site-Data": []string{"cookies"},
				"Cache-Control":   []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Clear-Site-Data": []string{"cookies"},
				"Cache-Control":   []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store ETag header field",
			endpoint:         "/headers-store-ETag",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"ETag": []string{"ghijkl"},
				"Date": []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"ETag":          []string{"ghijkl"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"ETag":          []string{"ghijkl"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Expires header field",
			endpoint:         "/headers-store-Expires",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires": []string{"Tue, 19 Jan 2038 14:14:08 GMT"},
				"Date":    []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires":       []string{"Tue, 19 Jan 2038 14:14:08 GMT"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Expires":       []string{"Tue, 19 Jan 2038 14:14:08 GMT"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store Keep-Alive header field",
			endpoint:         "/headers-store-Keep-Alive",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Keep-Alive": []string{"ananananananana"},
				"Date":       []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Keep-Alive":    []string{"ananananananana"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Keep-Alive":    []string{""},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store Proxy-Authenticate header field",
			endpoint:         "/headers-store-Proxy-Authenticate",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Authenticate": []string{"akueoyiscmwgqak"},
				"Date":               []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Authenticate": []string{"akueoyiscmwgqak"},
				"Cache-Control":      []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Authenticate": []string{""},
				"Cache-Control":      []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store Proxy-Authentication-Info header field",
			endpoint:         "/headers-store-Proxy-Authentication-Info",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Authentication-Info": []string{"aaaaaaaaaaaaaaa"},
				"Date":                      []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Authentication-Info": []string{"aaaaaaaaaaaaaaa"},
				"Cache-Control":             []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Authentication-Info": []string{""},
				"Cache-Control":             []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store Proxy-Authorization header field",
			endpoint:         "/headers-store-Proxy-Authorization",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Authorization": []string{"aaaaaaaaaaaaaaa"},
				"Date":                []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Authorization": []string{"aaaaaaaaaaaaaaa"},
				"Cache-Control":       []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Authorization": []string{""},
				"Cache-Control":       []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store ProxProxy-Authentication-Infy-Connection header field",
			endpoint:         "/headers-store-Proxy-Connection",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Connection": []string{"alwhsdozkvgrcny"},
				"Date":             []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Connection": []string{"alwhsdozkvgrcny"},
				"Cache-Control":    []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Proxy-Connection": []string{""},
				"Cache-Control":    []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Public-Key-Pins header field",
			endpoint:         "/headers-store-Public-Key-Pins",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Public-Key-Pins": []string{"askcumewogyqias"},
				"Date":            []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Public-Key-Pins": []string{"askcumewogyqias"},
				"Cache-Control":   []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Public-Key-Pins": []string{"askcumewogyqias"},
				"Cache-Control":   []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store Set-Cookie header field",
			endpoint:         "/headers-store-Set-Cookie",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Set-Cookie":    []string{"a=c"},
				"Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Set-Cookie": []string{"a=c"},
				"X-Cache":    []string{core.StatusUnreachable},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Set-Cookie": []string{"a=c"},
				"X-Cache":    []string{core.StatusUnreachable},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store TE header field",
			endpoint:         "/headers-store-TE",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"TE":   []string{"apetixmbqfujync"},
				"Date": []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"TE":            []string{"apetixmbqfujync"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"TE":            []string{""},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store Transfer-Encoding header field",
			endpoint:         "/headers-store-Transfer-Encoding",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Transfer-Encoding": []string{"arizqhypgxofwne"},
				"Date":              []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Transfer-Encoding": []string{""},
				"Cache-Control":     []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Transfer-Encoding": []string{""},
				"Cache-Control":     []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must not store Upgrade header field",
			endpoint:         "/headers-store-Upgrade",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"Upgrade": []string{"acegikmoqsuwyac"},
				"Date":    []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"Upgrade":       []string{"acegikmoqsuwyac"},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"Upgrade":       []string{""},
				"Cache-Control": []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store X-Frame-Options header field",
			endpoint:         "/headers-store-X-Frame-Options",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-Frame-Options": []string{"sameorigin"},
				"Date":            []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-Frame-Options": []string{"sameorigin"},
				"Cache-Control":   []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-Frame-Options": []string{"sameorigin"},
				"Cache-Control":   []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
			}},
			expectedThirdRes: http.Response{StatusCode: fiber.StatusNotImplemented},
		},
		{
			name:             "HTTP cache must store X-XSS-Protection header field",
			endpoint:         "/headers-store-X-XSS-Protection",
			firstReqHeaders:  http.Header{},
			SecondReqHeaders: http.Header{},
			ThirdReqHeaders:  http.Header{},
			upstreamRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-XSS-Protection": []string{"1; mode=block"},
				"Date":             []string{nowAsGMT}, "Cache-Control": []string{"max-age=3600"},
			}},
			expectedFirstRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-XSS-Protection": []string{"1; mode=block"},
				"Cache-Control":    []string{"max-age=3600"}, "X-Cache": []string{core.StatusMiss},
			}},
			expectedSecondRes: http.Response{StatusCode: 200, Header: http.Header{
				"X-XSS-Protection": []string{"1; mode=block"},
				"Cache-Control":    []string{"max-age=3600"}, "X-Cache": []string{core.StatusHit},
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
			for key := range tt.expectedFirstRes.Header {
				fmt.Printf("Expected Headers %#v\n", tt.expectedFirstRes.Header)
				fmt.Printf("Expected key '%s'\n", key)
				fmt.Printf("Expected Header %s%s%s\n", key, ":", tt.expectedFirstRes.Header[key][0])
				fmt.Printf("Actual Header %s%s%s\n", key, ":", resp.Header.Get(key))
				utils.AssertEqual(t, tt.expectedFirstRes.Header[key][0], resp.Header.Get(key))
			}

			// wait between the 2 requests (for freshness & stale checks)
			time.Sleep(3 * time.Second)

			// second request
			req = httptest.NewRequest("GET", tt.endpoint, nil)
			req.Header = tt.SecondReqHeaders
			resp, err = testBouine.Test(req)
			utils.AssertEqual(t, nil, err)
			defer resp.Body.Close()
			utils.AssertEqual(t, tt.expectedSecondRes.StatusCode, resp.StatusCode)

			for key := range tt.expectedSecondRes.Header {
				fmt.Printf("Expected Headers %#v\n", tt.expectedFirstRes.Header)
				fmt.Printf("Expected key '%s'\n", key)
				fmt.Printf("Expected Header %s%s%s\n", key, ":", tt.expectedFirstRes.Header[key][0])
				fmt.Printf("Actual Header %s%s%s\n", key, ":", resp.Header.Get(key))
				utils.AssertEqual(t, tt.expectedSecondRes.Header[key][0], resp.Header.Get(key))
			}

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
