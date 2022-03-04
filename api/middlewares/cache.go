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
//
package middlewares

import (
	"bytes"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cache"
)

// CacheSkippable returns true if proxied responses shouldn't be cached according to RFC7234 (Bouine default configuration).
func CacheSkippable(c *fiber.Ctx) bool {
	// force cache middleware on 'Cache-Control: public'
	if bytes.Equal(c.Context().Response.Header.Peek("Cache-Control"), []byte("public")) {
		return false
	}

	// skip cache middleware on res.statusCode == 500
	if c.Context().Response.StatusCode() == fiber.StatusInternalServerError {
		return true
	}

	// skip cache middleware on res.statusCode == 502
	if c.Context().Response.StatusCode() == fiber.StatusBadGateway {
		return true
	}

	// skip cache middleware on res.statusCode == 503
	if c.Context().Response.StatusCode() == fiber.StatusServiceUnavailable {
		return true
	}

	// skip cache middleware on res.statusCode == 504
	if c.Context().Response.StatusCode() == fiber.StatusGatewayTimeout {
		return true
	}

	// skip cache middleware if Authorization header is present in the request
	if len(c.Context().Request.Header.Peek("Authorization")) > 0 {
		return true
	}

	// skip cache middleware on 'Cache-Control: no-store'
	if bytes.Equal(c.Context().Response.Header.Peek("Cache-Control"), []byte("no-store")) {
		return true
	}

	// skip cache middleware on 'Cache-Control: private'
	if bytes.Equal(c.Context().Response.Header.Peek("Cache-Control"), []byte("private")) {
		return true
	}

	return false
}

// CustomExpirationGenerator return cache key expiration time.
// Use Upstream response expires, max-age or s-maxage otherwise default to config default value.
func CustomExpirationGenerator(c *fiber.Ctx, cfg *cache.Config) time.Duration {
	// s-maxage
	// if bytes.Contains(c.Context().Response.Header.Peek("Cache-Control"), []byte("s-maxage")) {
	// 	return <the_s-maxage>
	// }
	// max-age
	// if bytes.Contains(c.Context().Response.Header.Peek("Cache-Control"), []byte("max-age")) {
	// 	return <the_max-age>
	// }

	// Expires (in case max-age & s-maxage are missing)
	// Uses RFC5322 Date/Time format
	expiresTime, err := time.Parse("Thu, 01 Dec 1994 16:00:00 GMT", string(c.Context().Response.Header.Peek("Expires")))
	if err != nil {
		return cfg.Expiration
	}
	if expiresDuration := time.Now().Sub(expiresTime); expiresDuration > 0 {
		return expiresDuration
	}

	// newCacheTime, _ := strconv.Atoi(c.GetRespHeader("Cache-Time", "600"))
	// time.Second * time.Duration(newCacheTime)

	return cfg.Expiration
}
