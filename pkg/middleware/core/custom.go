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

package core

import (
	"bytes"
	"fmt"
	"regexp"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cache"
)

const (
	StatusUnreachable string = "unreachable"
	StatusHit         string = "hit"
	StatusMiss        string = "miss"
)

// use time.Until wrapper to allow predictive testing.
var until func(time.Time) time.Duration = func(deadline time.Time) time.Duration {
	return time.Until(deadline)
}

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

	// skip cache on certain invalid Expires format cases (still tolerate cases mix)
	// skip cache on past Expires
	if exp := bytes.ToTitle(c.Context().Response.Header.Peek("Expires")); len(exp) != 0 {
		// HTTP cache must not reuse a response with an invalid Expires (UTC)
		if bytes.HasSuffix(exp, []byte("UTC")) {
			return true
		}
		// List of RFC layouts to check against
		rfcLayouts := []string{
			time.RFC1123,
			time.RFC1123Z,
			time.ANSIC,
			time.RFC850,
		}

		// Function to determine which RFC layout the date string matches
		matchRFCLayout := func(dateStr string, layouts []string) (string, error) {
			for _, layout := range layouts {
				if _, err := time.Parse(layout, dateStr); err == nil {
					return layout, nil
				}
			}
			return "", fmt.Errorf("given Expires does not match any known RFC layout")
		}

		// Determine which RFC layout the date string matches
		layout, err := matchRFCLayout(string(exp), rfcLayouts)
		if err != nil {
			fmt.Println("Error validating Expires header:", err)
			return true
		}

		e, err := time.Parse(layout, string(exp))
		if err != nil {
			return false
		}
		date, err := time.Parse(layout, string(c.Context().Response.Header.Peek("Date")))
		if err != nil {
			return false
		}
		if e.Before(date) {
			return true
		}
	}

	return false
}

// CustomExpirationGenerator returns cache key expiration time.
// Use Upstream response expires, max-age or s-maxage from the response otherwise fallbacks to default config value.
func CustomExpirationGenerator(c *fiber.Ctx, cfg *cache.Config) time.Duration {
	// max-age
	re := regexp.MustCompile(`max-age=([0-9]*)`)
	if maxAge := re.FindSubmatch(c.Context().Response.Header.Peek("Cache-Control")); maxAge != nil {
		// TODO: handle badly formatted max-age
		maxAgeDuration, _ := time.ParseDuration(fmt.Sprintf("%ss", maxAge[1]))
		return maxAgeDuration
	}

	// s-maxage
	re = regexp.MustCompile(`s-maxage=([0-9]*)`)
	if sMaxAge := re.FindSubmatch(c.Context().Response.Header.Peek("Cache-Control")); sMaxAge != nil {
		// TODO: handle badly formatted s-maxage
		sMaxAgeDuration, _ := time.ParseDuration(fmt.Sprintf("%ss", sMaxAge[1]))
		return sMaxAgeDuration
	}

	// Expires (in case both max-age & s-maxage are missing)
	expiresTime, err := time.Parse(time.RFC1123, string(c.Context().Response.Header.Peek("Expires")))
	if err != nil {
		return cfg.Expiration
	}
	if expiresDuration := until(expiresTime); expiresDuration > 0 {
		return expiresDuration
	}

	return cfg.Expiration
}
