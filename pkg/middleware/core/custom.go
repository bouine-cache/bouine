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
	"strconv"
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

	// skip cache middleware on 'Cache-Control: max-age=0'
	if bytes.Equal(c.Context().Response.Header.Peek("Cache-Control"), []byte("max-age=0")) {
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

	fmt.Printf("Expires header %s\n", bytes.ToTitle(c.Context().Response.Header.Peek("Expires")))
	fmt.Printf("Date header %s\n", bytes.ToTitle(c.Context().Response.Header.Peek("Date")))

	// skip cache on certain invalid Expires format cases (still tolerate cases mix)
	// skip cache on past Expires
	if invalidOrOutdatedExpires(
		bytes.ToTitle(c.Context().Response.Header.Peek("Expires")),
		bytes.ToTitle(c.Context().Response.Header.Peek("Date")),
		c.Context().Response.Header.Peek("Cache-Control"),
		c.Context().Response.Header.Peek("Age"),
	) {
		fmt.Println("EXPIRED EXPIRES")
		return true
	}

	fmt.Println("Bruh")
	if AgeExceedsCacheDuration(
		c.Context().Response.Header.Peek("Age"),
		c.Context().Response.Header.Peek("Cache-Control"),
	) {
		fmt.Println("EXPIRED AGE")
		return true
	}

	return false
}

// TODO: breakdown this into multiple functions:
// 1. func for invalidExpires
// 2. func for Expired (comparison to Date & Age)
// 3. funcs should return (bool, err) for observability.
func invalidOrOutdatedExpires(expHeader, dateHeader, cacheControlHeader, ageHeader []byte) bool {
	if len(expHeader) == 0 {
		return false
	}
	// HTTP cache must not reuse a response with an invalid Expires (UTC)
	if bytes.HasSuffix(expHeader, []byte("UTC")) {
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
	layout, err := matchRFCLayout(string(expHeader), rfcLayouts)
	if err != nil {
		fmt.Println("Error validating Expires header:", err)

		re := regexp.MustCompile(`max-age=([0-9]*)`)
		maxAgeDirective := re.FindSubmatch(cacheControlHeader)

		// If Expires is invalid but max-age is set, we still want to serve cache
		fmt.Println("getting close")
		if len(maxAgeDirective) != 0 {
			if maxAge, err := time.ParseDuration(fmt.Sprintf("%ss", maxAgeDirective[1])); maxAge > 0 && err == nil {
				// In this case Date header is ignored completely
				return false
			}
		}
		return true
	}

	fmt.Println("made it HERE ")
	e, err := time.Parse(layout, string(expHeader))
	if err != nil {
		fmt.Println("made it HERE 1")
		return false
	}

	dateLayout, err := matchRFCLayout(string(dateHeader), rfcLayouts)
	if err != nil {
		return false
	}
	date, _ := time.Parse(dateLayout, string(dateHeader))

	fmt.Printf("expHeader %s\n", expHeader)
	fmt.Printf("dateHeader %s\n", dateHeader)
	fmt.Printf("Exp layout %s\n", layout)
	fmt.Printf("Date layout %s\n", dateLayout)
	fmt.Printf("Exp value %s\n", e.In(time.UTC).String())
	fmt.Printf("date value %s\n", date.In(time.UTC).String())

	if e.Before(date) {
		fmt.Println("made it HERE 3")
		return true
	}

	// in this case, Expires is older than Age
	if len(ageHeader) != 0 {
		age, err := time.ParseDuration(fmt.Sprintf("%ss", ageHeader))
		// ignore invalid Age headers
		if err != nil {
			return false
		}

		if e.Before(time.Now().Add(age)) {
			return true
		}
	}
	fmt.Println("made it HERE 4")
	return false
}

func AgeExceedsCacheDuration(ageHeader, cacheControlHeader []byte) bool {
	// in this case, Expires is older than Age
	fmt.Println("Bruh1")
	if len(ageHeader) != 0 {
		// ignore float values
		if bytes.Contains(ageHeader, []byte(".")) {
			fmt.Println("Bruh4")
			return false
		}
		age, _ := time.ParseDuration(fmt.Sprintf("%ss", ageHeader))

		re := regexp.MustCompile(`max-age=([0-9]*)`)
		maxAgeDirective := re.FindSubmatch(cacheControlHeader)

		fmt.Println("Bruh2")
		// If Expires is invalid but max-age is set, we still want to serve cache
		if len(maxAgeDirective) != 0 {
			fmt.Println("Bruh3")
			// ignore Age float value
			if maxAge, err := time.ParseDuration(fmt.Sprintf("%ss", maxAgeDirective[1])); age > maxAge && err == nil {
				return true
			}
		}
	}

	return false
}

// CustomExpirationGenerator returns cache key expiration time.
// Use Upstream response expires, max-age or s-maxage from the response otherwise fallbacks to default config value.
func CustomExpirationGenerator(c *fiber.Ctx, cfg *cache.Config) time.Duration {
	var maxAgeDuration time.Duration
	var sMaxAgeDuration time.Duration
	var err error

	// max-age
	re := regexp.MustCompile(`max-age=([0-9]*)`)
	if maxAge := re.FindSubmatch(c.Context().Response.Header.Peek("Cache-Control")); maxAge != nil {
		// Still cache if max-age exceeds maximum duration
		if ma, _ := strconv.Atoi(string(maxAge[1])); ma > 99999999 {
			maxAge[1] = []byte("99999999")
		}
		maxAgeDuration, err = time.ParseDuration(fmt.Sprintf("%ss", maxAge[1]))
		if err != nil {
			fmt.Printf("BROOOOOOOOOOOOOOOOOOO: %s\n", err)
			return time.Duration(0)
		}
	}

	// s-maxage
	re = regexp.MustCompile(`s-maxage=([0-9]*)`)
	if sMaxAge := re.FindSubmatch(c.Context().Response.Header.Peek("Cache-Control")); sMaxAge != nil {
		// Still cache if s-maxage exceeds maximum duration
		if sma, _ := strconv.Atoi(string(sMaxAge[1])); sma > 99999999 {
			sMaxAge[1] = []byte("99999999")
		}
		sMaxAgeDuration, err = time.ParseDuration(fmt.Sprintf("%ss", sMaxAge[1]))
		if err != nil {
			fmt.Printf("BROOOOOOOOOOOOOOOOOOO: %s\n", err)
			return time.Duration(0)
		}
	}

	if sMaxAgeDuration > time.Duration(0) {
		return sMaxAgeDuration
	} else if maxAgeDuration > time.Duration(0) {
		return maxAgeDuration
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
