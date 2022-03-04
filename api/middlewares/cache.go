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

	"github.com/gofiber/fiber/v2"
)

// CacheSkippable returns true if proxied responses shouldn't be cached according to RFC7234 (Bouine default configuration).
func CacheSkippable(c *fiber.Ctx) bool {
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

	// skip cache middleware on 'Cache-Control: no-store'
	if bytes.Equal(c.Context().Response.Header.Peek("Cache-Control"), []byte("no-store")) {
		return true
	}

	// skip cache middleware on 'Cache-Control: private'
	if bytes.Equal(c.Context().Response.Header.Peek("Cache-Control"), []byte("private")) {
		return true
	}

	// FIXME: skip cache middleware on Basic Auth requests without 'Cache-Control: public'
	return false
}
