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
	"github.com/gofiber/fiber/v2"
	"github.com/thylong/bouine/internal/consensus"
)

// ConsensusMiddleware shares upstream responses as cache items to other nodes.
func ConsensusMiddleware(config consensus.Config) fiber.Handler {
	// Return new handler
	return func(c *fiber.Ctx) error {
		if consensus.IsLeader(config) {
			// Pass request to next middleware (ProxyMiddleware)
			return c.Next()
		}
		// Forwards to leader
		return consensus.ForwardToLeader(config)
	}
}
