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
package consensus

import "errors"

type Config struct {
	Leader string
}

var ErrForwardToLeaderAsLeader = errors.New("cannot forward to leader as leader")

// ForwardToLeader forwards request to leader and return leader response.
func ForwardToLeader(config Config) error {
	if IsLeader(config) {
		return ErrForwardToLeaderAsLeader
	}
	return nil
}

// IsLeader returns true if the current node is cluster leader.
func IsLeader(config Config) bool {
	return config.Leader == "leader" // Not implemented yet
}
