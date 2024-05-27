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
package middleware

import (
	"sync"
	"time"
)

type Upstreams struct {
	entries map[string]*upstream
	Mux     sync.RWMutex
}

type upstream struct {
	Ticker  *time.Ticker
	Healthy bool
}

// Set adds a new upstream to the map.
func (up *Upstreams) Set(key string, value upstream) {
	// obtain an exclusive lock
	up.Mux.Lock()
	// set the key/value in the map
	up.entries[key] = &value
	// release the exclusive lock
	up.Mux.Unlock()
}

// Get retrieves the value for a given key.
func (up *Upstreams) Get(key string) *upstream {
	// obtain a read lock
	up.Mux.RLock()
	// use defer to release the read lock
	// as we exit the function
	defer up.Mux.RUnlock()
	// return the value from the map
	return up.entries[key]
}

// Contains return true if the key is in the map.
func (up *Upstreams) Contains(key string) bool {
	// obtain a read lock
	up.Mux.RLock()
	// use defer to release the read lock
	// as we exit the function
	defer up.Mux.RUnlock()
	// return the value from the map

	_, ok := up.entries[key]
	return ok
}
