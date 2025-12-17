// Package collapse implements single-flight request collapsing per
// cache key. When multiple requests arrive for the same cache miss
// simultaneously, only one origin fetch is issued; the rest block on
// a channel and receive the same result.
package collapse

import (
	"net/http"
	"sync"

	"github.com/thylong/bouine/pkg/api"
)

// Result is the shared outcome of a collapsed request.
type Result struct {
	StatusCode int
	Header     http.Header
	Body       []byte
	Err        error
}

// Group manages in-flight requests. It is goroutine-safe.
type Group struct {
	mu    sync.Mutex
	calls map[api.Key]*call
}

type call struct {
	done chan struct{}
	res  Result
}

// NewGroup creates a collapse group.
func NewGroup() *Group {
	return &Group{calls: make(map[api.Key]*call)}
}

// Do deduplicates concurrent calls for the same key. The first caller
// (leader) executes fn; subsequent callers (followers) block until the
// leader finishes and receive the same Result.
//
// Returns the result and whether this call was the leader.
func (g *Group) Do(key api.Key, fn func() Result) (Result, bool) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		<-c.done
		return c.res, false
	}
	c := &call{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	c.res = fn()
	close(c.done)

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return c.res, true
}

// InFlight returns the number of keys currently being fetched.
func (g *Group) InFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}
