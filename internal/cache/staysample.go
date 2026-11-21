package cache

import "sync/atomic"

// stayinAliveLogSample is the 1-in-N sampling period for the Info-level
// "stayin-alive: serving stale" logs (issue #935). During an origin
// outage on a stayin_alive route every request logs today; sampling
// keeps the first event and one in N subsequent events per route.
const stayinAliveLogSample = 1024

// stayinLogSampler decides, per route, whether a stayin-alive stale
// serve should be logged at Info. It is a single atomic counter: the
// decision is a pure function of the counter value, so it is trivially
// testable and adds exactly one atomic.Add (no lock, no map, no time
// call, zero allocations) to the stale-serve path.
//
// Logged when: the counter is 0 (first event), or the incremented
// counter is a multiple of stayinAliveLogSample.
type stayinLogSampler struct {
	n atomic.Uint64
}

// shouldLog increments the counter and reports whether this event
// should be logged.
func (s *stayinLogSampler) shouldLog() bool {
	c := s.n.Add(1)
	return c == 1 || c%stayinAliveLogSample == 0
}
