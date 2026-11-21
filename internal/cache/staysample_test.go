package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStayinLogSampler pins issue #935's sampling decision: the first
// event always logs, then exactly every stayinAliveLogSample-th event.
func TestStayinLogSampler(t *testing.T) {
	t.Parallel()
	var s stayinLogSampler
	assert.True(t, s.shouldLog(), "first event must log (counter == 1)")
	logged := 1
	for i := 2; i <= 3*stayinAliveLogSample; i++ {
		if s.shouldLog() {
			logged++
			assert.Equal(t, 0, i%stayinAliveLogSample, "only multiples of the period (plus the first) log")
		}
	}
	assert.Equal(t, 4, logged, "first + 3 periods within 3*N events")
}

// TestStayinLogSampler_ZeroAlloc pins the zero-allocation property of
// the sampling decision on the stale-serve path.
func TestStayinLogSampler_ZeroAlloc(t *testing.T) {
	var s stayinLogSampler
	allocs := testing.AllocsPerRun(1000, func() {
		s.shouldLog()
	})
	assert.Equal(t, float64(0), allocs, "shouldLog must not allocate")
}
