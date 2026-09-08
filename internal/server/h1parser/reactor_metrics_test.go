package h1parser

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetricsRing_PushDrainRoundTrip asserts the SPSC ring's basic
// contract: records pop in push order with all fields intact.
func TestMetricsRing_PushDrainRoundTrip(t *testing.T) {
	t.Parallel()
	r := &metricsRing{}
	require.True(t, r.pushHit(hitMetricsRecord{
		pool: "default", cacheResult: "HIT", source: "hot",
		durNs: 1234, bytesOut: 56, status: 200,
	}))
	require.True(t, r.pushHit(hitMetricsRecord{
		pool: "default", cacheResult: "STALE", source: "warm",
		durNs: 99, bytesOut: 7, status: 200,
	}))

	var buf [4]hitMetricsRecord
	n := r.drain(buf[:])
	require.Equal(t, 2, n)
	assert.Equal(t, "HIT", buf[0].cacheResult)
	assert.EqualValues(t, 1234, buf[0].durNs)
	assert.Equal(t, "STALE", buf[1].cacheResult)

	// Fully drained: no phantom records.
	assert.Equal(t, 0, r.drain(buf[:]))
}

// TestMetricsRing_OverflowDropsNewest fills the ring and asserts the
// drop policy: the push fails, the dropped counter increments, and
// the buffered records are unaffected (drop-newest, not overwrite —
// overwriting could tear a record the consumer is mid-read on).
func TestMetricsRing_OverflowDropsNewest(t *testing.T) {
	t.Parallel()
	r := &metricsRing{}
	for i := range metricsRingCap {
		require.True(t, r.pushHit(hitMetricsRecord{durNs: int64(i)}), "push %d", i)
	}
	require.False(t, r.pushHit(hitMetricsRecord{durNs: -1}), "full ring must drop")
	require.EqualValues(t, 1, r.droppedTotal())

	// All capacity records are intact and in order.
	var buf [metricsRingCap]hitMetricsRecord
	n := r.drain(buf[:])
	require.Equal(t, metricsRingCap, n)
	for i := range metricsRingCap {
		assert.EqualValues(t, i, buf[i].durNs, "record %d corrupted", i)
	}
	assert.EqualValues(t, 1, r.droppedTotal(), "drain must not clear drops")
}

// TestMetricsDrainer_AppliesRecordsThroughHook boots a drainer against
// a ring, pushes records, and asserts the hook observes exactly the
// pushed observations (order included) after a full drain.
func TestMetricsDrainer_AppliesRecordsThroughHook(t *testing.T) {
	t.Parallel()
	ring := &metricsRing{}
	var mu sync.Mutex
	var got []hitMetricsRecord
	hook := func(pool, cacheResult, source string, status, bytesOut int, duration time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, hitMetricsRecord{
			pool: pool, cacheResult: cacheResult, source: source,
			durNs: duration.Nanoseconds(), bytesOut: bytesOut,
			status: status,
		})
	}
	d := &metricsDrainer{ring: ring, hook: hook}

	require.True(t, ring.pushHit(hitMetricsRecord{
		pool: "default", cacheResult: "HIT", source: "hot",
		durNs: 42, bytesOut: 100, status: 200,
	}))
	require.True(t, ring.pushHit(hitMetricsRecord{
		pool: "default", cacheResult: "STALE", source: "warm",
		durNs: 7, bytesOut: 200, status: 200,
	}))

	stop := make(chan struct{})
	done := make(chan struct{})
	go d.run(stop, done)
	close(stop)
	<-done

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 2, "both records must reach the hook after stop")
	assert.Equal(t, "HIT", got[0].cacheResult)
	assert.Equal(t, "hot", got[0].source)
	assert.EqualValues(t, 42, got[0].durNs)
	assert.Equal(t, "STALE", got[1].cacheResult)
	assert.EqualValues(t, 7, got[1].durNs)
}

// TestMetricsDrainer_TickerDrainsWithoutStop asserts the drainer's
// polling loop applies records while running — a stop-then-drain-only
// drainer would batch metrics until shutdown, which the 20ms interval
// exists to avoid.
func TestMetricsDrainer_TickerDrainsWithoutStop(t *testing.T) {
	t.Parallel()
	ring := &metricsRing{}
	var count atomic.Int64
	d := &metricsDrainer{ring: ring, hook: func(string, string, string, int, int, time.Duration) {
		count.Add(1)
	}}
	stop := make(chan struct{})
	done := make(chan struct{})
	go d.run(stop, done)

	require.True(t, ring.pushHit(hitMetricsRecord{cacheResult: "HIT"}))
	require.True(t, ring.pushHit(hitMetricsRecord{cacheResult: "HIT"}))

	deadline := time.Now().Add(2 * time.Second)
	for count.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	<-done
	require.GreaterOrEqual(t, count.Load(), int64(2), "ticker must drain records while running")
}
