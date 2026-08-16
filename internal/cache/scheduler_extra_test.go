package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestCompact_AllDeadEntries(t *testing.T) {
	t.Parallel()
	onPop := func(key api.Key) {}
	alive := func(key api.Key) *api.Object { return nil }
	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()
	for i := range 10 {
		s.Schedule(testkey.Key(uint64(i)), time.Now().Add(2*time.Second))
	}
	require.Equal(t, 10, s.Len())
	s.compact()
	assert.Equal(t, 0, s.Len())
}

func TestCompact_AllLiveEntries(t *testing.T) {
	t.Parallel()
	onPop := func(key api.Key) {}
	alive := func(key api.Key) *api.Object {
		return &api.Object{Key: key, TTL: 10 * time.Second, StoredAt: time.Now()}
	}
	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()
	for i := range 10 {
		s.Schedule(testkey.Key(uint64(i)), time.Now().Add(2*time.Second))
	}
	require.Equal(t, 10, s.Len())
	s.compact()
	assert.Equal(t, 10, s.Len())
}

func TestScheduler_RunTimerThenStop(t *testing.T) {
	t.Parallel()
	onPop := func(key api.Key) {}
	alive := func(key api.Key) *api.Object { return nil }
	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	// Schedule far in the future so the drainer enters the timer-wait path.
	s.Schedule(testkey.Key(1), time.Now().Add(10*time.Second))
	// Stop while timer is active — must not hang.
	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not terminate within 2s while timer was active")
	}
}

func TestScheduler_ConcurrentScheduleAndStop(t *testing.T) {
	t.Parallel()
	onPop := func(key api.Key) {}
	alive := func(key api.Key) *api.Object { return nil }
	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(n uint64) {
			defer wg.Done()
			s.Schedule(testkey.Key(n), time.Now().Add(50*time.Millisecond))
		}(uint64(i))
	}
	// Stop concurrently.
	s.Stop()
	wg.Wait()
	// After Stop, the drainer has exited. Some entries may have been
	// scheduled before stopped was set. The invariant is: no panic,
	// and the scheduler is in a clean stopped state.
	// Len() should be small (entries that slipped in before stopped).
	assert.LessOrEqual(t, s.Len(), 50)
}
