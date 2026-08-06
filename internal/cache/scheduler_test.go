package cache

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestRefreshHeapOrdering(t *testing.T) {
	t.Parallel()
	var h refreshHeap
	heapPush := func(key api.Key, at int64) {
		heap.Push(&h, &heapEntry{key: key, refreshAt: at})
	}
	heapPush(testkey.From(3), 300)
	heapPush(testkey.From(1), 100)
	heapPush(testkey.From(2), 200)

	require.Equal(t, 3, h.Len())
	// Pop in order.
	want := []api.Key{testkey.From(1), testkey.From(2), testkey.From(3)}
	for _, w := range want {
		got := heap.Pop(&h).(*heapEntry).key
		require.Equal(t, w, got)
	}
}

func TestSchedulerScheduleAndStop(t *testing.T) {
	t.Parallel()

	popped := make(chan api.Key, 1)
	onPop := func(key api.Key) {
		popped <- key
	}
	alive := func(key api.Key) *api.Object {
		return &api.Object{Key: key, TTL: 10 * time.Second}
	}

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()

	s.Schedule(testkey.From(42), time.Now().Add(50*time.Millisecond))

	select {
	case got := <-popped:
		require.Equal(t, testkey.From(42), got)
	case <-time.After(time.Second):
		t.Fatal("drainer did not pop within 1s")
	}
}

func TestSchedulerStopTerminatesDrainer(t *testing.T) {
	t.Parallel()

	onPop := func(key api.Key) {}
	alive := func(key api.Key) *api.Object { return nil }

	s := NewRefreshScheduler(onPop, alive)
	s.Start()

	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not terminate drainer within 1s")
	}
}

func TestSchedulerWakesOnEarlierTop(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	popped := make([]api.Key, 0)
	done := make(chan struct{})
	onPop := func(key api.Key) {
		mu.Lock()
		popped = append(popped, key)
		mu.Unlock()
		if len(popped) == 1 {
			close(done)
		}
	}
	alive := func(key api.Key) *api.Object { return nil }

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()

	// Schedule far in the future.
	s.Schedule(testkey.From(1), time.Now().Add(10*time.Second))
	// Schedule a nearer entry — should wake the drainer.
	s.Schedule(testkey.From(2), time.Now().Add(50*time.Millisecond))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drainer did not pop key 2 within 1s")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(popped) != 1 || popped[0] != testkey.From(2) {
		t.Fatalf("popped = %v, want [2]", popped)
	}
}

func TestSchedulerUpdateExistingKey(t *testing.T) {
	t.Parallel()

	popped := make(chan api.Key, 1)
	onPop := func(key api.Key) {
		popped <- key
	}
	alive := func(key api.Key) *api.Object { return nil }

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()

	// Schedule key 1 far in the future.
	s.Schedule(testkey.From(1), time.Now().Add(10*time.Second))
	// Update key 1 to fire soon.
	s.Schedule(testkey.From(1), time.Now().Add(50*time.Millisecond))

	select {
	case got := <-popped:
		require.Equal(t, testkey.From(1), got)
	case <-time.After(time.Second):
		t.Fatal("drainer did not pop within 1s")
	}
	require.Equal(t, 0, s.Len())
}

func TestSchedulerCompactionRemovesDeadEntries(t *testing.T) {
	t.Parallel()

	onPop := func(key api.Key) {}
	// alive returns nil for odd keys (dead), non-nil for even (live).
	alive := func(key api.Key) *api.Object {
		if key.Hash64()%2 == 0 {
			return &api.Object{Key: key, TTL: 10 * time.Second, StoredAt: time.Now()}
		}
		return nil
	}

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()

	// Schedule entries in the near-future window.
	for i := range 10 {
		s.Schedule(testkey.From(uint64(i)), time.Now().Add(2*time.Second))
	}
	require.Equal(t, 10, s.Len())

	s.compact()

	// Even keys (0,2,4,6,8) are live and should remain.
	// Odd keys (1,3,5,7,9) are dead and should be removed.
	got := s.Len()
	require.Equal(t, 5, got)
}

func TestSchedulerEmptyHeapBlocksOnReady(t *testing.T) {
	t.Parallel()

	onPop := func(key api.Key) {}
	alive := func(key api.Key) *api.Object { return nil }

	s := NewRefreshScheduler(onPop, alive)
	s.Start()

	// Verify Stop works cleanly — if the drainer were busy-looping
	// on an empty heap, Stop would hang waiting for the goroutine.
	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not terminate drainer within 1s (empty heap busy-loop?)")
	}
}

func TestSchedulerScheduleAfterStopIsNoop(t *testing.T) {
	t.Parallel()

	onPop := func(key api.Key) {}
	alive := func(key api.Key) *api.Object { return nil }

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	s.Stop()

	// Schedule after Stop should not insert into the heap.
	s.Schedule(testkey.From(1), time.Now().Add(50*time.Millisecond))
	require.Equal(t, 0, s.Len())
}

// TestSchedulerIndexConsistency verifies that the index map stays
// consistent with the heap after Schedule, Pop, and compact.
func TestSchedulerIndexConsistency(t *testing.T) {
	t.Parallel()

	var popped atomic.Int64
	onPop := func(key api.Key) {
		popped.Add(int64(key.Hash64()))
	}
	alive := func(key api.Key) *api.Object { return nil }

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()

	s.Schedule(testkey.From(1), time.Now().Add(50*time.Millisecond))
	s.Schedule(testkey.From(2), time.Now().Add(60*time.Millisecond))
	s.Schedule(testkey.From(3), time.Now().Add(70*time.Millisecond))

	// Update key 1 to fire later — should not create a duplicate.
	s.Schedule(testkey.From(1), time.Now().Add(80*time.Millisecond))
	require.Equal(t, 3, s.Len())

	// Wait for all three to pop.
	// Key 2 pops first (60ms), then key 3 (70ms), then key 1 (80ms).
	total := int64(1 + 2 + 3)
	poll.Eventually(t, 2*time.Second, 10*time.Millisecond, func() bool {
		return popped.Load() == total
	})
	require.Equal(t, 0, s.Len())
}
