package cache

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

func TestRefreshHeapOrdering(t *testing.T) {
	t.Parallel()
	var h refreshHeap
	heapPush := func(key api.Key, at int64) {
		heap.Push(&h, &heapEntry{key: key, refreshAt: at})
	}
	heapPush(3, 300)
	heapPush(1, 100)
	heapPush(2, 200)

	if h.Len() != 3 {
		t.Fatalf("heap len = %d, want 3", h.Len())
	}
	// Pop in order.
	want := []api.Key{1, 2, 3}
	for i, w := range want {
		got := heap.Pop(&h).(*heapEntry).key
		if got != w {
			t.Fatalf("pop %d: got key %d, want %d", i, got, w)
		}
	}
}

func TestSchedulerScheduleAndStop(t *testing.T) {
	t.Parallel()

	var popped atomic.Int64
	onPop := func(key api.Key) {
		popped.Add(int64(key))
	}
	alive := func(key api.Key) *api.Object {
		return &api.Object{Key: key, TTL: 10 * time.Second}
	}

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()

	s.Schedule(api.Key(42), time.Now().Add(50*time.Millisecond))

	time.Sleep(200 * time.Millisecond)

	if got := popped.Load(); got != 42 {
		t.Fatalf("popped key = %d, want 42", got)
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
	var popped []api.Key
	onPop := func(key api.Key) {
		mu.Lock()
		popped = append(popped, key)
		mu.Unlock()
	}
	alive := func(key api.Key) *api.Object { return nil }

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()

	// Schedule far in the future.
	s.Schedule(api.Key(1), time.Now().Add(10*time.Second))
	// Schedule a nearer entry — should wake the drainer.
	s.Schedule(api.Key(2), time.Now().Add(50*time.Millisecond))

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(popped) != 1 || popped[0] != 2 {
		t.Fatalf("popped = %v, want [2]", popped)
	}
}

func TestSchedulerUpdateExistingKey(t *testing.T) {
	t.Parallel()

	var popped atomic.Int64
	onPop := func(key api.Key) {
		popped.Add(int64(key))
	}
	alive := func(key api.Key) *api.Object { return nil }

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()

	// Schedule key 1 far in the future.
	s.Schedule(api.Key(1), time.Now().Add(10*time.Second))
	// Update key 1 to fire soon.
	s.Schedule(api.Key(1), time.Now().Add(50*time.Millisecond))

	time.Sleep(200 * time.Millisecond)

	if got := popped.Load(); got != 1 {
		t.Fatalf("popped key = %d, want 1", got)
	}
	if s.Len() != 0 {
		t.Fatalf("heap len after pop = %d, want 0", s.Len())
	}
}

func TestSchedulerCompactionRemovesDeadEntries(t *testing.T) {
	t.Parallel()

	onPop := func(key api.Key) {}
	// alive returns nil for odd keys (dead), non-nil for even (live).
	alive := func(key api.Key) *api.Object {
		if int64(key)%2 == 0 {
			return &api.Object{Key: key, TTL: 10 * time.Second, StoredAt: time.Now()}
		}
		return nil
	}

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()

	// Schedule entries in the near-future window.
	for i := range 10 {
		s.Schedule(api.Key(i), time.Now().Add(2*time.Second))
	}
	if s.Len() != 10 {
		t.Fatalf("heap len before compaction = %d, want 10", s.Len())
	}

	s.compact()

	// Even keys (0,2,4,6,8) are live and should remain.
	// Odd keys (1,3,5,7,9) are dead and should be removed.
	if got := s.Len(); got != 5 {
		t.Fatalf("heap len after compaction = %d, want 5", got)
	}
}

func TestSchedulerEmptyHeapBlocksOnReady(t *testing.T) {
	t.Parallel()

	onPop := func(key api.Key) {}
	alive := func(key api.Key) *api.Object { return nil }

	s := NewRefreshScheduler(onPop, alive)
	s.Start()
	defer s.Stop()

	// Heap is empty — drainer should be blocked on ready, not busy-looping.
	// Just verify Stop works cleanly after a brief delay.
	time.Sleep(50 * time.Millisecond)
}
