package cache

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// heapEntry is a single entry in the refresh min-heap.
// It records when a cache key should be background-refreshed.
type heapEntry struct {
	key       api.Key
	refreshAt int64 // unix nano
	index     int   // heap index (container/heap bookkeeping)
}

// refreshHeap implements heap.Interface. The entry with the smallest
// refreshAt is at the top (index 0).
type refreshHeap []*heapEntry

func (h refreshHeap) Len() int { return len(h) }

func (h refreshHeap) Less(i, j int) bool {
	return h[i].refreshAt < h[j].refreshAt
}

func (h refreshHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *refreshHeap) Push(x any) {
	entry := x.(*heapEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *refreshHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return entry
}

// compactionInterval is how often the scheduler runs a compaction
// pass to remove dead entries (keys whose objects have been evicted,
// banned, or deleted). Without compaction, dead entries accumulate
// until the drainer naturally reaches their refreshAt.
const compactionInterval = 60 * time.Second

// compactionWindow extends the compaction scan slightly past "now"
// to catch entries that are about to fire. Entries with
// refreshAt <= now + compactionWindow are popped and checked.
const compactionWindow = 5 * time.Second

// RefreshScheduler is a min-heap of cache keys keyed by their
// scheduled refresh time. A single drainer goroutine pops the
// earliest entry, sleeps until its refreshAt, and calls handler.triggerBgRefresh.
//
// Dead entries (evicted/banned/deleted objects) are bounded by the
// periodic compaction pass: every compactionInterval, entries in the
// near-future window are popped and re-inserted only if their object
// is still live and fresh.
//
// The scheduler is per-Handler (not shared across routes) so that hot
// reload can stop and GC the old scheduler cleanly.
type RefreshScheduler struct {
	mu    sync.Mutex
	heap  refreshHeap
	index map[api.Key]*heapEntry // O(1) lookup for updates
	done  chan struct{}
	ready chan struct{} // signals the drainer to wake early (new top)
	wg    sync.WaitGroup

	// stopped is set atomically when Stop is called. Schedule checks
	// this to avoid inserting entries into a heap whose drainer has
	// exited (which would leak memory).
	stopped atomic.Bool

	// onPop is called when an entry's refreshAt has elapsed.
	// The callback must not block — it should spawn a goroutine.
	onPop func(key api.Key)

	// alive checks whether key is still in the store and fresh.
	// Returns the live object, or nil if the key is gone or stale.
	// Used by the compaction pass to filter dead entries.
	alive func(key api.Key) *api.Object
}

// NewRefreshScheduler creates a scheduler. onPop is called for each
// entry whose refreshAt has elapsed. alive is used by the compaction
// pass to check whether a key is still live.
func NewRefreshScheduler(onPop func(key api.Key), alive func(key api.Key) *api.Object) *RefreshScheduler {
	s := &RefreshScheduler{
		done:  make(chan struct{}),
		ready: make(chan struct{}, 1),
		index: make(map[api.Key]*heapEntry),
		onPop: onPop,
		alive: alive,
	}
	return s
}

// Start launches the drainer goroutine. Must be called once before
// any Schedule calls. Stop must be called to join the drainer.
func (s *RefreshScheduler) Start() {
	s.wg.Add(1)
	go s.run()
}

// Schedule inserts or updates a key's refresh time in the heap.
// refreshAt is the absolute time at which the background refresh
// should fire. If the key already exists in the heap, its refreshAt
// is updated and the heap is fixed up.
func (s *RefreshScheduler) Schedule(key api.Key, refreshAt time.Time) {
	if s.stopped.Load() {
		return
	}
	s.mu.Lock()
	// Re-check under lock in case Stop ran between the atomic check and here.
	if s.stopped.Load() {
		s.mu.Unlock()
		return
	}
	if e, ok := s.index[key]; ok {
		e.refreshAt = refreshAt.UnixNano()
		heap.Fix(&s.heap, e.index)
		s.mu.Unlock()
		s.wake()
		return
	}
	entry := &heapEntry{key: key, refreshAt: refreshAt.UnixNano()}
	heap.Push(&s.heap, entry)
	s.index[key] = entry
	s.mu.Unlock()
	s.wake()
}

// Stop closes the done channel and waits for the drainer to exit.
// After Stop, no more onPop callbacks will fire.
func (s *RefreshScheduler) Stop() {
	s.stopped.Store(true)
	close(s.done)
	s.wake()
	s.wg.Wait()
}

// Len returns the number of entries in the heap.
func (s *RefreshScheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heap.Len()
}

// wake signals the drainer to re-evaluate the heap top. Non-blocking:
// if a signal is already pending, the extra signal is dropped.
func (s *RefreshScheduler) wake() {
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// run is the drainer goroutine. It sleeps until the next entry's
// refreshAt, pops it, calls onPop, and repeats. A periodic compaction
// pass removes dead entries.
func (s *RefreshScheduler) run() {
	defer s.wg.Done()

	compactionTicker := time.NewTicker(compactionInterval)
	defer compactionTicker.Stop()

	for {
		s.mu.Lock()
		if s.heap.Len() == 0 {
			s.mu.Unlock()
			select {
			case <-s.done:
				return
			case <-s.ready:
				continue
			}
		}
		top := s.heap[0]
		delay := time.Until(time.Unix(0, top.refreshAt))
		s.mu.Unlock()

		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-s.done:
				timer.Stop()
				return
			case <-s.ready:
				timer.Stop()
				continue
			case <-timer.C:
			}
		}

		s.mu.Lock()
		if s.heap.Len() == 0 {
			s.mu.Unlock()
			continue
		}
		entry := heap.Pop(&s.heap).(*heapEntry)
		delete(s.index, entry.key)
		s.mu.Unlock()

		s.onPop(entry.key)

		// Check for compaction on each iteration — cheap because
		// compaction only touches the near-future window.
		select {
		case <-compactionTicker.C:
			s.compact()
		default:
		}
	}
}

// compact removes dead entries from the heap. It pops all entries
// with refreshAt <= now + compactionWindow, checks alive(key), and
// re-inserts only those whose object is still live and fresh.
// This bounds dead entries to O(refreshRate × compactionInterval).
func (s *RefreshScheduler) compact() {
	now := time.Now().UnixNano()
	cutoff := now + compactionWindow.Nanoseconds()

	var live []*heapEntry
	s.mu.Lock()
	for s.heap.Len() > 0 {
		top := s.heap[0]
		if top.refreshAt > cutoff {
			break
		}
		entry := heap.Pop(&s.heap).(*heapEntry)
		delete(s.index, entry.key)
		// Call alive while holding the lock. This blocks Schedule
		// briefly, but alive is a hot-tier Get (microseconds) and
		// compaction touches only the near-future window (a few
		// entries). Holding the lock prevents a concurrent Schedule
		// from re-inserting the same key and causing a double-pop.
		obj := s.alive(entry.key)
		if obj != nil {
			live = append(live, entry)
		}
	}
	for _, e := range live {
		heap.Push(&s.heap, e)
		s.index[e.key] = e
	}
	s.mu.Unlock()
}
