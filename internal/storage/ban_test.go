package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestBan_ParallelEvictsAllMatching verifies that Ban with parallel
// shard scanning evicts all matching entries across all shards and
// returns the correct total count.
func TestBan_ParallelEvictsAllMatching(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 16})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 1000
	for i := range n {
		k := api.Key(i)
		o := obj(k, 64)
		o.Header.Set(header.XBouineHost, "example.com")
		o.Header.Set(header.XBouinePath, fmt.Sprintf("/ban-%d", i))
		_ = s.Put(context.Background(), k, o)
	}

	count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "example\\.com"})
	if err != nil {
		t.Fatalf("Ban: %v", err)
	}
	if count != n {
		t.Fatalf("ban count = %d, want %d", count, n)
	}

	for i := range n {
		if got, _, _ := s.Get(context.Background(), api.Key(i)); got != nil {
			t.Errorf("key %d still present after ban", i)
		}
	}
}

// TestBan_ParallelNonMatchingRegexReturnsZero verifies that a
// non-matching regex returns 0 and leaves all entries intact.
func TestBan_ParallelNonMatchingRegexReturnsZero(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 8})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 500
	for i := range n {
		k := api.Key(i)
		o := obj(k, 64)
		o.Header.Set(header.XBouineHost, "keep.example.com")
		_ = s.Put(context.Background(), k, o)
	}

	count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "^/never-matches$"})
	if err != nil {
		t.Fatalf("Ban: %v", err)
	}
	if count != 0 {
		t.Fatalf("ban count = %d, want 0", count)
	}

	for i := range n {
		if got, _, _ := s.Get(context.Background(), api.Key(i)); got == nil {
			t.Errorf("key %d missing after non-matching ban", i)
		}
	}
}

// TestBan_ParallelPartialMatch verifies that Ban only evicts matching
// entries and leaves non-matching ones in the store.
func TestBan_ParallelPartialMatch(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 8})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 400
	for i := range n {
		k := api.Key(i)
		o := obj(k, 64)
		if i%2 == 0 {
			o.Header.Set(header.XBouinePath, "/ban-me")
		} else {
			o.Header.Set(header.XBouinePath, "/keep")
		}
		_ = s.Put(context.Background(), k, o)
	}

	count, err := s.Ban(context.Background(), api.BanExpr{PathRegex: "^/ban-me$"})
	if err != nil {
		t.Fatalf("Ban: %v", err)
	}
	if count != n/2 {
		t.Fatalf("ban count = %d, want %d", count, n/2)
	}

	for i := range n {
		got, _, _ := s.Get(context.Background(), api.Key(i))
		if i%2 == 0 {
			if got != nil {
				t.Errorf("key %d should have been banned", i)
			}
		} else {
			if got == nil {
				t.Errorf("key %d should still be present", i)
			}
		}
	}
}

// TestBan_ParallelEvictionCount verifies that the eviction counter
// matches the returned count after parallel ban.
func TestBan_ParallelEvictionCount(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 16})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 800
	for i := range n {
		k := api.Key(i)
		o := obj(k, 64)
		o.Header.Set(header.XBouineHost, "evict.example.com")
		_ = s.Put(context.Background(), k, o)
	}

	before := s.stats.evictions.Load()
	count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "evict\\.example\\.com"})
	if err != nil {
		t.Fatalf("Ban: %v", err)
	}
	after := s.stats.evictions.Load()

	if count != n {
		t.Fatalf("ban count = %d, want %d", count, n)
	}
	if after-before != int64(n) {
		t.Fatalf("eviction counter delta = %d, want %d", after-before, n)
	}
}

// TestBan_ParallelSurrogateKey verifies that parallel Ban correctly
// matches by surrogate key across shards.
func TestBan_ParallelSurrogateKey(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 16})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 600
	for i := range n {
		k := api.Key(i)
		o := obj(k, 64)
		if i%3 == 0 {
			o.SurrogateKeys = []string{"target"}
		} else {
			o.SurrogateKeys = []string{"other"}
		}
		_ = s.Put(context.Background(), k, o)
	}

	count, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: "target"})
	if err != nil {
		t.Fatalf("Ban: %v", err)
	}
	want := n / 3
	if count != want {
		t.Fatalf("ban count = %d, want %d", count, want)
	}

	for i := range n {
		got, _, _ := s.Get(context.Background(), api.Key(i))
		if i%3 == 0 {
			if got != nil {
				t.Errorf("key %d with surrogate 'target' should have been banned", i)
			}
		} else {
			if got == nil {
				t.Errorf("key %d with surrogate 'other' should still be present", i)
			}
		}
	}
}

// TestBan_ParallelConcurrentBans verifies that two concurrent Ban
// calls don't race or corrupt state. Run with -race.
func TestBan_ParallelConcurrentBans(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 32})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 1000
	for i := range n {
		k := api.Key(i)
		o := obj(k, 64)
		if i%2 == 0 {
			o.Header.Set(header.XBouineHost, "a.example.com")
		} else {
			o.Header.Set(header.XBouineHost, "b.example.com")
		}
		_ = s.Put(context.Background(), k, o)
	}

	var total atomic.Int64
	var banErr atomic.Value // error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "a\\.example\\.com"})
		if err != nil {
			banErr.Store(err)
			return
		}
		total.Add(int64(count))
	}()
	go func() {
		defer wg.Done()
		count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "b\\.example\\.com"})
		if err != nil {
			banErr.Store(err)
			return
		}
		total.Add(int64(count))
	}()
	wg.Wait()

	if err, ok := banErr.Load().(error); ok && err != nil {
		t.Fatalf("concurrent Ban failed: %v", err)
	}

	// Both bans match disjoint host patterns, so every entry is
	// evicted by exactly one ban. total must equal n.
	if got := total.Load(); got != int64(n) {
		t.Fatalf("total evicted = %d, want %d", got, n)
	}
	for i := range n {
		if got, _, _ := s.Get(context.Background(), api.Key(i)); got != nil {
			t.Errorf("key %d still present after both bans", i)
		}
	}
}
