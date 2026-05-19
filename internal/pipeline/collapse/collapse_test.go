package collapse

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

func TestDo_SingleCaller(t *testing.T) {
	g := NewGroup()
	res, leader := g.Do(1, func() Result {
		return Result{StatusCode: 200, Body: []byte("ok")}
	})
	if !leader {
		t.Fatal("single caller should be leader")
	}
	if res.StatusCode != 200 {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

func TestDo_Collapse(t *testing.T) {
	g := NewGroup()
	var calls atomic.Int32

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, _ := g.Do(api.Key(42), func() Result {
				calls.Add(1)
				time.Sleep(50 * time.Millisecond)
				return Result{StatusCode: 200, Body: []byte("shared")}
			})
			if res.StatusCode != 200 {
				t.Errorf("status = %d", res.StatusCode)
			}
		}()
	}
	wg.Wait()

	if c := calls.Load(); c != 1 {
		t.Fatalf("fn called %d times, want 1", c)
	}
}

func TestDo_DifferentKeysNotCollapsed(t *testing.T) {
	g := NewGroup()
	var calls atomic.Int32

	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(key api.Key) {
			defer wg.Done()
			g.Do(key, func() Result {
				calls.Add(1)
				time.Sleep(10 * time.Millisecond)
				return Result{StatusCode: 200}
			})
		}(api.Key(i))
	}
	wg.Wait()

	if c := calls.Load(); c != 5 {
		t.Fatalf("fn called %d times, want 5 (one per key)", c)
	}
}

func TestInFlight(t *testing.T) {
	g := NewGroup()
	started := make(chan struct{})

	go func() {
		g.Do(1, func() Result {
			close(started)
			time.Sleep(100 * time.Millisecond)
			return Result{}
		})
	}()
	<-started
	time.Sleep(10 * time.Millisecond)

	if g.InFlight() != 1 {
		t.Fatalf("inflight = %d, want 1", g.InFlight())
	}
}
