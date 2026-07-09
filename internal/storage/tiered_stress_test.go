package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// TestStress_PutGetRaceOnHits reproduces issue #218: a data race on the
// hot-tier hit counter between hot.Get (incrementing hits under the shard
// write lock) and concurrent readers.
//
// The test runs concurrent Put, Get, and Hits on the same key with a
// warm-tier-enabled TieredStore. Objects are above the body threshold so
// Put calls encodeObject (the original race site, now Hits-free). Get
// increments hits on hotEntry under the shard write lock. Hits reads
// hits under the shard read lock. Under -race this fails without the fix
// if hits were still on api.Object and accessed without synchronization.
//
// Skipped in -short mode (pre-commit) because it is a wall-clock stress
// test; the full suite runs it in CI and on pre-push.
func TestStress_PutGetRaceOnHits(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test: requires wall-clock time, skipped in -short")
	}
	t.Parallel()
	ts := tieredStore(t, true)
	k := api.Key(42)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Writers: Put calls encodeObject, which no longer touches hits.
	const writers = 4
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				o := bigObj(k, 2048) // above 1024 threshold → encodeObject
				_ = ts.Put(ctx, k, o)
			}
		}()
	}

	// Readers: Get increments hits (under shard write lock). Hits reads
	// hits (under shard read lock). Both synchronize through the shard
	// lock — no race.
	const readers = 8
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				_, _, _ = ts.Get(ctx, k)
				_ = ts.Hits(k)
			}
		}()
	}

	wg.Wait()
}
