package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// TestStress_PutGetRaceOnHits reproduces issue #218: a data race on
// obj.Hits between hot.Get (incrementing Hits under the shard write lock)
// and tiered.Put calling encodeObject(obj) (reading Hits outside the
// shard lock).
//
// The test runs concurrent Put and Get on the same key with a warm-tier
// enabled TieredStore. Objects are above the body threshold so Put calls
// encodeObject, and Get increments Hits. Under -race this fails without
// the fix.
func TestStress_PutGetRaceOnHits(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	k := api.Key(42)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup

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
				// Body above 1024 threshold → encodeObject is called in Put.
				o := bigObj(k, 2048)
				_ = ts.Put(ctx, k, o)
			}
		}()
	}

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
			}
		}()
	}

	wg.Wait()
}
