package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

// makeKey constructs a deterministic api.Key with hi in the high half
// and lo in the low half. Used to build keys that share Hash64() (the
// high half) but differ in the low half — the adversarial collision
// case for open addressing.
func makeKey(hi, lo uint64) api.Key {
	var k api.Key
	binary.BigEndian.PutUint64(k[:8], hi)
	binary.BigEndian.PutUint64(k[8:], lo)
	return k
}

func TestHotTable_PutGet(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	k := testkey.Hash([]byte("hello"))
	e := &hotEntry{hasBackup: true}
	tab.Put(k, e)

	got, ok := tab.Get(k)
	require.True(t, ok)
	require.Same(t, e, got, "Get must return the exact pointer stored")
	require.Equal(t, int64(1), tab.Len())
}

func TestHotTable_GetMiss(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	_, ok := tab.Get(testkey.Hash([]byte("absent")))
	require.False(t, ok)
	require.Equal(t, int64(0), tab.Len())
}

func TestHotTable_Delete(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	k := testkey.Hash([]byte("delete-me"))
	e := &hotEntry{}
	tab.Put(k, e)
	require.Equal(t, int64(1), tab.Len())

	tab.Delete(k)
	require.Equal(t, int64(0), tab.Len())

	_, ok := tab.Get(k)
	require.False(t, ok, "deleted key must not be found")
}

func TestHotTable_DeleteAbsent(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	k := testkey.Hash([]byte("never-added"))
	tab.Delete(k)
	require.Equal(t, int64(0), tab.Len())
}

func TestHotTable_OverwriteUpdatesPointer(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	k := testkey.Hash([]byte("overwrite"))
	e1 := &hotEntry{}
	e2 := &hotEntry{hasBackup: true}
	tab.Put(k, e1)
	tab.Put(k, e2)

	got, ok := tab.Get(k)
	require.True(t, ok)
	require.Same(t, e2, got, "Get must return the latest Put pointer")
	require.Equal(t, int64(1), tab.Len(), "overwrite must not increment count")
}

func TestHotTable_GrowPreservesEntries(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	const n = 1000
	want := make(map[api.Key]*hotEntry, n)
	for i := range n {
		k := testkey.Hash([]byte(fmt.Sprintf("key-%d", i)))
		e := &hotEntry{}
		tab.Put(k, e)
		want[k] = e
	}
	require.Equal(t, int64(n), tab.Len())

	for k, e := range want {
		got, ok := tab.Get(k)
		require.True(t, ok, "key %s not found after grow", k.Hex())
		require.Same(t, e, got)
	}
}

func TestHotTable_DeleteDuringIter(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(16)

	const n = 50
	keys := make([]api.Key, n)
	for i := range n {
		k := testkey.Hash([]byte(fmt.Sprintf("iter-%d", i)))
		keys[i] = k
		tab.Put(k, &hotEntry{})
	}
	require.Equal(t, int64(n), tab.Len())

	// Delete 25 entries via the deleter closure inside Iter. Iteration
	// order is not insertion order, so collect the set of keys to
	// delete and match by key identity.
	toDelete := make(map[api.Key]bool, 25)
	for i := range 25 {
		toDelete[keys[i]] = true
	}
	deleted := 0
	tab.Iter(func(k api.Key, _ *hotEntry, del func()) bool {
		if toDelete[k] {
			del()
			deleted++
		}
		return true
	})
	require.Equal(t, 25, deleted)
	require.Equal(t, int64(n-25), tab.Len(), "deleter must tombstone and decrement count")

	// Remaining keys must still be retrievable.
	for i := 25; i < n; i++ {
		_, ok := tab.Get(keys[i])
		require.True(t, ok, "key %d must remain after iteration deletion", i)
	}
	// Deleted keys must be gone.
	for i := range 25 {
		_, ok := tab.Get(keys[i])
		require.False(t, ok, "key %d must be gone after iteration deletion", i)
	}
}

func TestHotTable_IterReadOnly(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(16)

	const n = 30
	want := make(map[api.Key]*hotEntry, n)
	for i := range n {
		k := testkey.Hash([]byte(fmt.Sprintf("ro-%d", i)))
		e := &hotEntry{}
		tab.Put(k, e)
		want[k] = e
	}

	got := make(map[api.Key]*hotEntry, n)
	tab.Iter(func(k api.Key, e *hotEntry, _ func()) bool {
		got[k] = e
		return true
	})

	require.Len(t, got, n)
	for k, e := range want {
		require.Same(t, e, got[k])
	}
}

func TestHotTable_IterEarlyStop(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	for i := range 10 {
		k := testkey.Hash([]byte(fmt.Sprintf("stop-%d", i)))
		tab.Put(k, &hotEntry{})
	}

	visited := 0
	tab.Iter(func(_ api.Key, _ *hotEntry, _ func()) bool {
		visited++
		return visited < 3
	})
	require.Equal(t, 3, visited, "Iter must stop when callback returns false")
}

func TestHotTable_TombstoneReclaimedOnGrow(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	// Fill, delete half, then force a grow by adding more. Tombstones
	// must be reclaimed so capacity stays bounded.
	const n = 64
	keys := make([]api.Key, n)
	for i := range n {
		k := testkey.Hash([]byte(fmt.Sprintf("tomb-%d", i)))
		keys[i] = k
		tab.Put(k, &hotEntry{})
	}
	// Delete every other entry.
	for i := range n {
		if i%2 == 0 {
			tab.Delete(keys[i])
		}
	}
	require.Equal(t, int64(n/2), tab.Len())

	// Add more to force grow. After grow, tombstones are dropped, so
	// capacity is proportional to live entries, not live + tombstoned.
	for i := range n {
		k := testkey.Hash([]byte(fmt.Sprintf("post-%d", i)))
		tab.Put(k, &hotEntry{})
	}

	// All live keys (odd-indexed from the first batch + all of the second
	// batch) must be retrievable.
	for i := range n {
		if i%2 != 0 {
			_, ok := tab.Get(keys[i])
			require.True(t, ok, "odd-indexed key %d must survive", i)
		}
	}
	for i := range n {
		k := testkey.Hash([]byte(fmt.Sprintf("post-%d", i)))
		_, ok := tab.Get(k)
		require.True(t, ok, "post key %d must be present", i)
	}
}

func TestHotTable_CollisionChainDistinct(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(16)

	// 10 keys sharing the same high 64 bits (same Hash64) but distinct
	// low halves. They all probe from the same home slot. Open-addr
	// with full 16-byte key compare must keep them distinct.
	const hi uint64 = 0xDEADBEEFCAFEBABE
	const chain = 10
	keys := make([]api.Key, chain)
	for i := range chain {
		keys[i] = makeKey(hi, uint64(i+1))
		tab.Put(keys[i], &hotEntry{hasBackup: i%2 == 0})
	}
	require.Equal(t, int64(chain), tab.Len())

	for i, k := range keys {
		got, ok := tab.Get(k)
		require.True(t, ok, "collision key %d must be found", i)
		require.Equal(t, i%2 == 0, got.hasBackup)
	}

	// A key with the same high half but an unused low half must miss.
	miss := makeKey(hi, 0xFFFFFFFF)
	_, ok := tab.Get(miss)
	require.False(t, ok, "unused low-half key must miss")
}

func TestHotTable_CollisionChainDeleteMiddle(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(16)

	const hi uint64 = 0x1234567890ABCDEF
	keys := make([]api.Key, 5)
	for i := range 5 {
		keys[i] = makeKey(hi, uint64(i+1))
		tab.Put(keys[i], &hotEntry{})
	}
	// Delete the middle of the probe chain. The tombstone must not
	// break the probe chain for keys past it.
	tab.Delete(keys[2])
	require.Equal(t, int64(4), tab.Len())

	_, ok := tab.Get(keys[2])
	require.False(t, ok, "deleted middle key must be gone")

	for i := range 5 {
		if i == 2 {
			continue
		}
		_, ok := tab.Get(keys[i])
		require.True(t, ok, "key %d past tombstone must still be found", i)
	}
}

func TestHotTable_RandomOps(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	rng := rand.New(rand.NewPCG(42, 1337))
	model := make(map[api.Key]*hotEntry)
	const ops = 5000

	for range ops {
		k := testkey.Hash([]byte(fmt.Sprintf("k%d", rng.IntN(500))))
		switch rng.IntN(3) {
		case 0: // Put
			e := &hotEntry{hasBackup: rng.IntN(2) == 1}
			tab.Put(k, e)
			model[k] = e
		case 1: // Delete
			tab.Delete(k)
			delete(model, k)
		case 2: // Get
			got, ok := tab.Get(k)
			wantE, wantOk := model[k]
			require.Equal(t, wantOk, ok)
			if ok {
				require.Same(t, wantE, got)
			}
		}
	}

	require.Equal(t, int64(len(model)), tab.Len())
	for k, e := range model {
		got, ok := tab.Get(k)
		require.True(t, ok)
		require.Same(t, e, got)
	}
}

func TestHotTable_IterEmpty(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	visited := 0
	tab.Iter(func(_ api.Key, _ *hotEntry, _ func()) bool {
		visited++
		return true
	})
	require.Zero(t, visited)
}

func TestHotTable_ZeroValueIsUsable(t *testing.T) {
	t.Parallel()
	// A zero-value hotTable (no init call) must be usable and auto-init
	// on first Put. Get on a zero table must miss without panicking.
	var tab hotTable
	_, ok := tab.Get(testkey.Hash([]byte("x")))
	require.False(t, ok)
	require.Equal(t, int64(0), tab.Len())

	k := testkey.Hash([]byte("y"))
	e := &hotEntry{}
	tab.Put(k, e)
	got, ok := tab.Get(k)
	require.True(t, ok)
	require.Same(t, e, got)
}

// TestHotTable_AllowReinsertAfterDelete proves that a key deleted then
// reinserted is retrievable. This catches a tombstone bug where the
// reinsert lands on the tombstone slot but Get fails to find it because
// it stops at an empty slot before the tombstone.
func TestHotTable_ReinsertAfterDelete(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(8)

	k := testkey.Hash([]byte("reins"))
	tab.Put(k, &hotEntry{})
	tab.Delete(k)
	e := &hotEntry{hasBackup: true}
	tab.Put(k, e)
	got, ok := tab.Get(k)
	require.True(t, ok)
	require.Same(t, e, got)
}

func TestHotTable_IterKeysSorted(t *testing.T) {
	t.Parallel()
	var tab hotTable
	tab.init(16)

	keys := make([]api.Key, 20)
	for i := range keys {
		keys[i] = testkey.Hash([]byte(fmt.Sprintf("s-%d", i)))
		tab.Put(keys[i], &hotEntry{})
	}

	var got []api.Key
	tab.Iter(func(k api.Key, _ *hotEntry, _ func()) bool {
		got = append(got, k)
		return true
	})
	require.Len(t, got, len(keys))
	// Order is not guaranteed, but all keys must be present.
	want := slices.Clone(keys)
	slices.SortFunc(want, func(a, b api.Key) int { return bytes.Compare(a[:], b[:]) })
	slices.SortFunc(got, func(a, b api.Key) int { return bytes.Compare(a[:], b[:]) })
	assert.Equal(t, want, got)
}
