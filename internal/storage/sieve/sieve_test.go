package sieve

import "testing"

func TestSieve_InsertAndAccess(t *testing.T) {
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	e1, ins := l.Access(1, func(k uint64) *Entry[uint64] { return m[k] })
	if !ins {
		t.Fatal("expected insert")
	}
	m[1] = e1

	e2, ins := l.Access(2, func(k uint64) *Entry[uint64] { return m[k] })
	if !ins {
		t.Fatal("expected insert")
	}
	m[2] = e2

	if l.Len() != 2 {
		t.Fatalf("len = %d, want 2", l.Len())
	}

	// Re-access key 1 — should not insert, should mark visited.
	_, ins = l.Access(1, func(k uint64) *Entry[uint64] { return m[k] })
	if ins {
		t.Fatal("expected hit, not insert")
	}
	if l.Len() != 2 {
		t.Fatalf("len = %d after re-access", l.Len())
	}
}

func TestSieve_EvictsLeastRecent(t *testing.T) {
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	// Insert 1, 2, 3.
	for _, k := range []uint64{1, 2, 3} {
		e, _ := l.Access(k, func(k uint64) *Entry[uint64] { return m[k] })
		m[k] = e
	}

	// Access 1 and 3 so they get visited=true.
	l.Access(1, func(k uint64) *Entry[uint64] { return m[k] })
	l.Access(3, func(k uint64) *Entry[uint64] { return m[k] })

	// Evict — should evict 2 (not visited, at tail after 1 was visited).
	key, ok := l.Evict()
	if !ok {
		t.Fatal("expected eviction")
	}
	delete(m, key)

	if key != 2 {
		t.Fatalf("evicted %d, want 2", key)
	}
	if l.Len() != 2 {
		t.Fatalf("len = %d, want 2", l.Len())
	}
}

func TestSieve_EvictAll(t *testing.T) {
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	for _, k := range []uint64{10, 20, 30} {
		e, _ := l.Access(k, func(k uint64) *Entry[uint64] { return m[k] })
		m[k] = e
	}

	evicted := map[uint64]bool{}
	for range 3 {
		k, ok := l.Evict()
		if !ok {
			t.Fatal("expected eviction")
		}
		evicted[k] = true
		delete(m, k)
	}

	if len(evicted) != 3 {
		t.Fatalf("evicted %d keys, want 3", len(evicted))
	}
	if l.Len() != 0 {
		t.Fatalf("len = %d, want 0", l.Len())
	}

	_, ok := l.Evict()
	if ok {
		t.Fatal("evict on empty should return false")
	}
}

func TestSieve_Remove(t *testing.T) {
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	for _, k := range []uint64{1, 2, 3} {
		e, _ := l.Access(k, func(k uint64) *Entry[uint64] { return m[k] })
		m[k] = e
	}

	l.Remove(m[2])
	delete(m, 2)

	if l.Len() != 2 {
		t.Fatalf("len = %d, want 2", l.Len())
	}

	// Evict remaining — should get 1 and 3 in some order.
	var keys []uint64
	for range 2 {
		k, ok := l.Evict()
		if !ok {
			t.Fatal("expected eviction")
		}
		keys = append(keys, k)
	}
	if len(keys) != 2 {
		t.Fatalf("got %d keys", len(keys))
	}
}

func TestSieve_SecondChance(t *testing.T) {
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	// Insert A, B.
	for _, k := range []uint64{1, 2} {
		e, _ := l.Access(k, func(k uint64) *Entry[uint64] { return m[k] })
		m[k] = e
	}

	// Visit both.
	l.Access(1, func(k uint64) *Entry[uint64] { return m[k] })
	l.Access(2, func(k uint64) *Entry[uint64] { return m[k] })

	// First evict: both visited, so both get second chance; then one
	// is evicted (the one the hand lands on after clearing visited).
	k1, ok := l.Evict()
	if !ok {
		t.Fatal("expected eviction")
	}
	delete(m, k1)
	if l.Len() != 1 {
		t.Fatalf("len = %d", l.Len())
	}
}
