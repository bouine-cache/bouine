package warm

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestStore_SetIndexWithSize_LookupWithSize(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	key := testkey.Key(1)
	s.SetIndexWithSize(key, 2, 1000, 512)
	segID, offset, size, ok := s.LookupWithSize(key)
	assert.True(t, ok)
	assert.Equal(t, 2, segID)
	assert.Equal(t, int64(1000), offset)
	assert.Equal(t, int64(512), size)
}

func TestStore_LookupWithSize_NotFound(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	_, _, _, ok := s.LookupWithSize(testkey.Key(999))
	assert.False(t, ok)
}

func TestStore_Lookup_NotFound(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	_, _, ok := s.Lookup(testkey.Key(999))
	assert.False(t, ok)
}

func TestStore_NeedsCompaction_Empty(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	assert.False(t, s.NeedsCompaction())
}

func TestStore_IndexLen(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	assert.Equal(t, 0, s.IndexLen())
	s.SetIndex(testkey.Key(1), 0, 0)
	assert.Equal(t, 1, s.IndexLen())
	s.SetIndex(testkey.Key(2), 0, 100)
	assert.Equal(t, 2, s.IndexLen())
}

func TestStore_DelIndex(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	key := testkey.Key(1)
	s.SetIndex(key, 0, 0)
	assert.Equal(t, 1, s.IndexLen())
	s.DelIndex(key)
	assert.Equal(t, 0, s.IndexLen())
	_, _, ok := s.Lookup(key)
	assert.False(t, ok)
}

func TestStore_SetIndex_UpdatesExisting(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	key := testkey.Key(1)
	s.SetIndex(key, 1, 100)
	s.SetIndex(key, 2, 200) // update
	segID, offset, ok := s.Lookup(key)
	assert.True(t, ok)
	assert.Equal(t, 2, segID)
	assert.Equal(t, int64(200), offset)
	assert.Equal(t, 1, s.IndexLen()) // should not create duplicate
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(Config{
		Dir:      t.TempDir(),
		MaxBytes: 100 << 20,
		SegMax:   1 << 20,
	})
	if err != nil {
		t.Fatalf("failed to open warm store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// Ensure api import is used.
var _ = api.Key{}
