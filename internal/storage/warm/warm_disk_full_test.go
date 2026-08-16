package warm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

// TestDiskOverBudget_MaxDiskBytes sets MaxDiskBytes to a small value,
// writes past it, and verifies DiskOverBudget returns true.
func TestDiskOverBudget_MaxDiskBytes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:          dir,
		MaxBytes:     100 << 20,
		SegMax:       1 << 20,
		MaxDiskBytes: 10, // 10 bytes — any single record exceeds this
	})
	require.NoError(t, err, "NewStore")
	defer func() { _ = s.Close() }()

	require.False(t, s.DiskOverBudget(), "empty store should not be over budget")

	_, _, err = s.Put(testkey.Key(1), []byte("payload"))
	require.NoError(t, err, "put")

	assert.True(t, s.DiskOverBudget(), "store should be over budget after write")
}

// TestDiskOverBudget_MinFreeDisk sets MinFreeDisk to a value larger
// than the temp dir's free space and verifies DiskOverBudget returns
// true.
func TestDiskOverBudget_MinFreeDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:         dir,
		MaxBytes:    100 << 20,
		SegMax:      1 << 20,
		MinFreeDisk: 1 << 50, // 1 PiB — always larger than real free space
	})
	require.NoError(t, err, "NewStore")
	defer func() { _ = s.Close() }()

	assert.True(t, s.DiskOverBudget(), "store should be over budget with unreachable MinFreeDisk")
}

// TestDiskOverBudget_PutStillSucceeds verifies that Put still succeeds
// when the disk is over budget. Put gates on stats.bytes (logical
// budget), not diskBytes (physical usage). This pins the existing
// behavior: disk-full triggers compaction, not write rejection.
func TestDiskOverBudget_PutStillSucceeds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:          dir,
		MaxBytes:     100 << 20,
		SegMax:       1 << 20,
		MaxDiskBytes: 10,
	})
	require.NoError(t, err, "NewStore")
	defer func() { _ = s.Close() }()

	// Write past disk budget.
	for i := range 5 {
		_, _, err := s.Put(testkey.Key(uint64(i)), []byte("data"))
		require.NoErrorf(t, err, "put %d should succeed despite disk over budget", i)
	}

	assert.True(t, s.DiskOverBudget(), "store should be over disk budget")

	// Verify data is still readable.
	got, err := s.Get(testkey.Key(0))
	require.NoError(t, err, "get")
	require.Equal(t, "data", string(got), "data should be intact")
}

// TestDiskOverBudget_NotOverBudget verifies DiskOverBudget returns
// false when the store is within budget.
func TestDiskOverBudget_NotOverBudget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:          dir,
		MaxBytes:     100 << 20,
		SegMax:       1 << 20,
		MaxDiskBytes: 1 << 30, // 1 GiB — plenty of room
		MinFreeDisk:  0,       // disabled
	})
	require.NoError(t, err, "NewStore")
	defer func() { _ = s.Close() }()

	_, _, err = s.Put(testkey.Key(1), []byte("small"))
	require.NoError(t, err, "put")

	assert.False(t, s.DiskOverBudget(), "store should not be over budget")
}
