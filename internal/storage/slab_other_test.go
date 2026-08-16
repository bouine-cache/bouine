//go:build !linux

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlabAllocator_NoOp(t *testing.T) {
	t.Parallel()
	s, err := NewSlabAllocator()
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Nil(t, s.Alloc(1024))
	s.Free(nil)
	s.FreeBatch(nil)
	allocs, frees, fallback := s.Stats()
	assert.Equal(t, int64(0), allocs)
	assert.Equal(t, int64(0), frees)
	assert.Equal(t, int64(0), fallback)
	assert.NoError(t, s.Close())
}
