package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestEntry_HasSize(t *testing.T) {
	t.Parallel()
	key := testkey.Key(1)
	assert.False(t, PutEntry(key, 1, 0).HasSize())
	assert.True(t, PutEntryWithSize(key, 1, 0, 100).HasSize())
	assert.False(t, DeleteEntry(key).HasSize())
}

func TestEntry_IsPut(t *testing.T) {
	t.Parallel()
	key := testkey.Key(1)
	assert.True(t, PutEntry(key, 1, 0).IsPut())
	assert.True(t, PutEntryWithSize(key, 1, 0, 100).IsPut())
	assert.False(t, DeleteEntry(key).IsPut())
}

func TestEntry_IsDelete(t *testing.T) {
	t.Parallel()
	key := testkey.Key(1)
	assert.False(t, PutEntry(key, 1, 0).IsDelete())
	assert.False(t, PutEntryWithSize(key, 1, 0, 100).IsDelete())
	assert.True(t, DeleteEntry(key).IsDelete())
}

func TestPutEntryWithSize_Fields(t *testing.T) {
	t.Parallel()
	key := testkey.Key(42)
	e := PutEntryWithSize(key, 5, 1000, 512)
	assert.Equal(t, key, e.Key)
	assert.Equal(t, int32(5), e.SegID)
	assert.Equal(t, int64(1000), e.Offset)
	assert.Equal(t, int64(512), e.Size)
	assert.True(t, e.HasSize())
}

func TestPutEntry_Fields(t *testing.T) {
	t.Parallel()
	key := testkey.Key(7)
	e := PutEntry(key, 3, 200)
	assert.Equal(t, key, e.Key)
	assert.Equal(t, int32(3), e.SegID)
	assert.Equal(t, int64(200), e.Offset)
	assert.Equal(t, int64(0), e.Size)
	assert.False(t, e.HasSize())
}

func TestDeleteEntry_Fields(t *testing.T) {
	t.Parallel()
	key := testkey.Key(99)
	e := DeleteEntry(key)
	assert.Equal(t, key, e.Key)
	assert.True(t, e.IsDelete())
	assert.False(t, e.IsPut())
}

func TestEntry_OpTypes(t *testing.T) {
	t.Parallel()
	key := api.Key{}
	assert.NotEqual(t, PutEntry(key, 0, 0).Op, DeleteEntry(key).Op)
	assert.NotEqual(t, PutEntry(key, 0, 0).Op, PutEntryWithSize(key, 0, 0, 0).Op)
}
