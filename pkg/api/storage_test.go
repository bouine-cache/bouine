package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObject_Fresh(t *testing.T) {
	t.Parallel()
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	obj := &Object{
		StoredAt: now,
		TTL:      10 * time.Second,
	}
	assert.True(t, obj.Fresh(now))
	assert.True(t, obj.Fresh(now.Add(9*time.Second)))
	assert.False(t, obj.Fresh(now.Add(10*time.Second)))
	assert.False(t, obj.Fresh(now.Add(11*time.Second)))
}

func TestObject_Fresh_ZeroTTL(t *testing.T) {
	t.Parallel()
	now := time.Now()
	obj := &Object{
		StoredAt: now,
		TTL:      0,
	}
	assert.False(t, obj.Fresh(now))
}

func TestObject_StaleForSWR(t *testing.T) {
	t.Parallel()
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	obj := &Object{
		StoredAt:             now,
		TTL:                  10 * time.Second,
		StaleWhileRevalidate: 5 * time.Second,
	}
	assert.False(t, obj.StaleForSWR(now))
	assert.False(t, obj.StaleForSWR(now.Add(9*time.Second)))
	assert.True(t, obj.StaleForSWR(now.Add(11*time.Second)))
	assert.True(t, obj.StaleForSWR(now.Add(14*time.Second)))
	assert.False(t, obj.StaleForSWR(now.Add(16*time.Second)))
}

func TestObject_StaleForSWR_NoSWR(t *testing.T) {
	t.Parallel()
	now := time.Now()
	obj := &Object{
		StoredAt: now,
		TTL:      10 * time.Second,
	}
	assert.False(t, obj.StaleForSWR(now.Add(15*time.Second)))
}

func TestObject_StaleForSIE(t *testing.T) {
	t.Parallel()
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	obj := &Object{
		StoredAt:     now,
		TTL:          10 * time.Second,
		StaleIfError: 5 * time.Second,
	}
	assert.False(t, obj.StaleForSIE(now))
	assert.False(t, obj.StaleForSIE(now.Add(9*time.Second)))
	assert.True(t, obj.StaleForSIE(now.Add(11*time.Second)))
	assert.True(t, obj.StaleForSIE(now.Add(14*time.Second)))
	assert.False(t, obj.StaleForSIE(now.Add(16*time.Second)))
}

func TestObject_StaleForSIE_NoSIE(t *testing.T) {
	t.Parallel()
	now := time.Now()
	obj := &Object{
		StoredAt: now,
		TTL:      10 * time.Second,
	}
	assert.False(t, obj.StaleForSIE(now.Add(15*time.Second)))
}

func TestObject_StaleButServable(t *testing.T) {
	t.Parallel()
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	obj := &Object{
		StoredAt:             now,
		TTL:                  10 * time.Second,
		StaleWhileRevalidate: 5 * time.Second,
	}
	assert.False(t, obj.StaleButServable(now))
	assert.True(t, obj.StaleButServable(now.Add(11*time.Second)))
}

func TestObject_StaleButServable_SIEOnly(t *testing.T) {
	t.Parallel()
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	obj := &Object{
		StoredAt:     now,
		TTL:          10 * time.Second,
		StaleIfError: 5 * time.Second,
	}
	assert.True(t, obj.StaleButServable(now.Add(11*time.Second)))
}

func TestObject_LoadSerializedHead_Nil(t *testing.T) {
	t.Parallel()
	obj := &Object{}
	assert.Nil(t, obj.LoadSerializedHead())
}

func TestObject_StoreAndLoadSerializedHead(t *testing.T) {
	t.Parallel()
	obj := &Object{}
	head := []byte("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n")
	obj.StoreSerializedHead(head)
	require.Equal(t, head, obj.LoadSerializedHead())
}

func TestObject_CloneForReturn(t *testing.T) {
	t.Parallel()
	obj := &Object{
		Key:          Key{1, 2, 3},
		VaryKey:      "vary",
		StatusCode:   200,
		Body:         []byte("original"),
		BodySize:     8,
		StoredAt:     time.Now(),
		TTL:          10 * time.Second,
		ETag:         "abc",
		Hits:         5,
		CacheControl: "max-age=10",
	}
	clone := obj.CloneForReturn([]byte("cloned"))
	require.NotSame(t, obj, clone)
	assert.Equal(t, obj.Key, clone.Key)
	assert.Equal(t, obj.VaryKey, clone.VaryKey)
	assert.Equal(t, obj.StatusCode, clone.StatusCode)
	assert.Equal(t, []byte("cloned"), clone.Body)
	assert.Equal(t, obj.BodySize, clone.BodySize)
	assert.Equal(t, obj.TTL, clone.TTL)
	assert.Equal(t, obj.ETag, clone.ETag)
	assert.Equal(t, obj.Hits, clone.Hits)
	assert.Equal(t, obj.CacheControl, clone.CacheControl)
}

func TestObject_CloneForReturn_PreservesSerializedHead(t *testing.T) {
	t.Parallel()
	obj := &Object{
		StoredAt: time.Now(),
		TTL:      10 * time.Second,
	}
	head := []byte("status line\r\n")
	obj.StoreSerializedHead(head)
	clone := obj.CloneForReturn(nil)
	require.Equal(t, head, clone.LoadSerializedHead())
}

func TestObject_CloneForRefresh(t *testing.T) {
	t.Parallel()
	obj := &Object{
		Key:          Key{1, 2, 3},
		VaryKey:      "vary",
		StatusCode:   200,
		Body:         []byte("body"),
		BodySize:     4,
		StoredAt:     time.Now(),
		TTL:          10 * time.Second,
		ETag:         "abc",
		Hits:         5,
		CacheControl: "max-age=10",
	}
	clone := obj.CloneForRefresh()
	require.NotSame(t, obj, clone)
	assert.Equal(t, obj.Key, clone.Key)
	assert.Equal(t, obj.VaryKey, clone.VaryKey)
	assert.Equal(t, obj.StatusCode, clone.StatusCode)
	assert.Equal(t, obj.Body, clone.Body)
	assert.Equal(t, obj.BodySize, clone.BodySize)
	assert.Equal(t, obj.TTL, clone.TTL)
	assert.Equal(t, obj.ETag, clone.ETag)
	assert.Equal(t, obj.Hits, clone.Hits)
	assert.Equal(t, obj.CacheControl, clone.CacheControl)
}

func TestObject_CloneForRefresh_NoSerializedHead(t *testing.T) {
	t.Parallel()
	obj := &Object{
		StoredAt: time.Now(),
		TTL:      10 * time.Second,
	}
	obj.StoreSerializedHead([]byte("head"))
	clone := obj.CloneForRefresh()
	assert.Nil(t, clone.LoadSerializedHead())
}

func TestObject_SerializedHead_ThreadSafe(t *testing.T) {
	t.Parallel()
	obj := &Object{}
	done := make(chan struct{})
	for i := 0; i < 100; i++ {
		go func() {
			obj.StoreSerializedHead([]byte("head"))
			_ = obj.LoadSerializedHead()
			done <- struct{}{}
		}()
	}
	for i := 0; i < 100; i++ {
		<-done
	}
}
