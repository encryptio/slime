package cache

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/encryptio/slime/internal/store"
	"github.com/encryptio/slime/internal/store/storetests"
)

var ErrErrorStore = errors.New("ErrorStore error")

type CountingStore struct {
	*storetests.MockStore
	gets, stats int32
}

func (c *CountingStore) Get(key string, opts store.GetOptions) ([]byte, store.Stat, error) {
	atomic.AddInt32(&c.gets, 1)
	return c.MockStore.Get(key, opts)
}

func (c *CountingStore) Stat(key string, cancel <-chan struct{}) (store.Stat, error) {
	atomic.AddInt32(&c.stats, 1)
	return c.MockStore.Stat(key, cancel)
}

type ErrorStore struct {
	*storetests.MockStore
	isErroring bool
}

func (e *ErrorStore) Get(key string, opts store.GetOptions) ([]byte, store.Stat, error) {
	if e.isErroring {
		return nil, store.Stat{}, ErrErrorStore
	}
	return e.MockStore.Get(key, opts)
}

func TestCacheGeneric(t *testing.T) {
	for size := 1; size <= 1024*128; size *= 4 {
		inner := storetests.NewMockStore(0)
		cache := New(size, inner)
		storetests.TestStore(t, cache)
		cache.assertUsedIsCorrect()
	}
}

func TestCacheCoalescesGets(t *testing.T) {
	inner := &CountingStore{MockStore: storetests.NewMockStore(0)}
	cache := New(1024, inner)

	inner.SetBlocked(true)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			cache.Get("key", store.GetOptions{})
			wg.Done()
		}()
	}

	// wait until Gets are in flight (unreliable)
	time.Sleep(20 * time.Millisecond)

	inner.SetBlocked(false)
	wg.Wait()

	if inner.gets != 1 {
		t.Errorf("wanted 1 inner Get, got %v", inner.gets)
	}

	if inner.stats != 0 {
		t.Errorf("wanted 0 inner Stats, got %v", inner.stats)
	}
}

func TestCacheEmptyHasCorrectUsed(t *testing.T) {
	cache := New(1024, storetests.NewMockStore(0))
	cache.assertUsedIsCorrect()
}

func TestCacheCachesGets(t *testing.T) {
	inner := &CountingStore{MockStore: storetests.NewMockStore(0)}
	cache := New(1024, inner)

	storetests.ShouldCAS(t, inner, "asdf", store.AnyV, store.DataV([]byte("hello")))
	cache.assertUsedIsCorrect()

	for i := 0; i < 10; i++ {
		storetests.ShouldGet(t, cache, "asdf", []byte("hello"))
		cache.assertUsedIsCorrect()
	}

	if inner.gets != 1 {
		t.Errorf("wanted 1 inner Get, got %v", inner.gets)
	}

	if inner.stats != 9 {
		t.Errorf("wanted 9 inner Stats, got %v", inner.stats)
	}

	cache.assertUsedIsCorrect()
}

func TestCacheDoesntCacheErrors(t *testing.T) {
	inner := &ErrorStore{MockStore: storetests.NewMockStore(0)}
	cache := New(1024, inner)

	storetests.ShouldCAS(t, cache, "asdf", store.AnyV, store.DataV([]byte("hello")))
	cache.Clear()

	inner.isErroring = true
	for i := 0; i < 10; i++ {
		storetests.ShouldGetError(t, cache, "asdf", ErrErrorStore)
	}

	cache.assertUsedIsCorrect()

	inner.isErroring = false
	storetests.ShouldGet(t, cache, "asdf", []byte("hello"))

	cache.assertUsedIsCorrect()
}

func TestCacheUpdatesOnKnownChanges(t *testing.T) {
	inner := &CountingStore{MockStore: storetests.NewMockStore(0)}
	cache := New(1024, inner)

	storetests.ShouldCAS(t, cache, "asdf", store.AnyV, store.DataV([]byte("hello")))
	cache.Clear()
	storetests.ShouldGet(t, cache, "asdf", []byte("hello"))
	storetests.ShouldCAS(t, cache, "asdf", store.AnyV, store.DataV([]byte("world")))
	storetests.ShouldGet(t, cache, "asdf", []byte("world"))

	if inner.gets != 1 {
		t.Errorf("wanted 1 inner gets, got %v", inner.gets)
	}

	if inner.stats != 1 {
		t.Errorf("wanted 1 inner Stats, got %v", inner.stats)
	}

	cache.assertUsedIsCorrect()
}

func TestCacheUpdatesOnUnknownChanges(t *testing.T) {
	inner := &CountingStore{MockStore: storetests.NewMockStore(0)}
	cache := New(1024, inner)

	storetests.ShouldCAS(t, cache, "asdf", store.AnyV, store.DataV([]byte("hello")))
	cache.Clear()
	storetests.ShouldGet(t, cache, "asdf", []byte("hello"))
	storetests.ShouldCAS(t, inner, "asdf", store.AnyV, store.DataV([]byte("world")))
	storetests.ShouldGet(t, cache, "asdf", []byte("world"))

	if inner.gets != 2 {
		t.Errorf("wanted 2 inner gets, got %v", inner.gets)
	}

	if inner.stats != 1 {
		t.Errorf("wanted 1 inner Stats, got %v", inner.stats)
	}

	cache.assertUsedIsCorrect()
}

func TestCacheUpdatesOnStats(t *testing.T) {
	inner := &CountingStore{MockStore: storetests.NewMockStore(0)}
	cache := New(1024, inner)

	storetests.ShouldCAS(t, cache, "asdf", store.AnyV, store.DataV([]byte("hello")))
	storetests.ShouldGet(t, cache, "asdf", []byte("hello"))
	newData := store.DataV([]byte("world"))
	storetests.ShouldCAS(t, inner, "asdf", store.AnyV, newData)
	storetests.ShouldStatNoTime(t, cache, "asdf", store.Stat{Size: 5, SHA256: newData.SHA256})
	storetests.ShouldGet(t, cache, "asdf", []byte("world"))

	if inner.gets != 1 {
		t.Errorf("wanted 1 inner gets, got %v", inner.gets)
	}

	if inner.stats != 2 {
		t.Errorf("wanted 2 inner Stats, got %v", inner.stats)
	}

	cache.assertUsedIsCorrect()
}

func TestCacheGCWorks(t *testing.T) {
	inner := &CountingStore{MockStore: storetests.NewMockStore(0)}
	cache := New(1024, inner)

	for i := 0; i < 1024; i++ {
		key := strconv.FormatInt(int64(i), 10)
		storetests.ShouldCAS(t, cache, key, store.AnyV, store.DataV([]byte(key)))
	}

	cache.assertUsedIsCorrect()
	t.Logf("cache.used after writes is %v", cache.used)

	for i := 0; i < 1024; i++ {
		key := strconv.FormatInt(int64(i), 10)
		storetests.ShouldGet(t, cache, key, []byte(key))
	}

	cache.assertUsedIsCorrect()
	t.Logf("cache.used after first reads is %v", cache.used)
	getsBefore := inner.gets

	for i := 0; i < 1024; i++ {
		key := strconv.FormatInt(int64(i), 10)
		storetests.ShouldGet(t, cache, key, []byte(key))
	}

	cache.assertUsedIsCorrect()
	t.Logf("cache.used after second reads is %v", cache.used)
	getsAfter := inner.gets

	if getsAfter != getsBefore+1024 {
		t.Errorf("wanted %v gets after, got %v", getsBefore+1024, getsAfter)
	}
}
