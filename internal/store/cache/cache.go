package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/encryptio/slime/internal/store"
)

const (
	// estimate of the number of bytes used by a cacheEntry and its slot in the
	// Cache.entries map, excluding the entry's Data backing array.
	perEntryMemoryFudge = 128
)

type Cache struct {
	size  int
	inner store.Store

	mu      sync.Mutex
	used    int
	entries map[string]*cacheEntry
}

type cacheEntry struct {
	Key      string
	NoVerify bool

	// Writes to these happen in getWorker, and all are set to their final value
	// before Ready is closed.
	Ready chan struct{}
	Error error
	Stat  store.Stat
	Data  []byte

	// Writes to these are protected by (*Cache).mu and happen in
	// (*Cache).Get(). When waiters == 0, Cancel may be closed.
	waiters  int
	Cancel   chan struct{}
	LastUsed time.Time
}

func New(size int, inner store.Store) *Cache {
	if size < 0 {
		panic("size must be non-negative")
	}
	return &Cache{
		inner:   inner,
		size:    size,
		entries: make(map[string]*cacheEntry, 128),
	}
}

func (c *Cache) UUID() [16]byte {
	return c.inner.UUID()
}

func (c *Cache) Name() string {
	return c.inner.Name()
}

func (c *Cache) Get(key string, opts store.GetOptions) ([]byte, store.Stat, error) {
	d, st, err := c.getUncopied(key, opts.Cancel, opts.NoVerify)
	if err != nil {
		return nil, store.Stat{}, err
	}
	d2 := make([]byte, len(d))
	copy(d2, d)
	return d2, st, err
}

func (c *Cache) GetPartial(key string, start, length int, opts store.GetOptions) ([]byte, store.Stat, error) {
	d, st, err := c.getUncopied(key, opts.Cancel, opts.NoVerify)
	if err != nil {
		return nil, store.Stat{}, err
	}
	if start < 0 {
		start = 0
	}
	if length < 0 || start+length > len(d) {
		length = len(d) - start
	}
	if length <= 0 {
		return []byte{}, st, nil
	}
	d2 := make([]byte, length)
	if copy(d2, d[start:]) != length {
		panic("never happens")
	}
	return d2, st, nil
}

func (c *Cache) getUncopied(
	key string,
	cancel <-chan struct{},
	noverify bool) ([]byte, store.Stat, error) {

	c.mu.Lock()
	ce, ok := c.entries[key]
	if ok && ce.NoVerify && !noverify {
		// The existing cache entry was called with NoVerify, but this call
		// wants verification. Delete the entry, we'll replace it below.

		// Note that attaching a NoVerify request to a verified cacheEntry is
		// okay.

		ok = false
		c.removeEntryLocked(key)
	}
	if !ok {
		// No cache entry; make one and spawn a getWorker.
		ce = &cacheEntry{
			Key:      key,
			NoVerify: noverify,
			Ready:    make(chan struct{}),
			Cancel:   make(chan struct{}),
		}
		c.setEntryLocked(key, ce)
		go c.getWorker(ce)
	}
	ce.waiters++
	ce.LastUsed = time.Now()
	c.mu.Unlock()

	// didWait allows us to skip the Stat call if we waited for a Get to finish
	didWait := true
	select {
	case <-ce.Ready:
		didWait = false
	default:
	}

	select {
	case <-ce.Ready:
	case <-cancel:
		c.mu.Lock()
		ce.waiters--
		if ce.waiters == 0 {
			// We're the last; cancel the inner.Get
			close(ce.Cancel)
			if c.entries[key] == ce {
				c.removeEntryLocked(key)
			}
		}
		c.mu.Unlock()
		return nil, store.Stat{}, store.ErrCancelled
	}

	c.mu.Lock()
	ce.waiters--
	// NB: even if ce.waiters == 0 here, getWorker has already returned, so
	// closing ce.Cancel will do nothing.
	c.mu.Unlock()

	if ce.Error != nil {
		// In this case, inner.Get returned an error. getWorker has already
		// removed the entry from the cache; further Gets on the key will start
		// new getWorkers.
		return nil, store.Stat{}, ce.Error
	}

	if !didWait {
		// This is a cached entry, not the result of a real Get. We need to Stat
		// the key again to make sure it's still the same.

		st, err := c.inner.Stat(key, cancel)
		if err != nil {
			return nil, store.Stat{}, err
		}

		if st != ce.Stat {
			// Stat did NOT match. Remove it from the cache and try again.
			c.mu.Lock()
			if c.entries[key] == ce {
				c.removeEntryLocked(key)
			}
			c.mu.Unlock()
			return c.Get(key, store.GetOptions{
				Cancel:   cancel,
				NoVerify: noverify,
			})
		}
	}

	// Get successful and complete.

	return ce.Data, ce.Stat, nil
}

func (c *Cache) getWorker(ce *cacheEntry) {
	defer close(ce.Ready)

	data, stat, err := c.inner.Get(ce.Key, store.GetOptions{
		Cancel:   ce.Cancel,
		NoVerify: ce.NoVerify,
	})

	c.mu.Lock()
	ce.Data, ce.Stat, ce.Error = data, stat, err
	if c.entries[ce.Key] == ce {
		if ce.Error != nil {
			// Don't cache the error for future gets
			c.removeEntryLocked(ce.Key)
		} else {
			// NB: perEntryMemoryFudge already applies
			c.used += len(ce.Data)
			c.garbageCollectLocked()
		}
	}
	c.mu.Unlock()
}

func (c *Cache) garbageCollectLocked() {
	for c.used > c.size {
		// TODO: optimize with auxilary structures, like a container/heap

		var oldestKey string
		var oldest *cacheEntry
		for k, v := range c.entries {
			select {
			case <-v.Ready:
				if oldest == nil || v.LastUsed.Before(oldest.LastUsed) {
					oldestKey = k
					oldest = v
				}
			default:
				// skip
			}
		}

		if oldest == nil {
			// Nothing to collect. Possible with tiny cache sizes.
			break
		}

		c.removeEntryLocked(oldestKey)
	}
}

func (c *Cache) List(after string, limit int, cancel <-chan struct{}) ([]string, error) {
	return c.inner.List(after, limit, cancel)
}

func (c *Cache) FreeSpace(cancel <-chan struct{}) (int64, error) {
	return c.inner.FreeSpace(cancel)
}

func (c *Cache) Stat(key string, cancel <-chan struct{}) (store.Stat, error) {
	st, err := c.inner.Stat(key, cancel)
	if err != nil {
		return st, err
	}

	c.mu.Lock()
	if ce, ok := c.entries[key]; ok {
		select {
		case <-ce.Ready:
			if st != ce.Stat {
				c.removeEntryLocked(key)
			}
		default:
		}
	}
	c.mu.Unlock()

	return st, err
}

func (c *Cache) CAS(key string, from, to store.CASV, cancel <-chan struct{}) error {
	err := c.inner.CAS(key, from, to, cancel)
	if err == nil {
		c.mu.Lock()
		if to.Present {
			ready := make(chan struct{})
			close(ready)

			data := make([]byte, len(to.Data))
			copy(data, to.Data)

			ce := &cacheEntry{
				Key:      key,
				NoVerify: false,
				Ready:    ready,
				Stat: store.Stat{
					SHA256: to.SHA256,
					Size:   int64(len(data)),
				},
				Data:     data,
				LastUsed: time.Now(),
			}

			c.setEntryLocked(key, ce)
		} else {
			if _, ok := c.entries[key]; ok {
				c.removeEntryLocked(key)
			}
		}
		c.mu.Unlock()
	}
	return err
}

func (c *Cache) Clear() {
	c.mu.Lock()
	for key := range c.entries {
		c.removeEntryLocked(key)
	}
	c.mu.Unlock()
	c.assertUsedIsCorrect()
}

func (c *Cache) removeEntryLocked(key string) {
	if ce, ok := c.entries[key]; ok {
		delete(c.entries, key)
		c.used -= len(ce.Data) + perEntryMemoryFudge
	}
}

func (c *Cache) setEntryLocked(key string, ce *cacheEntry) {
	if ce.Key != key {
		panic("ce.Key != key")
	}

	if _, ok := c.entries[key]; ok {
		c.removeEntryLocked(key)
	}

	c.entries[key] = ce
	c.used += len(ce.Data) + perEntryMemoryFudge

	c.garbageCollectLocked()
}

func (c *Cache) assertUsedIsCorrect() {
	c.mu.Lock()
	used := 0
	for _, ce := range c.entries {
		used += len(ce.Data) + perEntryMemoryFudge
	}
	if used != c.used {
		panic(fmt.Sprintf("c.used = %v but wanted %v", c.used, used))
	}
	c.mu.Unlock()
}

func (c *Cache) Close() error {
	return c.inner.Close()
}
