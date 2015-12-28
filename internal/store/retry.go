package store

import (
	"sync"
	"time"
)

// A RetryStore will repeatedly try to create a Store by calling the given
// constructor on the interval given. After the constructor succeeds (returns
// non-nil), the RetryStore will begin to pass through calls to the inner Store.
// If the inner Store returned is a SometimesStore, it will poll the inner store
// to see if it's still available. If it is not, the RetryStore will Close the
// inner store and go back to the constructor loop.
type RetryStore struct {
	construct func() Store
	interval  time.Duration

	done chan struct{}

	mu    sync.RWMutex
	inner Store
}

func NewRetryStore(construct func() Store, interval time.Duration) *RetryStore {
	rs := &RetryStore{
		interval:  interval,
		construct: construct,
		done:      make(chan struct{}),
	}
	go rs.loop()
	return rs
}

func (rs *RetryStore) loop() {
	defer func() {
		rs.mu.Lock()
		if rs.inner != nil {
			rs.inner.Close()
			rs.inner = nil
		}
		rs.mu.Unlock()
	}()

	for {
		// construct a store
		for {
			ret := rs.construct()
			if ret != nil {
				rs.mu.Lock()
				rs.inner = ret
				rs.mu.Unlock()
				break
			}

			select {
			case <-time.After(rs.interval):
			case <-rs.done:
				return
			}
		}

		// wait for it to become unavailable
		for {
			rs.mu.RLock()
			sstore, ok := rs.inner.(SometimesStore)
			rs.mu.RUnlock()

			if ok && !sstore.Available() {
				rs.mu.Lock()
				rs.inner.Close()
				rs.inner = nil
				rs.mu.Unlock()
				break
			}

			select {
			case <-time.After(rs.interval):
			case <-rs.done:
				return
			}
		}
	}
}

func (rs *RetryStore) Close() error {
	close(rs.done)
	return nil
}

func (rs *RetryStore) getInner() Store {
	rs.mu.RLock()
	ret := rs.inner
	rs.mu.RUnlock()
	return ret
}

func (rs *RetryStore) Available() bool {
	return rs.getInner() != nil
}

func (rs *RetryStore) UUID() [16]byte {
	s := rs.getInner()
	if s == nil {
		return [16]byte{}
	}
	return s.UUID()
}

func (rs *RetryStore) Name() string {
	s := rs.getInner()
	if s == nil {
		return ""
	}
	return s.Name()
}

func (rs *RetryStore) Get(key string, cancel <-chan struct{}) ([]byte, Stat, error) {
	s := rs.getInner()
	if s == nil {
		return nil, Stat{}, ErrUnavailable
	}
	return s.Get(key, cancel)
}

func (rs *RetryStore) List(after string, limit int, cancel <-chan struct{}) ([]string, error) {
	s := rs.getInner()
	if s == nil {
		return nil, ErrUnavailable
	}
	return s.List(after, limit, cancel)
}

func (rs *RetryStore) FreeSpace(cancel <-chan struct{}) (int64, error) {
	s := rs.getInner()
	if s == nil {
		return 0, ErrUnavailable
	}
	return s.FreeSpace(cancel)
}

func (rs *RetryStore) Stat(key string, cancel <-chan struct{}) (Stat, error) {
	s := rs.getInner()
	if s == nil {
		return Stat{}, ErrUnavailable
	}
	return s.Stat(key, cancel)
}

func (rs *RetryStore) CAS(key string, from, to CASV, cancel <-chan struct{}) error {
	s := rs.getInner()
	if s == nil {
		return ErrUnavailable
	}
	return s.CAS(key, from, to, cancel)
}
