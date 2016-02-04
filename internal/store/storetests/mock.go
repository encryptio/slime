package storetests

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/encryptio/slime/internal/store"
	"github.com/encryptio/slime/internal/uuid"
)

var ErrNotEnoughMockSpace = errors.New("no space left in MockStore")

type MockStore struct {
	mu         sync.Mutex
	cond       *sync.Cond
	contents   map[string]string
	size, used int64
	blocked    bool

	uuid [16]byte
}

func NewMockStore(size int64) *MockStore {
	m := &MockStore{
		contents: make(map[string]string, 128),
		uuid:     uuid.Gen4(),
		size:     size,
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func (m *MockStore) SetBlocked(blocked bool) {
	m.mu.Lock()
	m.blocked = blocked
	m.mu.Unlock()
	m.cond.Broadcast()
}

// Assumes m.mu is held.
func (m *MockStore) waitUnblocked() {
	for m.blocked {
		m.cond.Wait()
	}
}

func (m *MockStore) UUID() [16]byte {
	return m.uuid
}

func (m *MockStore) Name() string {
	return fmt.Sprintf("mock<%p>", m)
}

func (m *MockStore) Get(key string, opts store.GetOptions) ([]byte, store.Stat, error) {
	m.mu.Lock()
	m.waitUnblocked()
	defer m.mu.Unlock()

	data, ok := m.contents[key]
	if !ok {
		return nil, store.Stat{}, store.ErrNotFound
	}

	bytes := []byte(data)
	return bytes, store.Stat{
		SHA256: sha256.Sum256(bytes),
		Size:   int64(len(bytes)),
	}, nil
}

func (m *MockStore) GetPartial(key string, start, length int, opts store.GetOptions) ([]byte, store.Stat, error) {
	d, st, err := m.Get(key, opts)
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

func (m *MockStore) List(after string, limit int, cancel <-chan struct{}) ([]string, error) {
	m.mu.Lock()
	m.waitUnblocked()
	defer m.mu.Unlock()

	out := make([]string, 0, 32)
	for k := range m.contents {
		if k > after {
			out = append(out, k)
		}
	}

	sort.Strings(out)
	if limit > 0 && len(out) >= limit {
		out = out[:limit]
	}

	return out, nil
}

func (m *MockStore) FreeSpace(cancel <-chan struct{}) (int64, error) {
	m.mu.Lock()
	m.waitUnblocked()
	defer m.mu.Unlock()

	if m.size <= 0 {
		return 1, nil
	}

	return m.size - m.used, nil
}

func (m *MockStore) Stat(key string, cancel <-chan struct{}) (store.Stat, error) {
	_, st, err := m.Get(key, store.GetOptions{Cancel: cancel})
	return st, err
}

func (m *MockStore) CAS(key string, from, to store.CASV, cancel <-chan struct{}) error {
	m.mu.Lock()
	m.waitUnblocked()
	defer m.mu.Unlock()

	haveData, have := m.contents[key]
	haveSHA := sha256.Sum256([]byte(haveData))

	if !from.Any {
		if from.Present != have {
			return store.ErrCASFailure
		}
		if from.Present && haveSHA != from.SHA256 {
			return store.ErrCASFailure
		}
	}

	newUsed := m.used - int64(len(m.contents[key]))
	if to.Present {
		newUsed += int64(len(to.Data))
	}
	if m.size > 0 && newUsed > m.size {
		return ErrNotEnoughMockSpace
	}

	if !to.Present {
		delete(m.contents, key)
	} else {
		m.contents[key] = string(to.Data)
	}

	m.used = newUsed

	return nil
}

func (m *MockStore) Close() error {
	m.mu.Lock()
	m.waitUnblocked()
	m.mu.Unlock()
	return nil
}
