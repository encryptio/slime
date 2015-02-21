// package multi provides a store.Store which redundantly stores information.
//
// It uses Reed-Solomon erasure coding for efficient storage, at the cost of
// having to refer to many inner stores to read and write data.
package multi

import (
	"sync"

	"git.encryptio.com/kvl"
)

type Multi struct {
	db     kvl.DB
	finder *Finder
	uuid   [16]byte

	freeMapChannel chan map[[16]byte]int64

	stop chan struct{}

	mu     sync.Mutex
	config multiConfig
}

func NewMulti(db kvl.DB, finder *Finder) (*Multi, error) {
	m := &Multi{
		db:             db,
		finder:         finder,
		stop:           make(chan struct{}),
		freeMapChannel: make(chan map[[16]byte]int64),
	}

	err := m.loadUUID()
	if err != nil {
		return nil, err
	}

	err = m.loadConfig()
	if err != nil {
		return nil, err
	}

	go m.loadConfigLoop(loadConfigInterval)
	go m.scrubLoop()
	go m.rebalanceLoop()
	go m.freeMapLoop()

	return m, nil
}

func (m *Multi) Stop() {
	m.mu.Lock()

	select {
	case <-m.stop:
	default:
		close(m.stop)
	}

	m.mu.Unlock()
}
