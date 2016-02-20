// Package multi provides a store.Store which redundantly stores information.
//
// It uses Reed-Solomon erasure coding for efficient storage, at the cost of
// having to refer to many inner stores to read and write data.
package multi

import (
	"sync"

	"gopkg.in/tomb.v2"

	"github.com/encryptio/kvl"
)

type Multi struct {
	db     kvl.DB
	finder *Finder
	uuid   [16]byte

	freeMapChannel chan map[[16]byte]int64

	tomb tomb.Tomb

	mu     sync.Mutex
	config multiConfig
}

func NewMulti(db kvl.DB, finder *Finder, scrubbers int) (*Multi, error) {
	m := &Multi{
		db:             db,
		finder:         finder,
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

	m.tomb.Go(func() error {
		m.tomb.Go(m.loadConfigLoop)

		for i := 0; i < scrubbers; i++ {
			m.tomb.Go(m.scrubFilesLoop)
			m.tomb.Go(m.scrubLocationsLoop)
			m.tomb.Go(m.rebalanceLoop)
		}

		if scrubbers > 0 {
			m.tomb.Go(m.scrubWALLoop)
		}

		return nil
	})

	return m, nil
}

func (m *Multi) Close() error {
	m.tomb.Kill(nil)
	return m.tomb.Wait()
}

func (m *Multi) scrubAll() {
	m.scrubFilesAll()
	m.scrubLocationsAll()
}
