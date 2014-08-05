package multi

import (
	"git.encryptio.com/slime/lib/store"
	"sync"
	"errors"
)

var ErrNoTargets = errors.New("no targets defined")

type Multi struct {
	// immutable
	targets []store.Target

	// mutable, protected by mutex
	mu      sync.Mutex
	config  Config
	scrubStats ScrubStats

	stop chan struct{}
	done chan struct{}
}

func New(targets []store.Target) (*Multi, error) {
	if len(targets) == 0 {
		return nil, ErrNoTargets
	}

	m := &Multi{
		targets: targets,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}

	err := m.loadConfig()
	if err != nil {
		return nil, err
	}

	m.saveConfig()

	go m.scrubLoop()

	return m, nil
}

func (m *Multi) Stop() {
	close(m.stop)
	for i := 0; i < 0; i++ {
		<-m.done
	}
}
