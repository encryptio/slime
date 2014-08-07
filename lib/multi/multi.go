package multi

import (
	"errors"
	"git.encryptio.com/slime/lib/store"
	"sync"
)

var ErrNoTargets = errors.New("no targets defined")

type Multi struct {
	// immutable
	targets []store.Target

	// mutable, protected by mutex
	mu         sync.Mutex
	config     Config
	scrubStats ScrubStats
	rebal      rebalanceInfo

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
	for i := 0; i < 1; i++ {
		<-m.done
	}
}

func (m *Multi) isStopping() bool {
	select {
	case <-m.stop:
		return true
	default:
		return false
	}
}
