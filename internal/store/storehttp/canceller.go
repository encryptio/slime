package storehttp

import (
	"net/http"
	"sync"
)

type canceller struct {
	Cancel <-chan struct{}
	mu     sync.Mutex
	closed chan struct{}
}

func makeCanceller(w http.ResponseWriter) *canceller {
	cancel := make(chan struct{})

	c := &canceller{
		Cancel: cancel,
		closed: make(chan struct{}),
	}

	closeNotifier, ok := w.(http.CloseNotifier)
	if ok {
		ch := closeNotifier.CloseNotify()
		go func() {
			select {
			case <-c.closed:
			case <-ch:
				close(cancel)
			}
		}()
	}

	return c
}

func (c *canceller) Close() {
	c.mu.Lock()
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	c.mu.Unlock()
}
