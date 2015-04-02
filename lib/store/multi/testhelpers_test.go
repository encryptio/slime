package multi

import (
	"net/http"
	"sync"
)

type killHandler struct {
	inner http.Handler

	mu     sync.Mutex
	killed bool
}

func (k *killHandler) isKilled() bool {
	k.mu.Lock()
	ret := k.killed
	k.mu.Unlock()
	return ret
}

func (k *killHandler) setKilled(killed bool) {
	k.mu.Lock()
	k.killed = killed
	k.mu.Unlock()
}

func (k *killHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	k.mu.Lock()
	if k.killed {
		k.mu.Unlock()
		http.Error(w, "killed", http.StatusInternalServerError)
		return
	}
	k.mu.Unlock()
	k.inner.ServeHTTP(w, r)
}
