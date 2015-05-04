package multi

import (
	"net/http"
	"sync"
)

type killHandler struct {
	inner http.Handler

	mu      sync.Mutex
	cond    *sync.Cond
	killed  bool
	blocked bool
}

func newKillHandler(inner http.Handler) *killHandler {
	k := &killHandler{inner: inner}
	k.cond = sync.NewCond(&k.mu)
	return k
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

func (k *killHandler) setBlocked(blocked bool) {
	k.mu.Lock()
	k.blocked = blocked
	k.mu.Unlock()
	k.cond.Broadcast()
}

func (k *killHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	k.mu.Lock()
	if k.killed {
		k.mu.Unlock()
		http.Error(w, "killed", http.StatusInternalServerError)
		return
	}
	for k.blocked {
		k.cond.Wait()
	}
	k.mu.Unlock()
	k.inner.ServeHTTP(w, r)
}
