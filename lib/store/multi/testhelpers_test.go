package multi

import (
	"net/http"
)

type killHandler struct {
	inner  http.Handler
	killed bool
}

func (k *killHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if k.killed {
		http.Error(w, "killed", http.StatusInternalServerError)
		return
	}
	k.inner.ServeHTTP(w, r)
}
