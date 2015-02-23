package httputil

import (
	"net/http"
)

type Secret struct {
	Secret string
	Inner  http.Handler
}

func (s Secret) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("x-secret") != s.Secret {
		http.Error(w, "wrong x-secret", http.StatusForbidden)
		return
	}
	s.Inner.ServeHTTP(w, r)
}
