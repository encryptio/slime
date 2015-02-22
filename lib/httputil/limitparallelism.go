package httputil

import (
	"net/http"
)

type LimitParallelism struct {
	tokens chan struct{}
	h      http.Handler
}

func NewLimitParallelism(n int, h http.Handler) *LimitParallelism {
	ch := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		ch <- struct{}{}
	}
	return &LimitParallelism{ch, h}
}

func (lp *LimitParallelism) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	<-lp.tokens
	// must use defer, not inline after the call in case lp.h.ServeHTTP panics
	defer func(tokens chan struct{}) { tokens <- struct{}{} }(lp.tokens)
	lp.h.ServeHTTP(w, r)
}
