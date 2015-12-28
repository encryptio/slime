package retry

import (
	"math/rand"
	"time"
)

var initialDelay = 5 * time.Millisecond

type Retrier struct {
	max     int
	current int
	delay   time.Duration
}

func New(maxTries int) *Retrier {
	return &Retrier{max: maxTries, delay: initialDelay}
}

func (r *Retrier) Next() bool {
	if r.current >= r.max {
		return false
	}

	r.current++

	if r.current == 1 {
		return true
	}

	delay := time.Duration(float64(r.delay) * rand.Float64())
	time.Sleep(delay)
	r.delay *= 2

	return true
}
