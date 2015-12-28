package multi

import (
	"math/rand"
	"time"
)

func jitterDuration(d time.Duration) time.Duration {
	adj := (rand.Float64() - 0.5) / 10
	d += time.Duration(adj * float64(d))
	return d
}
