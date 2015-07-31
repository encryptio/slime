package httputil

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestLimitParallelism(t *testing.T) {
	release := make(chan struct{})

	var running, maxRunning int
	var mu sync.Mutex
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		running++
		if running > maxRunning {
			maxRunning = running
		}
		mu.Unlock()

		<-release

		mu.Lock()
		running--
		mu.Unlock()
	})

	lp := NewLimitParallelism(5, h)

	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			r, _ := http.NewRequest("GET", "/", nil)
			w := httptest.NewRecorder()
			lp.ServeHTTP(w, r)
			wg.Done()
		}()
	}

	// Try to wait until all the goroutines are blocked on lp.ServeHTTP. This is
	// racy, but can only cause spurious test successes.
	time.Sleep(time.Millisecond * 10)

	close(release)

	wg.Wait()

	if maxRunning > 5 {
		t.Errorf("maxRunning is %v, wanted at most 5", maxRunning)
	}
}
