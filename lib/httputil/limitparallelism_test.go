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
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			r, _ := http.NewRequest("GET", "/", nil)
			w := httptest.NewRecorder()
			lp.ServeHTTP(w, r)
			wg.Done()
		}()
	}

	time.Sleep(time.Millisecond * 10)

	close(release)

	wg.Wait()

	if maxRunning > 5 {
		t.Errorf("maxRunning is %v, wanted at most 5", maxRunning)
	}
}
