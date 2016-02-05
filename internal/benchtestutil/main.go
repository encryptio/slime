package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var baseURL = flag.String("baseurl", "", "http path to proxy data")
var parallelism = flag.Int("parallelism", 8, "number of parallel requests to make")

func main() {
	flag.Parse()

	var files [][]byte
	for _, size := range []int{100, 1000, 10000, 100000, 1000000, 10000000, 50000000} {
		file := make([]byte, size)
		for i := range file {
			file[i] = byte(rand.Int())
		}
		files = append(files, file)
	}

	fmt.Println("Testing PUT performance")
	timeRequests(files, func(body []byte) *http.Request {
		req, err := http.NewRequest("PUT", *baseURL+"file-"+strconv.Itoa(len(body)), bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		return req
	})

	fmt.Println("Testing GET performance")
	timeRequests(files, func(body []byte) *http.Request {
		req, err := http.NewRequest("GET", *baseURL+"file-"+strconv.Itoa(len(body)), nil)
		if err != nil {
			panic(err)
		}
		return req
	})

	fmt.Println("Testing GET NoVerify performance")
	timeRequests(files, func(body []byte) *http.Request {
		req, err := http.NewRequest("GET", *baseURL+"file-"+strconv.Itoa(len(body)), nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("X-Slime-Noverify", "true")
		return req
	})
}

func timeRequests(bodies [][]byte, makeRequest func([]byte) *http.Request) {
	type perfEntry struct {
		TimeNS   uint64
		Requests uint64
		Bytes    uint64
	}

	perf := make([]perfEntry, len(bodies))

	fmt.Printf("Using %v parallel requests\n", *parallelism)

	start := time.Now()
	var wg sync.WaitGroup
	for z := 0; z < *parallelism; z++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Sub(start) < time.Minute {
				for i, body := range bodies {
					atomic.AddUint64(&perf[i].TimeNS, uint64(timeRequest(makeRequest(body))))
					atomic.AddUint64(&perf[i].Requests, 1)
					atomic.AddUint64(&perf[i].Bytes, uint64(len(body)))
				}
			}
		}()
	}
	wg.Wait()

	for i := range perf {
		timeNS := atomic.LoadUint64(&perf[i].TimeNS) / uint64(*parallelism)
		bytes := atomic.LoadUint64(&perf[i].Bytes)
		requests := atomic.LoadUint64(&perf[i].Requests)

		seconds := float64(timeNS) / float64(time.Second)
		bytesPerSec := float64(bytes) / seconds
		requestsPerSec := float64(requests) / seconds
		fmt.Printf("% 10d byte files %.1fMiB/sec %.2f req/sec (%d total requests)\n",
			len(bodies[i]), bytesPerSec/1024/1024, requestsPerSec, requests)
	}
}

func timeRequest(r *http.Request) time.Duration {
	start := time.Now()
	resp, err := http.DefaultClient.Do(r)
	end := time.Now()
	if err != nil {
		panic(err)
	}
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		panic("response code out of range")
	}

	return end.Sub(start)
}
