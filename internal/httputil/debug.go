package httputil

import (
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"runtime"
	"runtime/debug"
	"time"
)

func getRuntimeStats(w http.ResponseWriter, r *http.Request) {
	var stats struct {
		MemStats     runtime.MemStats
		NumGoroutine int
		NumCgoCall   int64
		NumCPU       int
		Version      string
		GCStats      debug.GCStats
	}
	runtime.ReadMemStats(&stats.MemStats)
	stats.NumGoroutine = runtime.NumGoroutine()
	stats.NumCgoCall = runtime.NumCgoCall()
	stats.NumCPU = runtime.NumCPU()
	stats.Version = runtime.Version()
	stats.GCStats.PauseQuantiles = make([]time.Duration, 11)
	debug.ReadGCStats(&stats.GCStats)
	json.NewEncoder(w).Encode(&stats)
}

func AddDebugHandlers(h http.Handler, includeDangerousOnes bool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", h)
	mux.Handle("/debug/runtimestats", http.HandlerFunc(getRuntimeStats))
	if includeDangerousOnes {
		mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
		mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
		mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
		mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
		mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	}
	return mux
}
