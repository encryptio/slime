package main

import (
	"flag"
	"git.encryptio.com/multiconfig"
	"git.encryptio.com/slime/lib/api"
	"git.encryptio.com/slime/lib/multi"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/httputil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var args = struct {
	Listen string `help:"Address to listen on"   default:":17942"`
	Debug  bool   `help:"Allow remote debugging" default:"false"`
}{}

func main() {
	rand.Seed(time.Now().UnixNano())

	err := multiconfig.Setup(&args, "slimed")
	if err != nil {
		log.Fatal(err)
	}
	flag.Parse()

	var targets []store.Target
	for _, path := range flag.Args() {
		tgt, err := store.NewFS(path)
		if err != nil {
			log.Fatalf("Couldn't open %s: %v", path, err)
		}
		targets = append(targets, tgt)
	}

	m, err := multi.New(targets)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Stop()

	mux := http.NewServeMux()
	if args.Debug {
		mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
		mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
		mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
		mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	}
	mux.Handle("/", api.NewHandler(m))

	logger := httputil.LogHttpRequests(mux)

	lsock, err := net.Listen("tcp", args.Listen)
	if err != nil {
		log.Fatal(err)
	}

	counter := CountHTTPRequests(logger)
	defer counter.Wait()

	defer lsock.Close()

	server := http.Server{
		Handler:      counter,
		ReadTimeout:  time.Hour,
		WriteTimeout: time.Hour,
	}

	go server.Serve(lsock)

	stopSignal := make(chan os.Signal)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	sig := <-stopSignal
	log.Printf("Stopping on signal %v", sig)
}
