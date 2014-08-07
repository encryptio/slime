package main

import (
	"flag"
	"git.encryptio.com/multiconfig"
	"git.encryptio.com/slime/lib/api"
	"git.encryptio.com/slime/lib/multi"
	"git.encryptio.com/slime/lib/store"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var args = struct {
	Listen string `help:"Address to listen on" default:":17942"`
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

	http.Handle("/", api.NewHandler(m))
	go func() {
		log.Fatal(http.ListenAndServe(args.Listen, nil))
	}()

	stopSignal := make(chan os.Signal)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	sig := <-stopSignal
	log.Printf("Stopping on signal %v", sig)
}
