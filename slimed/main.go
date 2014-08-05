package main

import (
	"net/http"
	"log"
	"git.encryptio.com/multiconfig"
	"git.encryptio.com/slime/lib/api"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/multi"
	"flag"
	"math/rand"
	"time"
)

var args = struct {
	Listen string `help:"Address to listen on" default:":8080"`
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

	http.Handle("/", api.NewHandler(m))
	log.Fatal(http.ListenAndServe(args.Listen, nil))
}
