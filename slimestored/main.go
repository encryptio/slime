package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"git.encryptio.com/multiconfig"
)

var config struct {
	Dirs   string `help:"Comma separated list of directories to serve from"`
	Listen string `help:"Listen address" default:":17941"`

	CheckSleepFile time.Duration `help:"Time to wait per file checked" default:"50ms"`
	CheckSleepByte time.Duration `help:"Time to wait per byte checked" default:"200ns"`
}

func main() {
	err := multiconfig.Setup(&config, "slimestored")
	if err != nil {
		log.Fatal(err)
	}
	flag.Parse()

	if config.Dirs == "" {
		log.Fatalf("Must be given a list of directories to serve")
	}

	dirs := strings.Split(config.Dirs, ",")
	h := &handler{dirs: dirs}
	h.start()
	http.Handle("/", h)

	log.Fatal(http.ListenAndServe(config.Listen, nil))
}
