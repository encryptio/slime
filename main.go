package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"git.encryptio.com/slime/lib/chunkserver"
	"git.encryptio.com/slime/lib/httputil"
	"git.encryptio.com/slime/lib/store"
)

func help() {
	fmt.Fprintf(os.Stderr, "Usage: %v command [args]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "    fmt-dir dir\n")
	fmt.Fprintf(os.Stderr, "        initialize a new directory store\n")
	fmt.Fprintf(os.Stderr, "    chunk-server dir1 dir2 ...\n")
	fmt.Fprintf(os.Stderr, "        run a chunk server serving the given directories\n")
}

func fmtDir() {
	if len(os.Args) != 2 {
		help()
		os.Exit(1)
	}

	err := store.CreateDirectory(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func chunkServer() {
	listen := flag.String("listen", ":17941",
		"Address and port to serve on")

	sleepFile := flag.Duration("sleep-file", 50*time.Millisecond,
		"Sleep per file checked")
	sleepByte := flag.Duration("sleep-byte", 200*time.Nanosecond,
		"Sleep per byte checked")

	logEnabled := flag.Bool("log", true,
		"enable access logging")

	flag.Parse()

	dirs := flag.Args()

	if len(dirs) == 0 {
		log.Fatalf("Must be given a list of directories to serve")
	}

	var h http.Handler
	var err error
	h, err = chunkserver.New(dirs, *sleepFile, *sleepByte)
	if err != nil {
		log.Fatalf("Couldn't initialize handler: %v", err)
	}

	if *logEnabled {
		h = httputil.LogHttpRequests(h)
	}

	log.Fatal(http.ListenAndServe(*listen, h))
}

func main() {
	if len(os.Args) < 2 {
		help()
		os.Exit(1)
	}

	command := os.Args[1]
	os.Args = append(os.Args[0:1], os.Args[2:]...)

	switch command {
	case "fmt-dir":
		fmtDir()
	case "chunk-server":
		chunkServer()
	default:
		help()
	}
}
