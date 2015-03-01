package main

import (
	crand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"time"

	"git.encryptio.com/slime/lib/chunkserver"
	"git.encryptio.com/slime/lib/httputil"
	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/proxyserver"
	"git.encryptio.com/slime/lib/store/storedir"

	"git.encryptio.com/kvl/backend/psql"
)

func initRandom() {
	var buf [8]byte
	_, err := crand.Read(buf[:])
	if err != nil {
		panic(err)
	}

	rand.Seed(int64(binary.BigEndian.Uint64(buf[:])))
}

func help() {
	fmt.Fprintf(os.Stderr, "Usage: %v command [args]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "    fmt-dir dir\n")
	fmt.Fprintf(os.Stderr, "        initialize a new directory store\n")
	fmt.Fprintf(os.Stderr, "    chunk-server dir1 dir2 ...\n")
	fmt.Fprintf(os.Stderr, "        run a chunk server serving the given directories\n")
	fmt.Fprintf(os.Stderr, "    proxy-server ...\n")
	fmt.Fprintf(os.Stderr, "        run a proxy server (see -h for details)\n")
	fmt.Fprintf(os.Stderr, "    db-reindex\n")
	fmt.Fprintf(os.Stderr, "        reindex a database\n")
}

func serveOrDie(listen string, h http.Handler) {
	srv := &http.Server{
		Addr:         listen,
		Handler:      h,
		ReadTimeout:  time.Minute * 15,
		WriteTimeout: time.Minute * 15,
	}
	log.Fatal(srv.ListenAndServe())
}

func fmtDir() {
	if len(os.Args) != 2 {
		help()
		os.Exit(1)
	}

	err := storedir.CreateDirectory(os.Args[1])
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
	sleepByte := flag.Duration("sleep-byte", 600*time.Nanosecond,
		"Sleep per byte checked")

	parallel := flag.Int("parallel", 50,
		"max number of requests to handle in parallel")

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

	if *parallel > 0 {
		h = httputil.NewLimitParallelism(*parallel, h)
	}

	if *logEnabled {
		h = httputil.LogHttpRequests(h)
	}

	serveOrDie(*listen, h)
}

func proxyServer() {
	listen := flag.String("listen", ":17942",
		"Address and port to serve on")
	logEnabled := flag.Bool("log", true,
		"enable access logging")
	parallel := flag.Int("parallel", 50,
		"max number of requests to handle in parallel")
	flag.Parse()

	db, err := psql.Open(os.Getenv("SLIME_PGDSN"))
	if err != nil {
		if os.Getenv("SLIME_PGDSN") == "" {
			log.Printf("Set SLIME_PGDSN for PostgreSQL driver options")
		}
		log.Fatalf("Couldn't connect to postgresql database: %v", err)
	}
	defer db.Close()

	var h http.Handler
	h, err = proxyserver.New(db)
	if err != nil {
		log.Fatalf("Couldn't initialize handler: %v", err)
	}

	if *parallel > 0 {
		h = httputil.NewLimitParallelism(*parallel, h)
	}

	if *logEnabled {
		h = httputil.LogHttpRequests(h)
	}

	serveOrDie(*listen, h)
}

func dbReindex() {
	db, err := psql.Open(os.Getenv("SLIME_PGDSN"))
	if err != nil {
		if os.Getenv("SLIME_PGDSN") == "" {
			log.Printf("Set SLIME_PGDSN for PostgreSQL driver options")
		}
		log.Fatalf("Couldn't connect to postgresql database: %v", err)
	}
	defer db.Close()

	err = meta.Reindex(db) // does its own logging
	if err != nil {
		os.Exit(1)
	}
}

func main() {
	initRandom()
	runtime.GOMAXPROCS(runtime.NumCPU())

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
	case "proxy-server":
		proxyServer()
	case "db-reindex":
		dbReindex()
	default:
		help()
	}
}
