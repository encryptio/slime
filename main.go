package main

import (
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime/debug"
	"time"

	"git.encryptio.com/slime/lib/chunkserver"
	"git.encryptio.com/slime/lib/httputil"
	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/proxyserver"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/store/storedir"

	"git.encryptio.com/kvl/backend/psql"
	"github.com/naoina/toml"
)

const (
	defaultConfigLocation = "/etc/slime/server.toml"
)

type tomlDuration struct {
	time.Duration
}

func (d *tomlDuration) UnmarshalTOML(b []byte) error {
	// A duration has no escaped characters and no structural forms but a
	// string. This hacky parsing is sufficient.

	if len(b) < 2 || b[0] != '"' || b[len(b)-1] != '"' {
		return errors.New("bad duration format")
	}

	var err error
	d.Duration, err = time.ParseDuration(string(b[1 : len(b)-1]))
	return err
}

var config struct {
	GCPercent int `toml:"gc-percent"`

	Proxy struct {
		Listen           string
		ParallelRequests int `toml:"parallel-requests"`
		Debug            bool
		Scrubbers        int
		Database         struct {
			Type string
			DSN  string
		}
		CacheSize int `toml:"cache-size"`
	}
	Chunk struct {
		Listen           string
		ParallelRequests int `toml:"parallel-requests"`
		Dirs             []string
		Scrubber         struct {
			SleepPerFile tomlDuration `toml:"sleep-per-file"`
			SleepPerByte tomlDuration `toml:"sleep-per-byte"`
		}
	}
}

func loadConfigOrDie() {
	var configName string
	if len(os.Args) < 2 {
		configName = defaultConfigLocation
	} else {
		configName = os.Args[1]
		os.Args = append(os.Args[0:1], os.Args[2:]...)
	}

	fh, err := os.Open(configName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	defer fh.Close()

	err = toml.NewDecoder(fh).Decode(&config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if config.GCPercent <= 0 {
		config.GCPercent = 20
	}

	if config.Proxy.Listen == "" {
		config.Proxy.Listen = "127.0.0.1:17942"
	}
	if config.Proxy.ParallelRequests <= 0 {
		config.Proxy.ParallelRequests = 50
	}
	if config.Proxy.Scrubbers == 0 {
		config.Proxy.Scrubbers = 1
	}

	if config.Chunk.Listen == "" {
		config.Chunk.Listen = "127.0.0.1:17941"
	}
	if config.Chunk.ParallelRequests <= 0 {
		config.Chunk.ParallelRequests = 50
	}
	if config.Chunk.Scrubber.SleepPerFile.Duration <= 0 {
		config.Chunk.Scrubber.SleepPerFile.Duration = 50 * time.Millisecond
	}
	if config.Chunk.Scrubber.SleepPerByte.Duration <= 0 {
		config.Chunk.Scrubber.SleepPerByte.Duration = 1500 * time.Nanosecond
	}
}

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
	fmt.Fprintf(os.Stderr, "    chunk-server [config-file.toml]\n")
	fmt.Fprintf(os.Stderr, "        run a chunk server serving the directories in the config file\n")
	fmt.Fprintf(os.Stderr, "    proxy-server [config-file.toml]\n")
	fmt.Fprintf(os.Stderr, "        run a proxy server\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "    db-reindex [config-file.toml]\n")
	fmt.Fprintf(os.Stderr, "        reindex a database\n")
	fmt.Fprintf(os.Stderr, "    fmt-dir dir\n")
	fmt.Fprintf(os.Stderr, "        initialize a new directory store\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "If config-file.toml is needed but not given, it defaults to:\n")
	fmt.Fprintf(os.Stderr, "    %s\n", defaultConfigLocation)
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
	loadConfigOrDie()
	debug.SetGCPercent(config.GCPercent)

	stores := make([]store.Store, len(config.Chunk.Dirs))
	for i := range config.Chunk.Dirs {
		dir := config.Chunk.Dirs[i]
		construct := func() store.Store {
			ds, err := storedir.OpenDirectory(
				dir,
				config.Chunk.Scrubber.SleepPerFile.Duration,
				config.Chunk.Scrubber.SleepPerByte.Duration,
			)
			if err != nil {
				return nil
			}
			return ds
		}
		stores[i] = store.NewRetryStore(construct, time.Second*15)
	}

	var h http.Handler
	var err error
	h, err = chunkserver.New(stores)
	if err != nil {
		log.Fatalf("Couldn't initialize handler: %v", err)
	}

	h = httputil.NewLimitParallelism(config.Chunk.ParallelRequests, h)
	h = httputil.LogHTTPRequests(h)
	serveOrDie(config.Chunk.Listen, h)
}

func proxyServer() {
	loadConfigOrDie()
	debug.SetGCPercent(config.GCPercent)

	if config.Proxy.Database.Type != "postgresql" {
		log.Fatalf("\"postgresql\" is the only supported database type")
	}

	db, err := psql.Open(config.Proxy.Database.DSN)
	if err != nil {
		log.Fatalf("Couldn't connect to postgresql database: %v", err)
	}
	defer db.Close()

	var h http.Handler
	h, err = proxyserver.New(db, config.Proxy.Scrubbers, config.Proxy.CacheSize)
	if err != nil {
		log.Fatalf("Couldn't initialize handler: %v", err)
	}

	h = httputil.NewLimitParallelism(config.Proxy.ParallelRequests, h)

	if config.Proxy.Debug != false {
		mux := http.NewServeMux()
		mux.Handle("/", h)
		mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
		mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
		mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
		mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
		mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
		h = mux
	}

	h = httputil.LogHTTPRequests(h)

	if config.Proxy.Listen == "none" {
		for {
			time.Sleep(time.Hour)
		}
	} else {
		serveOrDie(config.Proxy.Listen, h)
	}
}

func dbReindex() {
	loadConfigOrDie()
	debug.SetGCPercent(config.GCPercent)

	if config.Proxy.Database.Type != "postgresql" {
		log.Fatalf("\"postgresql\" is the only supported database type")
	}

	db, err := psql.Open(config.Proxy.Database.DSN)
	if err != nil {
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

	log.SetFlags(log.Lshortfile)

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
