package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"git.encryptio.com/slime/lib/httputil"
)

var (
	addr    = flag.String("addr", ":8080", "listening address")
	repo    = flag.String("repo", ".", "git annex repository to serve from")
	remote  = flag.String("remote", "slime", "remote to use for slime")
	loghttp = flag.Bool("loghttp", true, "log http requests")
)

func main() {
	flag.Parse()

	uuid, err := gitAnnexUUID(*remote)
	if err != nil {
		log.Printf("Couldn't get UUID for remote %v: %v", *remote, err)
		os.Exit(1)
	}

	log.Printf("got uuid %v", uuid)

	baseurl, err := getSlimeBaseURL(uuid)
	if err != nil {
		log.Printf("Couldn't get slime base url for UUID %v: %v", uuid, err)
		os.Exit(1)
	}

	log.Printf("got base url %v", baseurl)

	var handler http.Handler = http.FileServer(&FileSystem{
		BaseURL: baseurl,
		UUID:    uuid,
	})
	if *loghttp {
		handler = httputil.LogHTTPRequests(handler)
	}

	log.Fatal(http.ListenAndServe(*addr, handler))
}
