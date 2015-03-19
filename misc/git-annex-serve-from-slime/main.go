package main

import (
	"flag"
	"log"
	"net/http"
	"os"
)

var (
	addr   = flag.String("addr", ":8080", "listening address")
	repo   = flag.String("repo", ".", "git annex repository to serve from")
	remote = flag.String("remote", "slime", "remote to use for slime")
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

	log.Fatal(http.ListenAndServe(*addr,
		http.FileServer(&FileSystem{BaseURL: baseurl, UUID: uuid})))
}
