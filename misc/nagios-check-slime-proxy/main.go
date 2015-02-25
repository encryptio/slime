package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"git.encryptio.com/slime/lib/uuid"
)

func dieUnknown(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
	os.Exit(3)
}

func urlJoin(addr, path string) string {
	return fmt.Sprintf("http://%v%v", addr, path)
}

func checkUUID(addr string, wantID [16]byte) {
	resp, err := http.Get(urlJoin(addr, "/data/?mode=uuid"))
	if err != nil {
		dieUnknown("Couldn't get uuid: %v", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		dieUnknown("Got response code %v from uuid request",
			resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		dieUnknown("Couldn't read response for uuid: %v", err)
	}

	resp.Body.Close()

	id, err := uuid.Parse(string(data))
	if err != nil {
		dieUnknown("Couldn't parse uuid from server: %v", err)
	}

	if id != wantID {
		fmt.Printf("wanted id %v, got id %v\n",
			uuid.Fmt(wantID), uuid.Fmt(id))
		os.Exit(2)
	}
}

func checkConnectivity(addr string, warn, crit int) {
	resp, err := http.Get(urlJoin(addr, "/stores"))
	if err != nil {
		dieUnknown("Couldn't get store list: %v", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		dieUnknown("Get response code %v from stores request",
			resp.StatusCode)
	}

	type store struct {
		UUID      string `json:"uuid"`
		Name      string `json:"name"`
		Dead      bool   `json:"dead"`
		Connected bool   `json:"connected"`
	}

	var ret []store
	err = json.NewDecoder(resp.Body).Decode(&ret)
	if err != nil {
		dieUnknown("Couldn't read response for stores: %v", err)
	}
	resp.Body.Close()

	unconnected := 0
	ok := 0
	dead := 0
	extra := ""
	for _, st := range ret {
		if !st.Connected && !st.Dead {
			unconnected++
			extra += fmt.Sprintf("%v (%v)\n", st.UUID, st.Name)
		} else if st.Connected {
			ok++
		} else if st.Dead {
			dead++
		}
	}

	fmt.Printf("%v bad, %v ok, %v ok dead\n", unconnected, ok, dead)
	fmt.Print(extra)
	if unconnected >= crit {
		os.Exit(2)
	} else if unconnected >= warn {
		os.Exit(1)
	}
}

func main() {
	addr := flag.String("addr", "", "host:port of slime-proxy to check")
	uuidFlag := flag.String("uuid", "", "uuid of metadata store to verify")
	warnUnconnected := flag.Int("warn", 0,
		"WARN if number of undead unconnected stores exceeds this number")
	critUnconnected := flag.Int("crit", 0,
		"CRIT if number of undead unconnected stores exceeds this number")
	flag.Parse()

	if *addr == "" ||
		*uuidFlag == "" ||
		*warnUnconnected == 0 ||
		*critUnconnected == 0 {

		dieUnknown("Incorrect invocation, all arguments are required.\n" +
			"Run with -h for usage.")
	}

	wantID, err := uuid.Parse(*uuidFlag)
	if err != nil {
		dieUnknown("Couldn't parse uuid given: %v", err)
	}

	checkUUID(*addr, wantID)
	checkConnectivity(*addr, *warnUnconnected, *critUnconnected)
	os.Exit(0)
}
