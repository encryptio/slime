package multi

import (
	"bytes"
	"math/rand"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"git.encryptio.com/slime/lib/chunkserver"

	"git.encryptio.com/kvl/backend/ram"
)

func prepareMultiTest(t *testing.T, need, total, serverCount int) ([]*killHandler, *Multi, func()) {
	var killers []*killHandler

	var servers []*httptest.Server
	var chunkServers []*chunkserver.Handler
	var tmpPaths []string
	var finder *Finder

	done := func() {
		if finder != nil {
			finder.Stop()
		}
		for _, srv := range servers {
			srv.Close()
		}
		for _, cs := range chunkServers {
			cs.Stop()
		}
		for _, path := range tmpPaths {
			os.RemoveAll(path)
		}
	}

	for i := 0; i < serverCount; i++ {
		_, tmpPath := makeDirectory(t)
		tmpPaths = append(tmpPaths, tmpPath)

		cs, err := chunkserver.New([]string{tmpPath}, 0, 0)
		if err != nil {
			done()
			t.Fatalf("Couldn't create chunkserver: %v", err)
		}
		chunkServers = append(chunkServers, cs)

		killer := &killHandler{inner: cs}
		srv := httptest.NewServer(killer)

		killers = append(killers, killer)
		servers = append(servers, srv)
	}

	db := ram.New()

	finder, err := NewFinder(db)
	if err != nil {
		done()
		t.Fatalf("Couldn't create new finder: %v", err)
	}

	for _, srv := range servers {
		err = finder.Scan(srv.URL)
		if err != nil {
			done()
			t.Fatalf("Couldn't scan %v: %v", srv.URL, err)
		}
	}

	if len(finder.Stores()) != len(servers) {
		done()
		t.Fatalf("Finder did not find all stores")
	}

	multi, err := NewMulti(db, finder)
	if err != nil {
		done()
		t.Fatalf("Couldn't create multi: %v", err)
	}

	err = multi.SetRedundancy(need, total)
	if err != nil {
		done()
		t.Fatalf("Couldn't set redundancy levels: %v", err)
	}

	return killers, multi, done
}

func TestMultiBasics(t *testing.T) {
	_, multi, done := prepareMultiTest(t, 3, 4, 5)
	defer done()

	testStoreBasics(t, multi)
}

func TestMultiRecovery(t *testing.T) {
	for total := 4; total < 8; total++ {
		killers, multi, done := prepareMultiTest(t, 3, total, 10)
		defer done()

		for i := 0; i < 50; i++ {
			key := strconv.FormatInt(int64(i), 10)
			var value []byte
			for j := 0; j < i; j++ {
				value = append(value, []byte(key)...)
			}

			err := multi.Set(key, value)
			if err != nil {
				t.Fatalf("Couldn't write to multi: %v", err)
			}
		}

		for i := 0; i < total-3; i++ {
			for {
				k := killers[rand.Intn(len(killers))]
				if k.killed {
					continue
				}
				k.killed = true
				break
			}
		}

		for i := 0; i < 50; i++ {
			key := strconv.FormatInt(int64(i), 10)
			var value []byte
			for j := 0; j < i; j++ {
				value = append(value, []byte(key)...)
			}

			gotVal, err := multi.Get(key)
			if err != nil {
				t.Fatalf("Couldn't get %v from multi after failing underneath redundancy level: %v", key, err)
			}

			if !bytes.Equal(value, gotVal) {
				t.Fatalf("Value for %v is incorrect (got %#v, wanted %#v)", gotVal, value)
			}
		}
	}
}
