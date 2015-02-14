package multi

import (
	"net/http/httptest"
	"os"
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

	multi, err := NewMulti(db, finder, MultiConfig{Need: need, Total: total})
	if err != nil {
		done()
		t.Fatalf("Couldn't create multi: %v", err)
	}

	return killers, multi, done
}

func TestMultiBasics(t *testing.T) {
	_, multi, done := prepareMultiTest(t, 3, 4, 5)
	defer done()

	testStoreBasics(t, multi)
}
