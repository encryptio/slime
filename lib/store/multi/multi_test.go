package multi

import (
	"net/http/httptest"
	"os"
	"testing"

	"git.encryptio.com/slime/lib/chunkserver"

	"git.encryptio.com/kvl/backend/ram"
)

func TestMultiBasics(t *testing.T) {
	var killers []*killHandler
	var servers []*httptest.Server
	for i := 0; i < 5; i++ {
		_, tmpPath := makeDirectory(t)
		defer os.RemoveAll(tmpPath)

		cs, err := chunkserver.New([]string{tmpPath}, 0, 0)
		if err != nil {
			t.Fatalf("Couldn't create chunkserver: %v", err)
		}
		defer cs.Stop()

		killer := &killHandler{inner: cs}
		srv := httptest.NewServer(killer)
		defer srv.Close()

		killers = append(killers, killer)
		servers = append(servers, srv)
	}

	db := ram.New()

	finder, err := NewFinder(db)
	if err != nil {
		t.Fatalf("Couldn't create new finder: %v", err)
	}
	defer finder.Stop()

	for _, srv := range servers {
		err = finder.Scan(srv.URL)
		if err != nil {
			t.Fatalf("Couldn't scan %v: %v", srv.URL, err)
		}
	}

	if len(finder.Stores()) != len(servers) {
		t.Fatalf("Finder did not find all stores")
	}

	m, err := NewMulti(db, finder, MultiConfig{Need: 3, Total: 4})
	if err != nil {
		t.Fatalf("Couldn't create multi: %v", err)
	}

	testStoreBasics(t, m)
}
