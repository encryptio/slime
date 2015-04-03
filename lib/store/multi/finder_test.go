package multi

import (
	"fmt"
	"net/http/httptest"
	"os"
	"testing"

	"git.encryptio.com/slime/lib/chunkserver"
	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/store/storedir"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/backend/ram"
)

func TestFinderScan(t *testing.T) {
	db := ram.New()

	ds, tmpPath := storedir.MakeTestingDirectory(t)
	defer os.RemoveAll(tmpPath)

	cs, err := chunkserver.New([]store.Store{ds})
	if err != nil {
		t.Fatalf("Couldn't create chunkserver: %v", err)
	}
	defer cs.Close()

	cs.WaitAllAvailable()

	killer := &killHandler{inner: cs}
	srv := httptest.NewServer(killer)
	defer srv.Close()

	f, err := NewFinder(db)
	if err != nil {
		t.Fatalf("Couldn't create new finder: %v", err)
	}
	defer f.Stop()

	err = f.Scan(srv.URL)
	if err != nil {
		t.Fatalf("Couldn't scan %v: %v", srv.URL, err)
	}

	// newly scanned store should be in the Finder
	stores := f.Stores()
	if _, ok := stores[ds.UUID()]; !ok {
		t.Fatalf("Finder did not find uuid of directory store")
	}

	// kill the store and update the Finder
	killer.setKilled(true)
	f.test(0)

	// should have been removed
	stores = f.Stores()
	if len(stores) > 0 {
		t.Fatalf("Finder did not remove dead store")
	}

	// but should stay in the DB
	_, err = db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		loc, err := layer.GetLocation(ds.UUID())
		if err != nil {
			return nil, err
		}

		if loc == nil {
			return nil, fmt.Errorf("No location in database")
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("Couldn't verify locations: %v", err)
	}

	// when the store comes back
	killer.setKilled(false)
	err = f.scanStart()
	if err != nil {
		t.Fatalf("Couldn't scanStart: %v", err)
	}

	// it should be there again
	stores = f.Stores()
	if _, ok := stores[ds.UUID()]; !ok {
		t.Fatalf("Finder did not find uuid of directory store after resurrection")
	}
}
