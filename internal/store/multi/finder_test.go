package multi

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/encryptio/slime/internal/chunkserver"
	"github.com/encryptio/slime/internal/meta"
	"github.com/encryptio/slime/internal/store"
	"github.com/encryptio/slime/internal/store/storetests"

	"github.com/encryptio/kvl"
	"github.com/encryptio/kvl/backend/ram"
)

func TestFinderScan(t *testing.T) {
	db := ram.New()

	mock := storetests.NewMockStore(0)

	cs, err := chunkserver.New([]store.Store{mock})
	if err != nil {
		t.Fatalf("Couldn't create chunkserver: %v", err)
	}
	defer cs.Close()

	cs.WaitAllAvailable()

	killer := newKillHandler(cs)
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
	if _, ok := stores[mock.UUID()]; !ok {
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
	err = db.RunReadTx(func(ctx kvl.Ctx) error {
		layer, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		loc, err := layer.GetLocation(mock.UUID())
		if err != nil {
			return err
		}

		if loc == nil {
			return fmt.Errorf("No location in database")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Couldn't verify locations: %v", err)
	}

	// when the store comes back
	killer.setKilled(false)
	err = f.Rescan()
	if err != nil {
		t.Fatalf("Couldn't Rescan: %v", err)
	}

	// it should be there again
	stores = f.Stores()
	if _, ok := stores[mock.UUID()]; !ok {
		t.Fatalf("Finder did not find uuid of directory store after resurrection")
	}
}
