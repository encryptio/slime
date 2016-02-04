package multi

import (
	"math/rand"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/encryptio/slime/internal/chunkserver"
	"github.com/encryptio/slime/internal/store"
	"github.com/encryptio/slime/internal/store/storetests"

	"git.encryptio.com/kvl/backend/ram"
)

func prepareMultiTest(t *testing.T, need, total, serverCount int) ([]*killHandler, *Multi, []*storetests.MockStore, func()) {
	var killers []*killHandler
	var mockstores []*storetests.MockStore

	var servers []*httptest.Server
	var chunkServers []*chunkserver.Handler
	var finder *Finder
	var multi *Multi

	done := func() {
		if multi != nil {
			multi.Close()
		}
		if finder != nil {
			finder.Stop()
		}
		for _, srv := range servers {
			srv.Close()
		}
		for _, cs := range chunkServers {
			cs.Close()
		}
		for _, mock := range mockstores {
			mock.Close()
		}
	}

	for i := 0; i < serverCount; i++ {
		mock := storetests.NewMockStore(0)
		mockstores = append(mockstores, mock)

		cs, err := chunkserver.New([]store.Store{mock})
		if err != nil {
			done()
			t.Fatalf("Couldn't create chunkserver: %v", err)
		}
		chunkServers = append(chunkServers, cs)

		killer := newKillHandler(cs)
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

	multi, err = NewMulti(db, finder, 0)
	if err != nil {
		done()
		t.Fatalf("Couldn't create multi: %v", err)
	}

	err = multi.SetRedundancy(need, total)
	if err != nil {
		done()
		t.Fatalf("Couldn't set redundancy levels: %v", err)
	}

	return killers, multi, mockstores, done
}

func TestMultiCommon(t *testing.T) {
	_, multi, _, done := prepareMultiTest(t, 2, 3, 4)
	defer done()
	storetests.TestStore(t, multi)
}

func TestMultiRecovery(t *testing.T) {
	for total := 4; total < 6; total++ {
		killers, multi, _, done := prepareMultiTest(t, 3, total, 6)
		defer done()

		for i := 0; i < 20; i++ {
			key := strconv.FormatInt(int64(i), 10)
			var value []byte
			for j := 0; j < i; j++ {
				value = append(value, []byte(key)...)
			}

			storetests.ShouldCAS(t, multi, key, store.MissingV, store.DataV(value))
		}

		for i := 0; i < total-3; i++ {
			for {
				k := killers[rand.Intn(len(killers))]
				if k.isKilled() {
					continue
				}
				k.setKilled(true)
				break
			}
		}

		for i := 0; i < 20; i++ {
			key := strconv.FormatInt(int64(i), 10)
			var value []byte
			for j := 0; j < i; j++ {
				value = append(value, []byte(key)...)
			}

			storetests.ShouldGet(t, multi, key, value)
		}
	}
}

func TestMultiScrubRecreatesMissing(t *testing.T) {
	_, multi, mocks, done := prepareMultiTest(t, 2, 3, 3)
	defer done()

	data := []byte("hello world! this is some test data.")

	storetests.ShouldCAS(t, multi, "key", store.MissingV, store.DataV(data))

	names, err := mocks[0].List("", 1, nil)
	if err != nil {
		t.Fatalf("Couldn't list first mock: %v", err)
	}
	if len(names) != 1 {
		t.Fatalf("Didn't get a name from mock")
	}

	storetests.ShouldCAS(t, mocks[0], names[0], store.AnyV, store.MissingV)

	multi.scrubAll()

	found := 0
	for _, mock := range mocks {
		names, err := mock.List("", 1, nil)
		if err != nil {
			t.Fatalf("Couldn't list mock: %v", err)
		}
		if len(names) == 1 {
			found++
		}
	}

	if found != 3 {
		t.Fatalf("scrubAll didn't recreate missing chunk")
	}
}

func TestMultiDuplicateContent(t *testing.T) {
	_, multi, _, done := prepareMultiTest(t, 1, 1, 1)
	defer done()

	data := []byte("this is some test data")

	for i := 0; i < 10; i++ {
		storetests.ShouldCAS(t, multi,
			strconv.FormatInt(int64(i), 10),
			store.MissingV, store.DataV(data))
	}

	for i := 0; i < 10; i++ {
		storetests.ShouldGet(t, multi,
			strconv.FormatInt(int64(i), 10),
			data)
	}

	for i := 0; i < 5; i++ {
		storetests.ShouldCAS(t, multi,
			strconv.FormatInt(int64(i), 10),
			store.AnyV, store.MissingV)
	}

	for i := 5; i < 10; i++ {
		storetests.ShouldGet(t, multi,
			strconv.FormatInt(int64(i), 10),
			data)
	}
}

func TestMultiScrubChangeRedundancy(t *testing.T) {
	killers, multi, _, done := prepareMultiTest(t, 2, 3, 5)
	defer done()

	data := []byte("who knows where the wind goes")

	for i := 0; i < 10; i++ {
		storetests.ShouldCAS(t, multi,
			strconv.FormatInt(int64(i), 10),
			store.MissingV, store.DataV(data))
	}

	err := multi.SetRedundancy(2, 5)
	if err != nil {
		t.Fatalf("Couldn't adjust redundancy: %v", err)
	}

	multi.scrubAll()

	killers[0].setKilled(true)
	killers[1].setKilled(true)
	killers[2].setKilled(true)

	for i := 0; i < 10; i++ {
		storetests.ShouldGet(t, multi,
			strconv.FormatInt(int64(i), 10),
			data)
	}
}

func TestMultiCanReplaceDeadKeys(t *testing.T) {
	killers, multi, _, done := prepareMultiTest(t, 3, 4, 4)
	defer done()

	storetests.ShouldCAS(t, multi, "a", store.MissingV, store.DataV([]byte("hello")))
	killers[0].setKilled(true)

	err := multi.SetRedundancy(1, 2)
	if err != nil {
		t.Fatalf("Couldn't adjust redundancy: %v", err)
	}

	storetests.ShouldCAS(t, multi, "a", store.DataV([]byte("hello")), store.DataV([]byte("there")))
	killers[1].setKilled(true)
	storetests.ShouldCAS(t, multi, "a", store.DataV([]byte("there")), store.MissingV)
}

func TestMultiScrubRemovesWeirdChunks(t *testing.T) {
	_, multi, mocks, done := prepareMultiTest(t, 1, 1, 1)
	defer done()

	storetests.ShouldCAS(t, mocks[0], "a", store.MissingV, store.DataV([]byte("data")))
	multi.scrubAll()
	storetests.ShouldGetMiss(t, mocks[0], "a")
}

func TestMultiScrubRemovesUnreferencedChunks(t *testing.T) {
	killers, multi, mocks, done := prepareMultiTest(t, 1, 2, 2)
	defer done()

	storetests.ShouldCAS(t, multi, "a", store.MissingV, store.DataV([]byte("data")))
	killers[0].setKilled(true)
	storetests.ShouldCAS(t, multi, "a", store.DataV([]byte("data")), store.MissingV)
	killers[0].setKilled(false)
	storetests.ShouldListCount(t, multi, 0)
	storetests.ShouldListCount(t, mocks[0], 1)
	multi.finder.Rescan()
	multi.scrubAll()
	storetests.ShouldListCount(t, mocks[0], 0)
}

func TestMultiHungStoreDoesntBlock(t *testing.T) {
	killers, multi, _, done := prepareMultiTest(t, 2, 4, 4)
	defer done()

	oldTimeout := dataOnlyTimeout
	dataOnlyTimeout = 10 * time.Millisecond
	defer func() { dataOnlyTimeout = oldTimeout }()

	storetests.ShouldCAS(t, multi, "a", store.MissingV, store.DataV([]byte("data")))

	for _, k := range killers {
		k.setBlocked(true)

		result := make(chan error)
		go func() {
			_, _, err := multi.Get("a", store.GetOptions{})
			result <- err
		}()

		select {
		case err := <-result:
			if err != nil {
				t.Errorf("couldn't get key: %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("timed out waiting for read")
		}

		k.setBlocked(false)
	}
}
