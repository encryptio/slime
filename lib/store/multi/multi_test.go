package multi

import (
	"bytes"
	"math/rand"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"git.encryptio.com/slime/lib/chunkserver"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/store/storedir"
	"git.encryptio.com/slime/lib/store/storetests"

	"git.encryptio.com/kvl/backend/ram"
)

func mustCAS(t *testing.T, message string, st store.Store, key string, from, to store.CASV) {
	err := st.CAS(key, from, to)
	if err != nil {
		t.Fatalf("Couldn't CAS(%#v) during %v: %v", key, message, err)
	}
}

func mustGet(t *testing.T, message string, st store.Store, key string, value []byte) {
	got, _, err := st.Get(key)
	if err != nil {
		t.Fatalf("Couldn't Get(%#v) during %v: %v", key, message, err)
	}

	if !bytes.Equal(got, value) {
		t.Fatalf("Get(%#v) returned %#v during %v, but wanted %#v", key, string(got), message, string(value))
	}
}

func mustGetMiss(t *testing.T, message string, st store.Store, key string) {
	got, _, err := st.Get(key)
	if err != store.ErrNotFound {
		if err != nil {
			t.Fatalf("Couldn't Get(%#v) during %v: %v", key, message, err)
		}
		t.Fatalf("Get(%#v) returned unexpected data %#v during %v, but wanted ErrNotFound",
			key, string(got), message)
	}
}

func mustListCount(t *testing.T, message string, st store.Store, count int) {
	actualCount := 0

	from := ""
	for {
		list, err := st.List(from, 100)
		if err != nil {
			t.Fatalf("Couldn't List(%#v, 100) during %v: %v", from, message, err)
		}

		actualCount += len(list)
		if len(list) < 100 {
			break
		}
	}

	if actualCount != count {
		t.Fatalf("List returned %v elements during %v but wanted %v", actualCount, message, count)
	}
}

func prepareMultiTest(t *testing.T, need, total, serverCount int) ([]*killHandler, *Multi, []*storedir.Directory, func()) {
	var killers []*killHandler
	var dirstores []*storedir.Directory

	var servers []*httptest.Server
	var chunkServers []*chunkserver.Handler
	var tmpPaths []string
	var finder *Finder
	var multi *Multi

	done := func() {
		if multi != nil {
			multi.Stop()
		}
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
		dirstore, tmpPath := storedir.MakeTestingDirectory(t)
		tmpPaths = append(tmpPaths, tmpPath)
		dirstores = append(dirstores, dirstore)

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

	multi, err = NewMulti(db, finder)
	if err != nil {
		done()
		t.Fatalf("Couldn't create multi: %v", err)
	}

	err = multi.SetRedundancy(need, total)
	if err != nil {
		done()
		t.Fatalf("Couldn't set redundancy levels: %v", err)
	}

	return killers, multi, dirstores, done
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

			mustCAS(t, "fill multi", multi, key, store.MissingV, store.DataV(value))
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

			mustGet(t, "after failing underneath redundancy level", multi, key, value)
		}
	}
}

func TestMultiScrubRecreatesMissing(t *testing.T) {
	_, multi, dirstores, done := prepareMultiTest(t, 2, 3, 3)
	defer done()

	data := []byte("hello world! this is some test data.")

	mustCAS(t, "fill", multi, "key", store.MissingV, store.DataV(data))

	names, err := dirstores[0].List("", 1)
	if err != nil {
		t.Fatalf("Couldn't list first dirstore: %v", err)
	}
	if len(names) != 1 {
		t.Fatalf("Didn't get a name from dirstore")
	}

	mustCAS(t, "kill", dirstores[0], names[0], store.AnyV, store.MissingV)

	multi.scrubAll()

	found := 0
	for _, ds := range dirstores {
		names, err := ds.List("", 1)
		if err != nil {
			t.Fatalf("Couldn't list dirstore: %v", err)
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
		mustCAS(t, "fill", multi,
			strconv.FormatInt(int64(i), 10),
			store.MissingV, store.DataV(data))
	}

	for i := 0; i < 10; i++ {
		mustGet(t, "after fill", multi,
			strconv.FormatInt(int64(i), 10),
			data)
	}

	for i := 0; i < 5; i++ {
		mustCAS(t, "delete half", multi,
			strconv.FormatInt(int64(i), 10),
			store.AnyV, store.MissingV)
	}

	for i := 5; i < 10; i++ {
		mustGet(t, "after delete", multi,
			strconv.FormatInt(int64(i), 10),
			data)
	}
}

func TestMultiScrubChangeRedundancy(t *testing.T) {
	killers, multi, _, done := prepareMultiTest(t, 2, 3, 5)
	defer done()

	data := []byte("who knows where the wind goes")

	for i := 0; i < 10; i++ {
		mustCAS(t, "fill", multi,
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
		mustGet(t, "check", multi,
			strconv.FormatInt(int64(i), 10),
			data)
	}
}

func TestMultiCanReplaceDeadKeys(t *testing.T) {
	killers, multi, _, done := prepareMultiTest(t, 3, 4, 4)
	defer done()

	mustCAS(t, "initial write", multi, "a", store.MissingV, store.DataV([]byte("hello")))
	killers[0].setKilled(true)

	err := multi.SetRedundancy(1, 2)
	if err != nil {
		t.Fatalf("Couldn't adjust redundancy: %v", err)
	}

	mustCAS(t, "write after fail 1", multi, "a", store.DataV([]byte("hello")), store.DataV([]byte("there")))
	killers[1].setKilled(true)
	mustCAS(t, "write after fail 2", multi, "a", store.DataV([]byte("there")), store.MissingV)
}

func TestMultiScrubRemovesWeirdChunks(t *testing.T) {
	_, multi, dirs, done := prepareMultiTest(t, 1, 1, 1)
	defer done()

	mustCAS(t, "write key", dirs[0], "a", store.MissingV, store.DataV([]byte("data")))
	multi.scrubAll()
	mustGetMiss(t, "after scrub", dirs[0], "a")
}

func TestMultiScrubRemovesUnreferencedChunks(t *testing.T) {
	killers, multi, dirs, done := prepareMultiTest(t, 1, 2, 2)
	defer done()

	mustCAS(t, "write key", multi, "a", store.MissingV, store.DataV([]byte("data")))
	killers[0].setKilled(true)
	mustCAS(t, "remove key", multi, "a", store.DataV([]byte("data")), store.MissingV)
	killers[0].setKilled(false)
	mustListCount(t, "multi after remove", multi, 0)
	mustListCount(t, "dirs[0] after remove", dirs[0], 1)
	multi.scrubAll()
	mustListCount(t, "dirs[0] after scrub", dirs[0], 0)
}
